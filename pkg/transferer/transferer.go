package transferer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"pmm-dump/pkg/dump"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type Transferer struct {
	dumpPath     string
	sources      []dump.Source
	workersCount int
	piped        bool
}

func New(dumpPath string, piped bool, s []dump.Source, workersCount int) (*Transferer, error) {
	if len(s) == 0 {
		return nil, errors.New("no sources provided")
	}

	if workersCount <= 0 {
		workersCount = runtime.NumCPU()
	}

	return &Transferer{
		dumpPath:     dumpPath,
		sources:      s,
		workersCount: workersCount,
		piped:        piped,
	}, nil
}

type ChunkPool interface {
	Next() (dump.ChunkMeta, bool)
}

type LoadStatusGetter interface {
	GetLatestStatus() (LoadStatus, int)
}

const maxChunksInMem = 4

func (t Transferer) readChunksFromSource(ctx context.Context, lc LoadStatusGetter, p ChunkPool, chunkC chan<- *dump.Chunk) error {
	for {
		log.Debug().Msg("New chunks reading loop iteration has been started")

		select {
		case <-ctx.Done():
			log.Debug().Msg("Context is done, stopping chunks reading")
			return ctx.Err()
		default:
			status, count := lc.GetLatestStatus()
			switch status {
			case LoadStatusWait:
				if count > MaxWaitStatusInSequence {
					log.Warn().Msgf("Too many %v in a sequence. Aborting", LoadStatusWait)
					return fmt.Errorf("terminated by exceeding max load (got wait load status) threshold %d times. Check --max-load value or use --ignore-load", MaxWaitStatusInSequence)
				}
				log.Debug().Msgf("Exceeded max load threshold(got wait load status): putting chunks reading to sleep for %v", MaxLoadWaitDuration)
				time.Sleep(MaxLoadWaitDuration)
				continue
			case LoadStatusTerminate:
				log.Debug().Msg("Got terminate load status: stopping chunks reading")
				return errors.New("terminated by exceeding critical load threshold (got terminate load status). Check --critical-load value or use --ignore-load")
			case LoadStatusOK:
			default:
				return errors.New("unknown load status")
			}

			chMeta, ok := p.Next()
			if !ok {
				log.Debug().Msg("Pool is empty: stopping chunks reading")
				return nil
			}

			s, ok := t.sourceByType(chMeta.Source)
			if !ok {
				return errors.New("failed to find source to read chunk")
			}

			c, err := s.ReadChunk(chMeta)
			if err != nil {
				return errors.Wrap(err, "failed to read chunk")
			}

			log.Debug().
				Stringer("source", c.Source).
				Str("filename", c.Filename).
				Msg("Successfully read chunk. Sending to chunks channel...")

			chunkC <- c
		}
	}
}

func getDumpFilepath(customPath string, ts time.Time) (string, error) {
	autoFilename := fmt.Sprintf("pmm-dump-%v.tar.gz", ts.Unix())
	if customPath == "" {
		return autoFilename, nil
	}

	customPathInfo, err := os.Stat(customPath)
	if err != nil && !os.IsNotExist(err) {
		return "", errors.Wrap(err, "failed to get custom path info")
	}

	if (err == nil && customPathInfo.IsDir()) || os.IsPathSeparator(customPath[len(customPath)-1]) {
		// file exists and it's directory
		return path.Join(customPath, autoFilename), nil
	}

	return customPath, nil
}

func (t Transferer) writeChunksToFile(ctx context.Context, meta dump.Meta, chunkC <-chan *dump.Chunk, logBuffer *bytes.Buffer) error {
	var file *os.File
	if t.piped {
		file = os.Stdout
	} else {
		exportTS := time.Now().UTC()
		log.Debug().Msgf("Trying to determine filepath")
		filepath, err := getDumpFilepath(t.dumpPath, exportTS)
		if err != nil {
			return err
		}

		log.Debug().Msgf("Preparing dump file: %s", filepath)
		if err := os.MkdirAll(path.Dir(filepath), 0777); err != nil {
			return errors.Wrap(err, "failed to create folders for the dump file")
		}
		file, err = os.Create(filepath)
		if err != nil {
			return errors.Wrapf(err, "failed to create %s", filepath)
		}
	}
	defer file.Close()

	gzw, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		return errors.Wrap(err, "failed to create gzip writer")
	}
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	for {
		log.Debug().Msg("New chunks writing loop iteration has been started")

		select {
		case <-ctx.Done():
			log.Debug().Msg("Context is done, stopping chunks writing")
			return ctx.Err()
		default:
			c, ok := <-chunkC
			if !ok {
				if err := writeMetafile(tw, meta); err != nil {
					return err
				}

				if err = writeLog(tw, logBuffer); err != nil {
					return err
				}

				log.Debug().Msg("Chunks channel is closed: stopping chunks writing")
				return nil
			}

			s, ok := t.sourceByType(c.Source)
			if !ok {
				return errors.New("failed to find source to write chunk")
			}

			log.Info().
				Stringer("source", c.Source).
				Str("filename", c.Filename).
				Msg("Writing chunk to the dump...")

			chunkSize := int64(len(c.Content))
			if chunkSize > meta.MaxChunkSize {
				meta.MaxChunkSize = chunkSize
			}

			err = tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,
				Name:     path.Join(s.Type().String(), c.Filename),
				Size:     chunkSize,
				Mode:     0600,
				ModTime:  time.Now(),
			})
			if err != nil {
				return errors.Wrap(err, "failed to write file header")
			}

			if _, err = tw.Write(c.Content); err != nil {
				return errors.Wrap(err, "failed to write chunk content")
			}
		}
	}
}

func writeLog(tw *tar.Writer, logBuffer *bytes.Buffer) error {
	log.Debug().Msg("Writing dump log")

	byteLog := logBuffer.Bytes()

	err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     dump.LogFilename,
		Size:     int64(len(byteLog)),
		Mode:     0600,
		ModTime:  time.Now(),
	})
	if err != nil {
		return errors.Wrap(err, "failed to write dump log header")
	}

	if _, err = tw.Write(byteLog); err != nil {
		return errors.Wrap(err, "failed to write dump log content")
	}

	return nil
}

func (t Transferer) Export(ctx context.Context, lc LoadStatusGetter, meta dump.Meta, pool ChunkPool, logBuffer *bytes.Buffer) error {
	log.Info().Msg("Exporting metrics...")

	chunksCh := make(chan *dump.Chunk, maxChunksInMem)
	log.Debug().
		Int("size", maxChunksInMem).
		Msg("Created chunks channel")

	readWG := &sync.WaitGroup{}
	g, gCtx := errgroup.WithContext(ctx)

	log.Debug().Msgf("Starting %d goroutines to read chunks from sources...", t.workersCount)
	readWG.Add(t.workersCount)
	for i := 0; i < t.workersCount; i++ {
		g.Go(func() error {
			defer log.Debug().Msgf("Exiting from read chunks goroutine")
			defer readWG.Done()

			if err := t.readChunksFromSource(gCtx, lc, pool, chunksCh); err != nil {
				return errors.Wrap(err, "failed to read chunks from source")
			}
			return nil
		})
	}

	log.Debug().Msgf("Starting goroutine to close channel after read finish...")
	go func() {
		readWG.Wait()
		close(chunksCh)
		log.Debug().Msgf("Exiting from goroutine waiting for read to finish")
	}()

	log.Debug().Msg("Starting single goroutine for writing chunks to the dump...")
	g.Go(func() error {
		defer log.Debug().Msgf("Exiting from write chunks goroutine")
		if err := t.writeChunksToFile(ctx, meta, chunksCh, logBuffer); err != nil {
			return errors.Wrap(err, "failed to write chunks to the dump")
		}
		return nil
	})
	log.Debug().Msg("Waiting for all chunks to be processed...")
	if err := g.Wait(); err != nil {
		log.Debug().Msg("Got error, finishing export")
		return err
	}

	log.Info().Msg("Successfully exported!")

	return nil
}

func (t Transferer) writeChunksToSource(ctx context.Context, chunkC <-chan *dump.Chunk) error {
	for {
		log.Debug().Msg("New chunks writing loop iteration has been started")

		select {
		case <-ctx.Done():
			log.Debug().Msg("Context is done, stopping chunks writing")
			return ctx.Err()
		default:
			c, ok := <-chunkC
			if !ok {
				log.Debug().Msg("Chunks channel is closed: stopping chunks writing")
				return nil
			}

			s, ok := t.sourceByType(c.Source)
			if !ok {
				log.Warn().Msgf("Found dump data for %v, but it's not specified - skipped", c.Source)
				continue
			}

			log.Debug().Msgf("Writing chunk '%v' to the source...", c.Filename)
			if err := s.WriteChunk(c.Filename, bytes.NewBuffer(c.Content)); err != nil {
				return errors.Wrap(err, "failed to write chunk")
			}
			log.Info().Msgf("Successfully processed '%v'", c.Filename)
		}
	}
}

func (t Transferer) Import(ctx context.Context, runtimeMeta dump.Meta) error {
	log.Info().Msg("Importing metrics...")

	var file *os.File
	if t.piped {
		file = os.Stdin
	} else {
		var err error
		log.Info().
			Str("path", t.dumpPath).
			Msg("Opening dump file...")

		file, err = os.Open(t.dumpPath)
		if err != nil {
			return errors.Wrap(err, "failed to open file")
		}
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return errors.Wrap(err, "failed to open as gzip")
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	var metafileExists bool

	chunksC := make(chan *dump.Chunk, maxChunksInMem)

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < t.workersCount; i++ {
		g.Go(func() error {
			defer log.Debug().Msgf("Exiting from write chunks goroutine")
			if err := t.writeChunksToSource(gCtx, chunksC); err != nil {
				return errors.Wrap(err, "failed to write chunks to source")
			}
			return nil
		})
	}

	for {
		log.Debug().Msg("Reading file from dump...")

		header, err := tr.Next()

		if err == io.EOF {
			log.Debug().Msg("Processed complete dump file")
			break
		}

		if err != nil {
			return errors.Wrap(err, "failed to read file from dump")
		}

		dir, filename := path.Split(header.Name)

		if filename == dump.MetaFilename {
			readAndCompareDumpMeta(tr, runtimeMeta)
			metafileExists = true
			continue
		}

		if filename == dump.LogFilename {
			continue
		}

		log.Info().Msgf("Processing chunk '%s'...", header.Name)

		st := dump.ParseSourceType(dir[:len(dir)-1])
		if st == dump.UndefinedSource {
			return errors.Errorf("corrupted dump: found undefined source: %s", dir)
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			return errors.Wrap(err, "failed to read chunk content")
		}

		ch := &dump.Chunk{
			ChunkMeta: dump.ChunkMeta{
				Source: st,
			},
			Content:  content,
			Filename: filename,
		}

		log.Debug().Msgf("Sending chunk '%s' to the channel...", header.Name)
		chunksC <- ch
	}
	close(chunksC)

	if err := g.Wait(); err != nil {
		log.Debug().Msg("Got error, finishing import")
		return err
	}

	if !metafileExists {
		log.Error().Msg("No meta file found in dump. No version checks performed")
	}

	log.Debug().Msg("Finalizing writes...")

	for _, s := range t.sources {
		if err = s.FinalizeWrites(); err != nil {
			return errors.Wrap(err, "failed to finalize import")
		}
	}

	log.Info().Msg("Successfully imported!")

	return nil
}

func (t Transferer) sourceByType(st dump.SourceType) (dump.Source, bool) {
	for _, s := range t.sources {
		if s.Type() == st {
			return s, true
		}
	}
	return nil, false
}
