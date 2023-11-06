package victoriametrics

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pmm-dump/pkg/grafana"

	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

func TestWriteChunk(t *testing.T) {
	tests := []struct {
		name         string
		metricsSize  int
		contentLimit int
		nativeData   bool
		shouldErr    bool
	}{
		{
			name:         "native data",
			metricsSize:  20,
			contentLimit: 20,
			nativeData:   true,
			shouldErr:    true,
		},
		{
			name:         "0 content limit",
			metricsSize:  20,
			contentLimit: 0,
		},
		{
			name:         "with content limit",
			metricsSize:  20,
			contentLimit: 130,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpC := &fasthttp.Client{
				MaxConnsPerHost:           2,
				MaxIdleConnDuration:       time.Minute,
				MaxIdemponentCallAttempts: 5,
				ReadTimeout:               time.Minute,
				WriteTimeout:              time.Minute,
				MaxConnWaitTimeout:        time.Second * 30,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true, //nolint:gosec
				},
			}
			var recievedMetrics []Metric
			server := httptest.NewServer(http.HandlerFunc(
				func(rw http.ResponseWriter, req *http.Request) {
					defer req.Body.Close() //nolint:errcheck
					if req.ContentLength > int64(tt.contentLimit) && tt.contentLimit != 0 {
						rw.WriteHeader(http.StatusRequestEntityTooLarge)
						return
					}
					compressedContent, err := io.ReadAll(req.Body)
					if err != nil {
						t.Error(err)
						rw.WriteHeader(http.StatusBadRequest)
						return
					}
					metrics, err := decompressChunk(compressedContent)
					if err != nil {
						t.Error(err)
						rw.WriteHeader(http.StatusBadRequest)
						return
					}
					recievedMetrics = append(recievedMetrics, metrics...)
				},
			))
			defer server.Close()

			s := NewSource(grafana.NewClient(httpC), Config{
				ConnectionURL: server.URL,
				NativeData:    tt.nativeData,
				ContentLimit:  uint64(tt.contentLimit),
			})

			data, err := generateFakeChunk(tt.metricsSize)
			if err != nil {
				t.Fatal(err)
			}
			err = s.WriteChunk("", bytes.NewBuffer(data))
			if err != nil && !tt.shouldErr {
				t.Fatal(err)
			}
			if err == nil && tt.shouldErr {
				t.Fatal("should be error")
			}
		})
	}
}

func generateFakeChunk(size int) ([]byte, error) {
	metricsData, err := json.Marshal(Metric{
		Metric: map[string]string{
			"__name__": "test",
			"job":      "test",
			"instance": "test",
			"test":     "test",
		},
		Values:     []float64{100000000000000},
		Timestamps: []int64{time.Now().Unix()},
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal metrics")
	}
	var data []byte
	for i := 0; i < size; i++ {
		data = append(data, metricsData...)
	}
	return compressData(data)
}

func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, errors.Wrap(err, "write gzip")
	}
	if err := gw.Close(); err != nil {
		return nil, errors.Wrap(err, "close gzip")
	}
	return buf.Bytes(), nil
}
