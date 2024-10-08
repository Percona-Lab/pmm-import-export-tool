// Copyright 2023 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"pmm-dump/pkg/dump"
	grafana "pmm-dump/pkg/grafana"
	"pmm-dump/pkg/grafana/client"
	"pmm-dump/pkg/transferer"
	"pmm-dump/pkg/util"
	"pmm-dump/pkg/victoriametrics"
)

const defaultTimeframe = time.Hour * 4

var (
	GitBranch  string
	GitCommit  string
	GitVersion string
)

func main() { //nolint:gocyclo,maintidx
	var (
		cli = kingpin.New("pmm-dump", "Percona PMM Dump")

		// general options
		pmmURL = cli.Flag("pmm-url", "PMM connection string").Envar("PMM_URL").String()

		pmmHost     = cli.Flag("pmm-host", "PMM server host(with scheme)").Envar("PMM_HOST").String()
		pmmPort     = cli.Flag("pmm-port", "PMM server port").Envar("PMM_PORT").String()
		pmmUser     = cli.Flag("pmm-user", "PMM credentials user").Envar("PMM_USER").String()
		pmmToken    = cli.Flag("pmm-token", "PMM API token").Envar("PMM_TOKEN").String()
		pmmCookie   = cli.Flag("pmm-cookie", "PMM Auth cookie").Envar("PMM_COOKIE").String()
		pmmPassword = cli.Flag("pmm-pass", "PMM credentials password").Envar("PMM_PASS").String()

		victoriaMetricsURL = cli.Flag("victoria-metrics-url", "VictoriaMetrics connection string").String()
		clickHouseURL      = cli.Flag("click-house-url", "ClickHouse connection string").String()

		dumpCore = cli.Flag("dump-core", "Specify to export/import core metrics").Default("true").Bool()
		dumpQAN  = cli.Flag("dump-qan", "Specify to export/import QAN metrics").Bool()

		enableVerboseMode  = cli.Flag("verbose", "Enable verbose mode").Short('v').Bool()
		allowInsecureCerts = cli.Flag("allow-insecure-certs",
			"Accept any certificate presented by the server and any host name in that certificate").Bool()

		dumpPath = cli.Flag("dump-path", "Path to dump file").Short('d').String()

		workersCount = cli.Flag("workers", "Set the number of reading workers").Int()

		vmNativeData = cli.Flag("vm-native-data", "Use VictoriaMetrics' native export format. Reduces dump size, but can be incompatible between PMM versions").Bool()
		// export command options
		exportCmd = cli.Command("export", "Export PMM Server metrics to dump file."+
			"By default only the 4 last hours are exported, but it can be configured via start-ts/end-ts options")

		start = exportCmd.Flag("start-ts",
			"Start date-time to filter exported metrics, ex. "+time.RFC3339).String()
		end = exportCmd.Flag("end-ts",
			"End date-time to filter exported metrics, ex. "+time.RFC3339).String()

		tsSelector = exportCmd.Flag("ts-selector", "Time series selector to pass to VM").String()
		where      = exportCmd.Flag("where", "ClickHouse only. WHERE statement").Short('w').String()

		instances  = exportCmd.Flag("instance", "Name to filter instances by service names, node names, or instance names. Use multiple times to filter by multiple names").Strings()
		dashboards = exportCmd.Flag("dashboard", "Dashboard name to filter. Use multiple times to filter by multiple dashboards").Strings()

		chunkTimeRange = exportCmd.Flag("chunk-time-range", "Time range to be fit into a single chunk (core metrics). "+
			"5 minutes by default, example '45s', '5m', '1h'").Default("5m").Duration()
		chunkRows = exportCmd.Flag("chunk-rows", "Amount of rows to fit into a single chunk (qan metrics)").Default("100000").Int()

		ignoreLoad = exportCmd.Flag("ignore-load", "Disable checking for load threshold values").Bool()
		maxLoad    = exportCmd.Flag("max-load", "Max load threshold values. For the CPU value is overall regardless cores count: 0-100%").
				Default(fmt.Sprintf("%v=70,%v=80,%v=10", transferer.ThresholdCPU, transferer.ThresholdRAM, transferer.ThresholdMYRAM)).String()
		criticalLoad = exportCmd.Flag("critical-load", "Critical load threshold values. For the CPU value is overall regardless cores count: 0-100%").
				Default(fmt.Sprintf("%v=90,%v=90,%v=30", transferer.ThresholdCPU, transferer.ThresholdRAM, transferer.ThresholdMYRAM)).String()

		stdout = exportCmd.Flag("stdout", "Redirect output to STDOUT").Bool()

		exportServicesInfo = exportCmd.Flag("export-services-info", "Export overview info about all the services, that are being monitored").Bool()
		// import command options
		importCmd = cli.Command("import", "Import PMM Server metrics from dump file")

		vmContentLimit = importCmd.Flag("vm-content-limit", "Limit the chunk content size for VictoriaMetrics (in bytes). Doesn't work with native format").Default("0").Uint64()

		// show meta command options
		showMetaCmd  = cli.Command("show-meta", "Shows metadata from the specified dump file")
		prettifyMeta = showMetaCmd.Flag("prettify", "Print meta in human readable format").Default("true").Bool()

		// version command options
		versionCmd = cli.Command("version", "Shows tool version of the binary")
	)

	ctx := context.Background()

	logConsoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		NoColor:    true,
		TimeFormat: time.RFC3339,
	}

	log.Logger = log.Output(logConsoleWriter)

	cmd, err := cli.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		log.Fatal().Msgf("Error parsing parameters: %s", err.Error())
	}

	if *enableVerboseMode {
		log.Logger = log.Logger.
			With().Caller().Logger().
			Hook(goroutineLoggingHook{}).
			Level(zerolog.DebugLevel)
	} else {
		log.Logger = log.Logger.
			Level(zerolog.InfoLevel)
	}

	switch cmd {
	case exportCmd.FullCommand():
		var startTime, endTime time.Time

		if *end != "" {
			endTime, err = time.ParseInLocation(time.RFC3339, *end, time.UTC)
			if err != nil {
				log.Fatal().Msgf("Error parsing end date-time: %v", err)
			}
		} else {
			endTime = time.Now().UTC()
		}

		if *start != "" {
			startTime, err = time.ParseInLocation(time.RFC3339, *start, time.UTC)
			if err != nil {
				log.Fatal().Msgf("Error parsing start date-time: %v", err)
			}
		} else {
			startTime = endTime.Add(-1 * defaultTimeframe)
		}

		if startTime.After(endTime) {
			log.Fatal().Msg("Invalid time range: start > end")
		}

		httpC := newClientHTTP(*allowInsecureCerts)

		parseURL(pmmURL, pmmHost, pmmPort, pmmUser, pmmPassword)

		authParams := client.AuthParams{
			User:       *pmmUser,
			Password:   *pmmPassword,
			APIToken:   *pmmToken,
			AuthCookie: *pmmCookie,
		}
		grafanaC, err := client.NewClient(httpC, authParams)
		if err != nil {
			log.Fatal().Msgf("Failed to create HTTP client: %v", err)
		}

		var dumpLog bytes.Buffer

		hasLevel := log.Logger.GetLevel()

		log.Logger = log.Logger.Level(zerolog.DebugLevel).Output(zerolog.MultiLevelWriter(LevelWriter{
			Writer: logConsoleWriter,
			Level:  hasLevel,
		}, &dumpLog))

		if !(*dumpQAN || *dumpCore) {
			log.Fatal().Msg("Please, specify at least one data source")
		}

		if *dumpQAN && *dumpCore && len(*instances) == 0 {
			if *where == "" && (*tsSelector != "" || len(*dashboards) > 0) {
				log.Warn().Msg("Filter for core dump found, but not for QAN. QAN metrics for all metrics would be exported")
			} else if *where != "" && *tsSelector == "" && len(*dashboards) == 0 {
				log.Warn().Msg("Filter for QAN found, but not for core dump. Core metrics for all metrics would be exported")
			}
		}

		var sources []dump.Source

		pmmConfig, err := util.GetPMMConfig(*pmmURL, *victoriaMetricsURL, *clickHouseURL)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get PMM config")
		}

		checkVersionSupport(grafanaC, *pmmURL, pmmConfig.VictoriaMetricsURL)

		selectors, err := grafana.GetSelectorsFromDashboards(grafanaC, *pmmURL, *dashboards, *instances, startTime, endTime)
		if err != nil {
			log.Fatal().Msgf("Error retrieving dashboard selectors: %v", err)
		}
		if *tsSelector != "" {
			selectors = append(selectors, *tsSelector)
		} else if len(selectors) == 0 && len(*instances) > 0 {
			for _, serviceName := range *instances {
				selectors = append(selectors, fmt.Sprintf(`{service_name="%s" or node_name="%s" or instance="%s"}`, serviceName, serviceName, serviceName))
			}
		}

		var chunks []dump.ChunkMeta
		if *dumpCore {
			vmSource := prepareVictoriaMetricsSource(grafanaC, pmmConfig.VictoriaMetricsURL, selectors, *vmNativeData, *vmContentLimit)
			sources = append(sources, vmSource)
			hasMetrics, err := vmSource.HasMetrics(startTime, endTime)
			if err != nil {
				log.Fatal().Msgf("Failed to check metrics in VictoriaMetrics: %v", err)
			}
			if hasMetrics {
				chunks = append(chunks, victoriametrics.SplitTimeRangeIntoChunks(startTime, endTime, *chunkTimeRange)...)
			}
		}

		if *dumpQAN { //nolint:nestif
			if *where == "" && len(*instances) > 0 {
				for i, serviceName := range *instances {
					if i != 0 {
						*where += " OR "
					}
					*where += fmt.Sprintf("service_name='%s'", serviceName)
				}
			}

			chSource, err := prepareClickHouseSource(ctx, pmmConfig.ClickHouseURL, *where)
			if err != nil {
				log.Fatal().Msgf("Failed to connect to ClickHouse: %v", err)
			}
			sources = append(sources, chSource)

			chChunks, err := chSource.SplitIntoChunks(startTime, endTime, *chunkRows)
			if err != nil {
				log.Fatal().Msgf("Failed to create clickhouse chunks: %s", err.Error())
			}
			if len(chChunks) > 0 {
				chunks = append(chunks, chChunks...)
			}
		}

		if len(chunks) == 0 {
			if len(*instances) > 0 {
				log.Warn().Msgf("It seems that data about instances specified in the `--instance' option does not exist in the PMM server.")
			}
			log.Fatal().Msgf("Failed to create a dump. No data was found")
		}

		file, err := createFile(*dumpPath, *stdout)
		if err != nil {
			log.Fatal().Msgf("Failed to create file: %v", err)
		}
		defer file.Close() //nolint:errcheck

		t, err := transferer.New(file, sources, *workersCount)
		if err != nil {
			log.Fatal().Msgf("Failed to setup export: %v", err) //nolint:gocritic //TODO: potential problem here, see muted linter warning
		}

		meta, err := composeMeta(*pmmURL, grafanaC, *exportServicesInfo, cli, *vmNativeData)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to compose meta")
		}

		pool, err := dump.NewChunkPool(chunks)
		if err != nil {
			log.Fatal().Msgf("Failed to generate chunk pool: %v", err)
		}

		var thresholds []transferer.Threshold
		if !*ignoreLoad {
			thresholds, err = transferer.ParseThresholdList(*maxLoad, *criticalLoad)
			if err != nil {
				log.Fatal().Err(err).Msgf("Failed to parse max/critical load args")
			}
		}

		lc := transferer.NewLoadChecker(ctx, grafanaC, pmmConfig.VictoriaMetricsURL, thresholds)

		if err = t.Export(ctx, lc, *meta, pool, &dumpLog); err != nil {
			log.Fatal().Msgf("Failed to export: %v", err)
		}
	case importCmd.FullCommand():
		httpC := newClientHTTP(*allowInsecureCerts)
		parseURL(pmmURL, pmmHost, pmmPort, pmmUser, pmmPassword)

		authParams := client.AuthParams{
			User:       *pmmUser,
			Password:   *pmmPassword,
			APIToken:   *pmmToken,
			AuthCookie: *pmmCookie,
		}
		grafanaC, err := client.NewClient(httpC, authParams)
		if err != nil {
			log.Fatal().Msgf("Failed to create HTTP client: %v", err)
		}
		if !(*dumpQAN || *dumpCore) {
			log.Fatal().Msg("Please, specify at least one data source")
		}

		var sources []dump.Source

		pmmConfig, err := util.GetPMMConfig(*pmmURL, *victoriaMetricsURL, *clickHouseURL)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get PMM config")
		}

		checkVersionSupport(grafanaC, *pmmURL, pmmConfig.VictoriaMetricsURL)

		piped, err := checkPiped()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to check if a program is piped")
		}

		if piped { //nolint:nestif
			if *vmNativeData {
				log.Warn().Msgf("Cannot read meta file during import in a pipeline. Using VictoriaMetrics' native export format because `--vm-native-data` was provided")
			} else {
				log.Warn().Msgf("Cannot read meta file during import in a pipeline. Using VictoriaMetrics' JSON export format")
			}
		} else {
			dumpMeta, err := transferer.ReadMetaFromDump(*dumpPath, false)
			if err != nil {
				log.Warn().Msgf("Can't show meta: %v", err)
				*vmNativeData = true
			} else {
				switch dumpMeta.VMDataFormat {
				case "":
					log.Warn().Msgf("Meta file doesn't contain `vm-data-format`. Using VictoriaMetrics' native export format")
					*vmNativeData = true
				case "native":
					*vmNativeData = true
				case "json":
					*vmNativeData = false
				default:
					*vmNativeData = false
					log.Warn().Msgf("Meta file contains invalid `vm-data-format`. Using VictoriaMetrics' JSON export format")
				}
			}
		}

		if *vmNativeData && *vmContentLimit > 0 {
			log.Fatal().Msgf("`--vm-content-limit` is not supported with native data format")
		}

		if *dumpCore {
			vmSource := prepareVictoriaMetricsSource(grafanaC, pmmConfig.VictoriaMetricsURL, nil, *vmNativeData, *vmContentLimit)
			sources = append(sources, vmSource)
		}

		if *dumpQAN {
			chSource, err := prepareClickHouseSource(ctx, pmmConfig.ClickHouseURL, *where)
			if err != nil {
				log.Fatal().Msgf("Failed to connect to ClickHouse: %v", err)
			}
			sources = append(sources, chSource)
		}

		if *dumpPath == "" && !piped {
			log.Fatal().Msg("Please, specify path to dump file")
		}

		file, err := getFile(*dumpPath, piped)
		if err != nil {
			log.Fatal().Msgf("Failed to get file: %v", err)
		}
		defer file.Close() //nolint:errcheck

		t, err := transferer.New(file, sources, *workersCount)
		if err != nil {
			log.Fatal().Msgf("Failed to setup import: %v", err)
		}

		meta, err := composeMeta(*pmmURL, grafanaC, *exportServicesInfo, cli, *vmNativeData)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to compose meta")
		}

		if err = t.Import(ctx, *meta); err != nil {
			var additionalInfo string
			if victoriametrics.ErrIsRequestEntityTooLarge(err) {
				additionalInfo = ". Consider to use \"vm-content-limit\" option. Also, you can decrease \"chunk-time-range\" or \"chunk-rows\" values. " +
					"If you use nginx or Apache HTTP Server, consider increasing the maximum size of the client " +
					"request body in their configuration"
			}
			log.Fatal().Msgf("Failed to import: %v%s", err, additionalInfo)
		}
	case showMetaCmd.FullCommand():
		piped, err := checkPiped()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to check if a program is piped")
		}
		if *dumpPath == "" && !piped {
			log.Fatal().Msg("Please, specify path to dump file")
		}

		meta, err := transferer.ReadMetaFromDump(*dumpPath, piped)
		if err != nil {
			log.Fatal().Msgf("Can't show meta: %v", err)
		}

		if *prettifyMeta {
			fmt.Printf("Build: %v\n", meta.Version.GitCommit)
			fmt.Printf("PMM Version: %v\n", meta.PMMServerVersion)
			fmt.Printf("Max Chunk Size: %v (%v)\n", ByteCountDecimal(meta.MaxChunkSize), ByteCountBinary(meta.MaxChunkSize))
			if meta.PMMTimezone != nil {
				fmt.Printf("PMM Timezone: %s\n", *meta.PMMTimezone)
			}
			fmt.Printf("Arguments: %s\n", meta.Arguments)
			if len(meta.PMMServerServices) > 0 {
				fmt.Printf("Services:\n")
				for _, s := range meta.PMMServerServices {
					fmt.Printf("\t- Name: %s\n", s.Name)
					fmt.Printf("\t  Node ID: %s\n", s.NodeID)
					fmt.Printf("\t  Node Name: %s\n", s.NodeName)
					fmt.Printf("\t  Agents ID: %v\n", s.AgentsIDs)
				}
			}
		} else {
			jsonMeta, err := json.MarshalIndent(meta, "", "\t")
			if err != nil {
				log.Fatal().Msgf("Failed to format meta as json: %v", err)
			}

			fmt.Printf("%v\n", string(jsonMeta))
		}
	case versionCmd.FullCommand():
		fmt.Printf("Version: %v, Build: %v\n", GitVersion, GitCommit)
	default:
		log.Fatal().Msgf("Undefined command found: %s", cmd)
	}
}
