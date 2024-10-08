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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"

	"pmm-dump/pkg/clickhouse"
	"pmm-dump/pkg/dump"
	"pmm-dump/pkg/grafana/client"
	"pmm-dump/pkg/victoriametrics"
)

const minPMMServerVersion = "2.12.0"

func newClientHTTP(insecureSkipVerify bool) *fasthttp.Client {
	return &fasthttp.Client{
		MaxConnsPerHost:           2, //nolint:mnd
		MaxIdleConnDuration:       time.Minute,
		MaxIdemponentCallAttempts: 5, //nolint:mnd
		ReadTimeout:               time.Minute,
		WriteTimeout:              time.Minute,
		MaxConnWaitTimeout:        time.Second * 30, //nolint:mnd
		TLSConfig: &tls.Config{
			InsecureSkipVerify: insecureSkipVerify, //nolint:gosec
		},
	}
}

type goroutineLoggingHook struct{}

func (h goroutineLoggingHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	e.Int("goroutine_id", getGoroutineID())
}

func getGoroutineID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

// getPMMVersion returns version, full-version and error.
func getPMMVersion(pmmURL string, c *client.Client) (string, string, error) {
	type versionResp struct {
		Version string `json:"version"`
		Server  struct {
			Version     string    `json:"version"`
			FullVersion string    `json:"full_version"`
			Timestamp   time.Time `json:"timestamp"`
		} `json:"server"`
		DistributionMethod string `json:"distribution_method"`
	}

	statusCode, body, err := c.Get(pmmURL + "/v1/version")
	if err != nil {
		return "", "", err
	}
	if statusCode != fasthttp.StatusOK {
		return "", "", fmt.Errorf("non-ok status: %d", statusCode)
	}
	var resp versionResp
	if err = json.Unmarshal(body, &resp); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return resp.Server.Version, resp.Server.FullVersion, nil
}

func getPMMServices(pmmURL string, c *client.Client) ([]dump.PMMServerService, error) {
	type servicesResp map[string][]struct {
		ID     string `json:"service_id"`
		Name   string `json:"service_name"`
		NodeID string `json:"node_id"`
	}

	// Services

	statusCode, body, err := c.Post(pmmURL + "/v1/inventory/Services/List")
	if err != nil {
		return nil, err
	}
	if statusCode != fasthttp.StatusOK {
		return nil, fmt.Errorf("non-ok status: %d", statusCode)
	}
	var serviceResp servicesResp
	if err = json.Unmarshal(body, &serviceResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	services := make([]dump.PMMServerService, 0)
	for _, serviceType := range serviceResp {
		for _, service := range serviceType {
			newService := dump.PMMServerService{
				Name:   service.Name,
				NodeID: service.NodeID,
			}

			nodeName, err := getPMMServiceNodeName(pmmURL, c, service.NodeID)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get pmm service node name")
			}
			newService.NodeName = nodeName

			agentsIds, err := getPMMServiceAgentsIds(pmmURL, c, service.ID)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get pmm service agents ids")
			}
			newService.AgentsIDs = agentsIds

			services = append(services, newService)
		}
	}
	return services, nil
}

func getPMMServiceNodeName(pmmURL string, c *client.Client, nodeID string) (string, error) {
	type nodeRespStruct struct {
		Generic struct {
			Name string `json:"node_name"`
		} `json:"generic"`
	}

	statusCode, body, err := c.PostJSON(pmmURL+"/v1/inventory/Nodes/Get", struct {
		NodeID string `json:"node_id"`
	}{nodeID})
	if err != nil {
		return "", err
	}
	if statusCode != fasthttp.StatusOK {
		return "", fmt.Errorf("non-ok status: %d", statusCode)
	}
	var nodeResp nodeRespStruct
	if err = json.Unmarshal(body, &nodeResp); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return nodeResp.Generic.Name, nil
}

func getPMMServiceAgentsIds(pmmURL string, c *client.Client, serviceID string) ([]string, error) {
	type agentsRespStruct map[string][]struct {
		ServiceID *string `json:"service_id"`
		AgentID   *string `json:"agent_id"`
	}

	statusCode, body, err := c.Post(pmmURL + "/v1/inventory/Agents/List")
	if err != nil {
		return nil, err
	}
	if statusCode != fasthttp.StatusOK {
		return nil, fmt.Errorf("non-ok status: %d", statusCode)
	}
	var agentsResp agentsRespStruct
	if err = json.Unmarshal(body, &agentsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	agentsIDs := make([]string, 0)

	for _, agentType := range agentsResp {
		for _, agent := range agentType {
			if agent.ServiceID != nil && *agent.ServiceID == serviceID {
				agentsIDs = append(agentsIDs, *agent.AgentID)
			}
		}
	}

	return agentsIDs, nil
}

// getTimeZone returns empty string result if there is no preferred timezone in pmm-server graphana settings.
func getPMMTimezone(pmmURL string, c *client.Client) (string, error) {
	type tzResp struct {
		Timezone string `json:"timezone"`
	}

	statusCode, body, err := c.Get(pmmURL + "/graph/api/org/preferences")
	if err != nil {
		return "", err
	}
	if statusCode != fasthttp.StatusOK {
		return "", fmt.Errorf("non-ok status: %d", statusCode)
	}

	var resp tzResp
	if err = json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return resp.Timezone, nil
}

func composeMeta(pmmURL string, c *client.Client, exportServices bool, cli *kingpin.Application, vmNativeData bool) (*dump.Meta, error) {
	_, pmmVer, err := getPMMVersion(pmmURL, c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get PMM version")
	}

	pmmTzRaw, err := getPMMTimezone(pmmURL, c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get PMM timezone")
	}
	var pmmTz *string
	if len(pmmTzRaw) == 0 || pmmTzRaw == "browser" {
		pmmTz = nil
	} else {
		pmmTz = &pmmTzRaw
	}

	context, err := cli.DefaultEnvars().ParseContext(os.Args[1:])
	if err != nil {
		return nil, err
	}
	var args []string
	for _, element := range context.Elements {
		switch cl := element.Clause.(type) {
		case *kingpin.CmdClause:
			args = append(args, cl.FullCommand())
		case *kingpin.FlagClause:
			model := cl.Model()
			value := model.Value.String()
			switch model.Name {
			case "pmm-user", "pmm-pass":
				value = "***"
			}
			args = append(args, fmt.Sprintf("--%s=%s", model.Name, value))
		}
	}

	pmmServices := []dump.PMMServerService(nil)
	if exportServices {
		pmmServices, err = getPMMServices(pmmURL, c)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get PMM services")
		}
	}

	meta := &dump.Meta{
		Version: dump.PMMDumpVersion{
			GitBranch: GitBranch,
			GitCommit: GitCommit,
		},
		PMMServerVersion:  pmmVer,
		PMMTimezone:       pmmTz,
		Arguments:         strings.Join(args, " "),
		PMMServerServices: pmmServices,
		VMDataFormat:      "json",
	}

	if vmNativeData {
		meta.VMDataFormat = "native"
	}

	return meta, nil
}

func ByteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

func ByteCountBinary(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func checkPiped() (bool, error) {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false, err
	}
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		return true, nil
	}
	return false, nil
}

type LevelWriter struct {
	Writer io.Writer
	Level  zerolog.Level
}

func (lw LevelWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	if level >= lw.Level {
		return lw.Write(p)
	}

	return len(p), nil
}

func (lw LevelWriter) Write(p []byte) (int, error) {
	return lw.Writer.Write(p)
}

func checkVersionSupport(c *client.Client, pmmURL, victoriaMetricsURL string) {
	if err := victoriametrics.ExportTestRequest(c, victoriaMetricsURL); err != nil {
		if !errors.Is(err, victoriametrics.ErrNotFound) {
			log.Fatal().Err(err).Msg("Failed to make test requests")
		}
		log.Error().Msg("There are 404 not-found errors occurred when making test requests. Maybe PMM-server version is not supported!")
	}

	pmmVer, _, err := getPMMVersion(pmmURL, c)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get PMM version")
	}
	if pmmVer == "" {
		log.Fatal().Msg("Could not find server version")
	}

	if pmmVer < minPMMServerVersion {
		log.Fatal().Msgf("Your PMM-server version %s is lower, than minimum required: %s!", pmmVer, minPMMServerVersion)
	}
}

func prepareVictoriaMetricsSource(grafanaC *client.Client, url string, selectors []string, nativeData bool, contentLimit uint64) *victoriametrics.Source {
	if contentLimit > math.MaxInt {
		log.Fatal().Msgf("`--vm-content-limit` can't have a value greater than %d", math.MaxInt)
	}

	c := &victoriametrics.Config{
		ConnectionURL:       url,
		TimeSeriesSelectors: selectors,
		NativeData:          nativeData,
		ContentLimit:        int(contentLimit),
	}

	log.Debug().Msgf("Got Victoria Metrics URL: %s", c.ConnectionURL)

	return victoriametrics.NewSource(grafanaC, *c)
}

func prepareClickHouseSource(ctx context.Context, url, where string) (*clickhouse.Source, error) {
	c := &clickhouse.Config{
		ConnectionURL: url,
		Where:         where,
	}

	clickhouseSource, err := clickhouse.NewSource(ctx, *c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ClickHouse source")
	}

	log.Debug().Msgf("Got ClickHouse URL: %s", c.ConnectionURL)

	return clickhouseSource, nil
}

func parseURL(pmmURL, pmmHost, pmmPort, pmmUser, pmmPassword *string) {
	parsedURL, err := url.Parse(*pmmURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot parse pmm url")
	}

	// Host(scheme + hostname)
	if parsedURL.Host == "" && parsedURL.Path != "" {
		log.Error().Msg("pmm-url input can be mismatched as path and not as host!")
	}
	if *pmmHost != "" {
		parsedHostURL, err := url.Parse(*pmmHost)
		if err != nil {
			log.Fatal().Err(err).Msg("Cannot parse pmm-host")
		}

		parsedURL.Scheme = parsedHostURL.Scheme
		parsedURL.Host = parsedHostURL.Hostname()
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		log.Fatal().Msg("There is no host found neither in pmm-url or pmm-host")
	}

	// Port
	if *pmmPort != "" {
		_, err := strconv.Atoi(*pmmPort)
		if err != nil {
			log.Fatal().Msg("Cannot parse port!")
		}
		parsedURL.Host = parsedURL.Hostname() + ":" + *pmmPort
	}

	// User
	if parsedURL.User != nil {
		if *pmmUser == "" {
			log.Info().Msg("Credential user was obtained from pmm-url")
			*pmmUser = parsedURL.User.Username()
		}
		if *pmmPassword == "" {
			log.Info().Msg("Credential password was obtained from pmm-url")
			*pmmPassword, _ = parsedURL.User.Password()
		}
		parsedURL.User = nil
	}

	*pmmURL = parsedURL.String()
}

func getFile(dumpPath string, piped bool) (io.ReadWriteCloser, error) {
	var file io.ReadWriteCloser
	if piped {
		file = os.Stdin
	} else {
		var err error
		log.Info().
			Str("path", dumpPath).
			Msg("Opening dump file...")

		file, err = os.Open(dumpPath) //nolint:gosec
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open dump file %s", dumpPath)
		}
	}
	return file, nil
}

const dirPermission = 0o777

func createFile(dumpPath string, piped bool) (io.ReadWriteCloser, error) {
	var file *os.File
	if piped {
		file = os.Stdout
	} else {
		exportTS := time.Now().UTC()
		log.Debug().Msgf("Trying to determine filepath")
		filepath, err := getDumpFilepath(dumpPath, exportTS)
		if err != nil {
			return nil, err
		}

		log.Debug().Msgf("Preparing dump file: %s", filepath)
		if err := os.MkdirAll(path.Dir(filepath), dirPermission); err != nil {
			return nil, errors.Wrap(err, "failed to create folders for the dump file")
		}
		file, err = os.Create(filepath) //nolint:gosec
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create %s", filepath)
		}
	}
	return file, nil
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
