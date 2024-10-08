//go:build e2e

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

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/valyala/fasthttp"

	"pmm-dump/internal/test/deployment"
	"pmm-dump/internal/test/util"
	"pmm-dump/pkg/dump"
)

func TestDashboard(t *testing.T) {
	ctx := context.Background()

	c := deployment.NewController(t)
	pmm := c.NewPMM("dashboard", ".env.test")
	if err := pmm.Deploy(ctx); err != nil {
		t.Fatal(err)
	}

	importCustomDashboards(t, pmm)
	names := getAllDashbaordNames(t, pmm)

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			testDir := t.TempDir()

			var b util.Binary

			dashboardDumpPath := filepath.Join(testDir, "dump.tar.gz")
			args := []string{"-d", dashboardDumpPath, "--pmm-url", pmm.PMMURL(), "--pmm-user", "admin", "--pmm-pass", "admin", "--dashboard", name}

			pmm.Log("Exporting data with `--dashboard` flag to", dashboardDumpPath)
			stdout, stderr, err := b.Run(append([]string{"export", "--ignore-load"}, args...)...)
			if err != nil {
				if strings.Contains(stderr, "Failed to create a dump. No data was found") {
					// If pmm-dump returns this error, it also means that the dashboard selector parsing was successful
					return
				}
				t.Fatal("failed to export", err, stdout, stderr)
			}

			dashboardDumpPath = filepath.Join(testDir, "dump2.tar.gz")
			args = []string{"-d", dashboardDumpPath, "--pmm-url", pmm.PMMURL(), "--pmm-user", "admin", "--pmm-pass", "admin", "--dashboard", name, "--instance", "pmm-server"}
			pmm.Log("Exporting data with `--dashboard` flag and `--instance` to", dashboardDumpPath)
			stdout, stderr, err = b.Run(append([]string{"export", "--ignore-load"}, args...)...)
			if err != nil {
				if strings.Contains(stderr, "Failed to create a dump. No data was found") {
					// If pmm-dump returns this error, it also means that the dashboard selector parsing was successful
					return
				}
				t.Fatal("failed to export", err, stdout, stderr)
			}
			checkDumpFiltering(t, dashboardDumpPath, "pmm-server")
		})
	}
}

func checkDumpFiltering(t *testing.T, dumpPath, instanceFilter string) {
	t.Helper()

	chunkMap, err := readChunks(dumpPath)
	if err != nil {
		t.Fatal("failed to read dump", dumpPath)
	}

	for filename, data := range chunkMap {
		dir, _ := path.Split(filename)
		st := dump.ParseSourceType(dir[:len(dir)-1])
		switch st {
		case dump.VictoriaMetrics:
			chunk, err := vmParseChunk(data)
			if err != nil {
				t.Fatal("failed to parse chunk", filename, "error", err.Error())
			}

			for _, metric := range chunk {
				if metric.Metric["service_name"] != instanceFilter && metric.Metric["instance"] != instanceFilter && metric.Metric["node_name"] != instanceFilter {
					t.Fatal("metric", metric, "wasn't filtered by --instance option", instanceFilter, "in chunk", filename)
				}
			}
		case dump.ClickHouse:
		default:
			t.Fatal("unknown source type", st)
		}
	}
}

func importCustomDashboards(t *testing.T, pmm *deployment.PMM) {
	t.Helper()

	grafanaClient, err := pmm.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(filepath.Join(util.RepoPath, "internal", "test", "e2e", "testdata", "dashboards"))
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range entries {
		data, err := os.ReadFile(filepath.Join(util.RepoPath, "internal", "test", "e2e", "testdata", "dashboards", v.Name()))
		if err != nil {
			t.Fatal(err)
		}
		dashboard := make(map[string]any)
		if err := json.Unmarshal(data, &dashboard); err != nil {
			t.Fatal(err)
		}
		importReq := map[string]any{
			"dashboard": dashboard,
			"folderId":  0,
			"inputs":    make([]any, 0),
		}
		status, data, err := grafanaClient.PostJSON(pmm.PMMURL()+"/graph/api/dashboards/import", importReq)
		if err != nil {
			t.Fatal("failed to import dashboard", err)
		}
		if status != fasthttp.StatusOK {
			t.Fatalf("non-ok status: %d: %s", status, string(data))
		}
	}
}

func getAllDashbaordNames(t *testing.T, pmm *deployment.PMM) []string {
	t.Helper()

	grafanaClient, err := pmm.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	q := fasthttp.AcquireArgs()
	defer fasthttp.ReleaseArgs(q)

	q.Add("query", "")
	status, data, err := grafanaClient.Get(pmm.PMMURL() + "/graph/api/search?" + q.String())
	if err != nil {
		t.Fatal(err)
	}
	if status != fasthttp.StatusOK {
		t.Fatalf("non-ok status: %d", status)
	}

	type dashboardResp struct {
		Title string `json:"title"`
	}
	var s []dashboardResp
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(s))
	for _, v := range s {
		names = append(names, v.Title)
	}
	return names
}
