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
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"pmm-dump/internal/test/deployment"
	"pmm-dump/internal/test/util"
)

func TestExportImport(t *testing.T) {
	c := deployment.NewController(t)
	pmm := c.NewPMM("export-import", ".env.test")
	newPMM := c.NewPMM("export-import-2", ".env2.test")

	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return pmm.Deploy(gCtx)
	})

	g.Go(func() error {
		return newPMM.Deploy(gCtx)
	})
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Second)

	var b util.Binary
	testDir := t.TempDir()

	pmm.Log("Checking filtering with `--instance` flag")
	args := []string{"-d", filepath.Join(testDir, "filter-dump.tar.gz"), "--pmm-url", pmm.PMMURL(), "--dump-qan", "--click-house-url", pmm.ClickhouseURL(), "--instance", "pmm-server"}
	stdout, stderr, err := b.Run(append([]string{"export", "--ignore-load"}, args...)...)
	if err != nil {
		t.Fatal("failed to export", err, stdout, stderr)
	}
	checkDumpFiltering(t, filepath.Join(testDir, "filter-dump.tar.gz"), "pmm-server")

	args = []string{"-d", filepath.Join(testDir, "dump.tar.gz"), "--pmm-url", pmm.PMMURL(), "--dump-qan", "--click-house-url", pmm.ClickhouseURL()}

	pmm.Log("Exporting data to", filepath.Join(testDir, "dump.tar.gz"))
	stdout, stderr, err = b.Run(append([]string{"export", "--ignore-load"}, args...)...)
	if err != nil {
		t.Fatal("failed to export", err, stdout, stderr)
	}

	args = []string{"-d", filepath.Join(testDir, "dump.tar.gz"), "--pmm-url", newPMM.PMMURL(), "--dump-qan", "--click-house-url", newPMM.ClickhouseURL()}
	pmm.Log("Importing data from", filepath.Join(testDir, "dump.tar.gz"))
	stdout, stderr, err = b.Run(append([]string{"import"}, args...)...)
	if err != nil {
		t.Fatal("failed to import", err, stdout, stderr)
	}
}

func TestShowMeta(t *testing.T) {
	var b util.Binary
	stdout, stderr, err := b.Run("show-meta", "-d", filepath.Join(util.RepoPath, "internal", "test", "e2e", "testdata", "onlymeta.tar.gz"))
	if err != nil {
		t.Fatal(err, stdout, stderr)
	}
	want := `Build: 8f26789
PMM Version: 2.34.0-20.2301131343.a7f5d22.el7
Max Chunk Size: 617.2 kB (602.7 KiB)
Arguments: export --verbose=true --dump-path=dump.tar.gz --pmm-url=http://localhost:8282 --dump-core=true --vm-native-data=true
`
	if stdout != want {
		t.Fatalf("want %s, got %s", want, stdout)
	}
}
