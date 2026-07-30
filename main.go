// Copyright ITsysCOM GmbH
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	coprAPI     = "https://copr.fedorainfracloud.org/api_3"
	owner       = "cgrates"
	packageName = "cgrates"
	packageDir  = "/var/packages/rpm"

	masterProject  = "master"
	v010Project    = "v0.10"
	nightlyDir     = "nightly"
	currentSymlink = "cgrates-current.rpm"

	apiTimeout      = 30 * time.Second
	downloadTimeout = 10 * time.Minute
)

var projects = []string{masterProject, v010Project}

type buildListResp struct {
	Items []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
	} `json:"items"`
}

type chrootListResp struct {
	Items []buildChroot `json:"items"`
}

type buildChroot struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	ResultURL string `json:"result_url"`
}

type builtPackagesResp struct {
	Packages []rpmPackage `json:"packages"`
}

type rpmPackage struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Release string `json:"release"`
	Arch    string `json:"arch"`
}

type errResp struct {
	Output string `json:"output"`
	Error  string `json:"error"`
}

// CoprClient holds the shared http client and per-project last-seen build IDs.
type CoprClient struct {
	httpClient *http.Client

	mu           sync.Mutex
	lastBuildIDs map[string]int
}

func NewCoprClient() *CoprClient {
	return &CoprClient{
		httpClient:   &http.Client{},
		lastBuildIDs: make(map[string]int),
	}
}

func (f *CoprClient) getLastBuildID(project string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastBuildIDs[project]
}

func (f *CoprClient) setLastBuildID(project string, id int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastBuildIDs[project] = id
}

// coprReq performs a GET against the Copr API and decodes JSON
func (f *CoprClient) coprReq(ctx context.Context, url string, dest any) error {
	reqCtx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		var errRsp errResp
		if json.Unmarshal(body, &errRsp) == nil && errRsp.Error != "" {
			return fmt.Errorf("copr API %d: %s", resp.StatusCode, errRsp.Error)
		}
		return fmt.Errorf("copr API %d: %s", resp.StatusCode, body)
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func (f *CoprClient) latestBuildID(ctx context.Context, project string) (int, error) {
	u := fmt.Sprintf("%s/build/list?ownername=%s&projectname=%s&limit=1&order_by=id&order_type=DESC&status=succeeded",
		coprAPI, owner, project)
	var r buildListResp
	if err := f.coprReq(ctx, u, &r); err != nil {
		return 0, err
	}
	if len(r.Items) == 0 {
		return 0, fmt.Errorf("no succeeded builds for %s/%s", owner, project)
	}
	return r.Items[0].ID, nil
}

func (f *CoprClient) getChrootList(ctx context.Context, buildID int) ([]buildChroot, error) {
	u := fmt.Sprintf("%s/build-chroot/list?build_id=%d", coprAPI, buildID)
	var r chrootListResp
	if err := f.coprReq(ctx, u, &r); err != nil {
		return nil, err
	}
	return r.Items, nil
}

func (f *CoprClient) getBuiltPackages(ctx context.Context, buildID int, chrootName string) ([]rpmPackage, error) {
	u := fmt.Sprintf("%s/build-chroot/built-packages?build_id=%d&chrootname=%s", coprAPI, buildID, chrootName)
	var r builtPackagesResp
	if err := f.coprReq(ctx, u, &r); err != nil {
		return nil, err
	}
	return r.Packages, nil
}

func (f *CoprClient) processProject(ctx context.Context, project string) error {
	prevbuildID := f.getLastBuildID(project)

	buildID, err := f.latestBuildID(ctx, project)
	if err != nil {
		return fmt.Errorf("get latest build id: %w", err)
	}
	if buildID == prevbuildID {
		slog.Info("no new build", "project", project, "build", buildID)
		return nil
	}
	slog.Info("new build detected", "project", project, "build", buildID, "previous", prevbuildID)

	chroots, err := f.getChrootList(ctx, buildID)
	if err != nil {
		return fmt.Errorf("list chroots: %w", err)
	}
	var errs []error
	for _, ch := range chroots {
		if ch.State != "succeeded" || ch.ResultURL == "" {
			continue
		}
		if err := f.processChroot(ctx, project, ch, buildID); err != nil {
			errs = append(errs, fmt.Errorf("chroot %s: %w", ch.Name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	f.setLastBuildID(project, buildID)
	return nil
}

func (f *CoprClient) processChroot(ctx context.Context, project string, ch buildChroot, buildID int) error {
	pkgs, err := f.getBuiltPackages(ctx, buildID, ch.Name)
	if err != nil {
		return fmt.Errorf("list packages: %w", err)
	}
	for _, pkg := range pkgs {
		if pkg.Name != packageName || pkg.Arch == "src" {
			continue
		}
		if err := f.downloadRPM(ctx, project, ch, pkg); err != nil {
			return err
		}
	}
	return nil
}

func projectDirName(project string) string {
	if project == masterProject {
		return nightlyDir
	}
	return project
}

func rpmFileName(pkg rpmPackage) string {
	return fmt.Sprintf("%s-%s-%s.%s.rpm", pkg.Name, pkg.Version, pkg.Release, pkg.Arch)
}

func (f *CoprClient) downloadRPM(ctx context.Context, project string, ch buildChroot, pkg rpmPackage) error {
	destDir := filepath.Join(packageDir, projectDirName(project), ch.Name)
	if err := os.MkdirAll(destDir, 0o775); err != nil {
		return fmt.Errorf("create dest dir: %w", err)
	}

	fileName := rpmFileName(pkg)
	destPath := filepath.Join(destDir, fileName)

	if _, err := os.Stat(destPath); err == nil {
		slog.Debug("package already exists", "chroot", ch.Name, "file", fileName)
		return replaceSymlink(destDir, destPath)
	}

	downloadURL := strings.TrimRight(ch.ResultURL, "/") + "/" + fileName
	slog.Info("downloading", "url", downloadURL)

	if err := f.fetchToFile(ctx, downloadURL, destPath); err != nil {
		return err
	}
	slog.Info("download complete", "path", destPath)

	if project == v010Project && strings.Contains(fileName, "+") {
		if err := cleanDevBuildsExcept(destDir, fileName); err != nil {
			slog.Warn("clean dev builds failed", "chroot", ch.Name, "err", err)
		}
	}
	return replaceSymlink(destDir, destPath)
}

func (f *CoprClient) fetchToFile(ctx context.Context, url, destPath string) error {
	reqCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("download %s: http %d: %s", url, resp.StatusCode, body)
	}

	tmpPath := destPath + ".part"
	_ = os.Remove(tmpPath)

	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}

func replaceSymlink(dir, target string) error {
	link := filepath.Join(dir, currentSymlink)
	tmp := link + ".tmp"
	_ = os.Remove(tmp)
	if err := os.Symlink(filepath.Base(target), tmp); err != nil {
		return fmt.Errorf("create temp symlink: %w", err)
	}
	if err := os.Rename(tmp, link); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename symlink: %w", err)
	}
	return nil
}

func cleanDevBuildsExcept(dir, keep string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if name == keep {
			continue
		}
		if strings.Contains(name, "+") && strings.HasSuffix(name, ".rpm") {
			slog.Info("removing old dev build", "file", name)
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func (f *CoprClient) runQuery(ctx context.Context) {
	var wg sync.WaitGroup
	for _, project := range projects {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.processProject(ctx, project); err != nil {
				slog.Error("project failed", "project", project, "err", err)
			}
		}()
	}
	wg.Wait()
}

func main() {
	scheduleInterval := flag.Duration("schedule", 24*time.Hour, "interval to query Copr API")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	slog.Info("starting fecopack", "schedule", *scheduleInterval, "projects", projects)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	f := NewCoprClient()

	f.runQuery(ctx)

	ticker := time.NewTicker(*scheduleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.runQuery(ctx)
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		}
	}
}
