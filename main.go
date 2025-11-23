package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ConsulNode struct {
	Node    string `json:"Node"`
	Address string `json:"Address"`
}

type ConsulService struct {
	ID      string   `json:"ServiceID"`
	Name    string   `json:"ServiceName"`
	Address string   `json:"ServiceAddress"`
	Port    int      `json:"ServicePort"`
	Tags    []string `json:"ServiceTags"`
}

type ConsulCheck struct {
	Status string `json:"Status"`
}

type ConsulServiceEntry struct {
	Node    ConsulNode
	Service ConsulService
	Checks  []ConsulCheck
}

type consulEndpoint struct {
	URL     string
	Latency time.Duration
	Alive   bool
}

type Group struct {
	wg      sync.WaitGroup
	errOnce sync.Once
	err     error
}

func (g *Group) Go(f func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic: %v", r)
				g.setErr(err)
				log.Error().Interface("panic", r).Msg("goroutine recovered")
			}
		}()
		if err := f(); err != nil {
			g.setErr(err)
		}
	}()
}

func (g *Group) setErr(err error) { g.errOnce.Do(func() { g.err = err }) }
func (g *Group) Wait() error      { g.wg.Wait(); return g.err }

var (
	dcsMap    = make(map[string][]consulEndpoint)
	dcsMu     sync.RWMutex
	cache     = make(map[string][]byte)
	cacheTime = make(map[string]time.Time)
	cacheMu   sync.RWMutex
	cacheTTL  = time.Second
	client    *http.Client
)

func main() {
	setupLogger()

	timeoutStr := getenv("CONSUL_TIMEOUT", "2s")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		timeout = 2 * time.Second
	}
	client = &http.Client{Timeout: timeout}
	log.Info().Dur("timeout", timeout).Msg("http client configured")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initDatacenters(ctx)
	updateLatenciesOnce(ctx)
	go updateLatenciesLoop(ctx)

	host := getenv("HOST", "0.0.0.0")
	port := getenv("PORT", "8501")
	addr := host + ":" + port

	mux("/v1/catalog/services", handleCatalogServices)
	mux("/v1/catalog/service/", handleCatalogService)
	mux("/v1/health/service/", handleHealthService)

	srv := &http.Server{
		Addr:    addr,
		Handler: http.DefaultServeMux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info().Str("listen", addr).Msg("proxy started")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("server ended with error")
	}
}

func setupLogger() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	l := strings.ToLower(os.Getenv("LOG_LEVEL"))
	switch l {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

func mux(path string, h func(http.ResponseWriter, *http.Request)) {
	http.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h(w, r)
		log.Info().
			Str("path", r.URL.Path).
			Str("remote", r.RemoteAddr).
			Dur("dur", time.Since(start)).
			Msg("request")
	})
}

func getenv(k, d string) string {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	return v
}

func initDatacenters(ctx context.Context) {
	raw := os.Getenv("DATACENTERS")
	if raw == "" {
		log.Fatal().Msg("DATACENTERS required")
	}

	tmp := map[string][]consulEndpoint{}
	var mu sync.Mutex
	var g Group

	for _, p := range strings.Split(raw, ",") {
		addr := strings.TrimSpace(p)
		if addr == "" {
			continue
		}
		if !strings.HasPrefix(addr, "http") {
			addr = "http://" + addr
		}
		base := strings.TrimRight(addr, "/")
		a := base

		g.Go(func() error {
			dc, err := fetchDC(ctx, a)
			if err != nil {
				log.Warn().Str("endpoint", a).Err(err).Msg("dc detect failed")
				dc = "__unknown"
			}
			mu.Lock()
			tmp[dc] = append(tmp[dc], consulEndpoint{URL: a, Alive: true})
			mu.Unlock()
			log.Info().Str("endpoint", a).Str("dc", dc).Msg("registered endpoint")
			return nil
		})
	}

	_ = g.Wait()
	if len(tmp) == 0 {
		log.Fatal().Msg("no valid endpoints after init")
	}

	dcsMu.Lock()
	dcsMap = tmp
	dcsMu.Unlock()
}

func fetchDC(ctx context.Context, base string) (string, error) {
	var r struct {
		Config struct {
			DC string `json:"Datacenter"`
		} `json:"Config"`
		DebugConfig struct {
			DC string `json:"Datacenter"`
		} `json:"DebugConfig"`
	}
	if err := getJSONCtx(ctx, base+"/v1/agent/self", &r); err != nil {
		return "", err
	}
	if r.Config.DC != "" {
		return r.Config.DC, nil
	}
	if r.DebugConfig.DC != "" {
		return r.DebugConfig.DC, nil
	}
	return "", fmt.Errorf("dc not found in agent/self for %s", base)
}

func updateLatenciesLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("latency updater stopped")
			return
		case <-ticker.C:
			updateLatenciesOnce(ctx)
		}
	}
}

func updateLatenciesOnce(ctx context.Context) {
	snap := snapshotDC()
	for dc, eps := range snap {
		for i := range eps {
			lat, ok := probeCtx(ctx, eps[i].URL)
			if ok {
				eps[i].Latency = lat
				eps[i].Alive = true
			} else {
				eps[i].Latency = time.Hour
				eps[i].Alive = false
			}
		}
		sort.Slice(eps, func(i, j int) bool { return eps[i].Latency < eps[j].Latency })
		snap[dc] = eps
		log.Debug().Str("dc", dc).Interface("endpoints", eps).Msg("latency_update")
	}
	dcsMu.Lock()
	dcsMap = snap
	dcsMu.Unlock()
}

func snapshotDC() map[string][]consulEndpoint {
	dcsMu.RLock()
	defer dcsMu.RUnlock()
	out := make(map[string][]consulEndpoint, len(dcsMap))
	for dc, eps := range dcsMap {
		cp := make([]consulEndpoint, len(eps))
		copy(cp, eps)
		out[dc] = cp
	}
	return out
}

func probeCtx(ctx context.Context, base string) (time.Duration, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/status/leader", nil)
	if err != nil {
		return 0, false
	}
	start := time.Now()
	res, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, false
	}
	_, _ = io.Copy(io.Discard, res.Body)
	return time.Since(start), true
}

func getJSONCtx(ctx context.Context, u string, dst interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status %d: %s", resp.StatusCode, body)
	}
	return json.Unmarshal(body, dst)
}

func pickDCQueryCtx(ctx context.Context, path string) []ConsulServiceEntry {
	snap := snapshotDC()
	results := []ConsulServiceEntry{}
	var wg Group
	var mu sync.Mutex

	for _, eps := range snap {
		epList := eps
		wg.Go(func() error {
			var out []ConsulServiceEntry
			_ = queryDCCtx(ctx, epList, path, &out)
			if len(out) == 0 {
				return nil
			}
			mu.Lock()
			results = append(results, out...)
			mu.Unlock()
			return nil
		})
	}

	_ = wg.Wait()
	return dedup(results)
}

func pickDCMapCtx(ctx context.Context, path string) map[string][]string {
	snap := snapshotDC()
	out := map[string][]string{}
	var wg Group
	var mu sync.Mutex

	for _, eps := range snap {
		epList := eps
		wg.Go(func() error {
			tmp := map[string][]string{}
			_ = queryDCCtx(ctx, epList, path, &tmp)
			if len(tmp) == 0 {
				return nil
			}
			mu.Lock()
			for k, v := range tmp {
				out[k] = append(out[k], v...)
			}
			mu.Unlock()
			return nil
		})
	}

	_ = wg.Wait()
	return out
}

func queryDCCtx(ctx context.Context, eps []consulEndpoint, path string, dst interface{}) error {
	var last error
	for _, ep := range eps {
		if !ep.Alive {
			continue
		}
		u := ep.URL + path
		err := getJSONCtx(ctx, u, dst)
		if err == nil {
			return nil
		}
		last = err

		dcsMu.Lock()
		for dcName, list := range dcsMap {
			for i := range list {
				if list[i].URL == ep.URL {
					list[i].Alive = false
					list[i].Latency = time.Hour
					dcsMap[dcName] = list
				}
			}
		}
		dcsMu.Unlock()

		log.Warn().
			Str("endpoint", ep.URL).
			Str("path", path).
			Err(err).
			Msg("request failed, endpoint marked dead, trying next")
	}
	if last == nil {
		return fmt.Errorf("no alive endpoints for path %s", path)
	}
	return last
}

func dedup(in []ConsulServiceEntry) []ConsulServiceEntry {
	if len(in) == 0 {
		return in
	}
	m := map[string]bool{}
	out := make([]ConsulServiceEntry, 0, len(in))
	for _, e := range in {
		k := e.Service.ID + e.Service.Address + e.Service.Name
		if !m[k] {
			m[k] = true
			out = append(out, e)
		}
	}
	return out
}

func cached(key string) ([]byte, bool) {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	if t, ok := cacheTime[key]; ok && time.Since(t) < cacheTTL {
		return cache[key], true
	}
	return nil, false
}

func store(key string, b []byte) {
	cacheMu.Lock()
	cache[key] = b
	cacheTime[key] = time.Now()
	cacheMu.Unlock()
}

func handleCatalogServices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := "svcs:" + r.URL.RawQuery
	if b, ok := cached(key); ok {
		w.Write(b)
		return
	}
	path := "/v1/catalog/services"
	if q := r.URL.RawQuery; q != "" {
		path += "?" + q
	}
	res := pickDCMapCtx(ctx, path)
	b, _ := json.Marshal(res)
	store(key, b)
	w.Write(b)
}

func handleCatalogService(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := strings.TrimPrefix(r.URL.Path, "/v1/catalog/service/")
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}
	key := "svc:" + name + ":" + r.URL.RawQuery
	if b, ok := cached(key); ok {
		w.Write(b)
		return
	}
	path := "/v1/catalog/service/" + url.PathEscape(name)
	if q := r.URL.RawQuery; q != "" {
		path += "?" + q
	}
	res := pickDCQueryCtx(ctx, path)
	b, _ := json.Marshal(res)
	store(key, b)
	w.Write(b)
}

func handleHealthService(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pathPart := strings.TrimPrefix(r.URL.Path, "/v1/health/service/")
	name := strings.Split(pathPart, "?")[0]
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}
	key := "hlth:" + name + ":" + r.URL.RawQuery
	if b, ok := cached(key); ok {
		w.Write(b)
		return
	}
	path := "/v1/health/service/" + url.PathEscape(name)
	if q := r.URL.RawQuery; q != "" {
		path += "?" + q
	}
	res := pickDCQueryCtx(ctx, path)
	b, _ := json.Marshal(res)
	store(key, b)
	w.Write(b)
}
