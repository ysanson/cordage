//go:build realprocess

package distribute

import (
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func buildCordageBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "cordage")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/cordage")
	cmd.Dir = "../.." // internal/distribute -> repo root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build cordage: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

func waitForPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("worker at %s never came up", addr)
}

func startWorkerProcess(t *testing.T, bin, addr string) {
	t.Helper()
	cmd := exec.Command(bin, "worker", "--listen", addr)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	waitForPort(t, addr, 5*time.Second)
}

func writeRealProcessSchema(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "schema.json")
	schemaJSON := `{"delimiter":";","hasHeader":true,"columns":[{"name":"station","type":0,"kind":0},{"name":"temperature","type":2,"kind":1}]}`
	if err := os.WriteFile(p, []byte(schemaJSON), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	return p
}

var statLineRe = regexp.MustCompile(`^station=(.*?) count=(\d+) min\(temperature\)=([-\d.]+) avg\(temperature\)=([-\d.]+) max\(temperature\)=([-\d.]+)$`)

type stationStat struct {
	count         int64
	min, avg, max float64
}

func parseRunOutput(t *testing.T, output string) map[string]stationStat {
	t.Helper()
	out := make(map[string]stationStat)
	for _, line := range strings.Split(output, "\n") {
		m := statLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		count, _ := strconv.ParseInt(m[2], 10, 64)
		mn, _ := strconv.ParseFloat(m[3], 64)
		avg, _ := strconv.ParseFloat(m[4], 64)
		mx, _ := strconv.ParseFloat(m[5], 64)
		out[m[1]] = stationStat{count: count, min: mn, avg: avg, max: mx}
	}
	if len(out) == 0 {
		t.Fatalf("no station lines parsed from output:\n%s", output)
	}
	return out
}

// TestDistributedRealProcesses is the milestone's literal exit criterion:
// spin up N real `cordage worker` OS processes plus a real `cordage
// coordinator` process (all via exec.Command against a built binary, not
// in-process goroutines), and confirm the result matches a real,
// single-node `cordage run` process over the same file -- count/min/max
// exactly, avg within floating-point tolerance (summing the same values
// in a different grouping order isn't bit-exact; see the loopback test's
// assertResultsEqual doc comment in distribute_test.go for the same
// point made about the fast dev-loop tier).
//
// Gated behind the "realprocess" build tag so plain `go test ./...` (CI,
// everyday iteration) never spawns real processes or shells out to `go
// build`; run explicitly:
//
//	go test -tags realprocess ./internal/distribute/... -run TestDistributedRealProcesses -v
func TestDistributedRealProcesses(t *testing.T) {
	bin := buildCordageBinary(t)

	dir := t.TempDir()
	dataPath, _ := writeTestCSV(t, 20000)
	schemaPath := writeRealProcessSchema(t, dir)

	const numWorkers = 3
	addrs := make([]string, numWorkers)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	for _, addr := range addrs {
		startWorkerProcess(t, bin, addr)
	}

	coordOut, err := exec.Command(bin, "coordinator",
		"--file", dataPath,
		"--schema", schemaPath,
		"--group-by", "station",
		"--measure", "temperature:min,avg,max",
		"--workers", strings.Join(addrs, ","),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("coordinator: %v\n%s", err, coordOut)
	}

	runOut, err := exec.Command(bin, "run",
		"--file", dataPath,
		"--schema", schemaPath,
		"--group-by", "station",
		"--measure", "temperature:min,avg,max",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, runOut)
	}

	compareStationStats(t, parseRunOutput(t, string(coordOut)), parseRunOutput(t, string(runOut)))
}

// compareStationStats compares a distributed run's per-station stats
// against a single-node reference: count/min/max must match exactly,
// avg only within floating-point tolerance (summing the same values in a
// different grouping order isn't bit-exact -- see TestDistributedRealProcesses's
// doc comment).
func compareStationStats(t *testing.T, dist, single map[string]stationStat) {
	t.Helper()
	if len(dist) != len(single) {
		t.Fatalf("got %d stations distributed, %d single-node", len(dist), len(single))
	}
	for station, d := range dist {
		s, ok := single[station]
		if !ok {
			t.Errorf("station %q present in distributed output, missing from single-node", station)
			continue
		}
		if d.count != s.count {
			t.Errorf("%s: count = %d, want %d", station, d.count, s.count)
		}
		if d.min != s.min {
			t.Errorf("%s: min = %v, want %v", station, d.min, s.min)
		}
		if d.max != s.max {
			t.Errorf("%s: max = %v, want %v", station, d.max, s.max)
		}
		if math.Abs(d.avg-s.avg) > 1e-6 {
			t.Errorf("%s: avg = %v, want %v (within tolerance)", station, d.avg, s.avg)
		}
	}
}

func startCoordinatorListenProcess(t *testing.T, bin, addr, dataPath, schemaPath, workerAddrsCSV string) {
	t.Helper()
	cmd := exec.Command(bin, "coordinator",
		"--listen", addr,
		"--file", dataPath,
		"--schema", schemaPath,
		"--workers", workerAddrsCSV,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	waitForPort(t, addr, 5*time.Second)
}

// TestQueryRealProcess is M4's own literal exit criterion: a real
// `cordage coordinator --listen` process, real `cordage worker`
// processes, and a real `cordage query` process, all talking over real
// gRPC -- compared against a real single-node `cordage run` process over
// the same file. Requests MIN/AVG/MAX (not just AVG) so the query
// path's stdout matches statLineRe/parseRunOutput's existing regex, with
// no new parsing code needed.
//
//	go test -tags realprocess ./internal/distribute/... -run TestQueryRealProcess -v
func TestQueryRealProcess(t *testing.T) {
	bin := buildCordageBinary(t)
	dir := t.TempDir()
	dataPath, _ := writeTestCSV(t, 20000)
	schemaPath := writeRealProcessSchema(t, dir)

	const numWorkers = 3
	workerAddrs := make([]string, numWorkers)
	for i := range workerAddrs {
		workerAddrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	for _, addr := range workerAddrs {
		startWorkerProcess(t, bin, addr)
	}

	coordAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	startCoordinatorListenProcess(t, bin, coordAddr, dataPath, schemaPath, strings.Join(workerAddrs, ","))

	queryOut, err := exec.Command(bin, "query",
		"--grpc", coordAddr,
		"SELECT station, MIN(temperature), AVG(temperature), MAX(temperature) GROUP BY station",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("query: %v\n%s", err, queryOut)
	}

	runOut, err := exec.Command(bin, "run",
		"--file", dataPath, "--schema", schemaPath,
		"--group-by", "station", "--measure", "temperature:min,avg,max",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, runOut)
	}

	compareStationStats(t, parseRunOutput(t, string(queryOut)), parseRunOutput(t, string(runOut)))
}
