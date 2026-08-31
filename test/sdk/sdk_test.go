// Package sdk holds the TASK-033 lightweight integration tests for the frozen
// public surface github.com/sajadbayatani/slive/pkg/slive.
//
// These tests validate the SDK contract from the outside, the way a consumer
// sees it: they exec the real "go doc" and "go run" toolchain commands against
// the repository instead of importing internal packages. Nothing here touches
// the network — the module cache is pinned to the repository-local
// .gocache/mod when it exists, GOPROXY is disabled, and every example uses a
// STUN-free SDKConfig (cfg.STUNServers = []string{}), so ICE gathering stays
// in-process.
//
// Determinism budget: each toolchain invocation is capped at 45s, far below
// the 90s per-test ceiling; the three examples together run in ~5s warm.
package sdk

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// commandTimeout bounds every go toolchain invocation so a wedged build or a
// hung example fails the test instead of burning the CI budget.
const commandTimeout = 45 * time.Second

// readmeSnippetLineBudget is sprint-07's acceptance limit for the SDK helper
// flow ("NewClient, JoinRoom, PublishTrack, SubscribeTrack, LeaveRoom, and
// Snapshot health metrics in <20 lines"), counted over the whole program:
// package clause, imports and main included. README.md states this counting
// rule explicitly, and assertREADMETracksPublicSurface enforces it.
const readmeSnippetLineBudget = 20

// repoRoot returns the absolute repository root. Tests run with their working
// directory set to their own package directory (test/sdk), so the root is the
// canonical "../..".
func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s (this test must run from test/sdk): %v", root, err)
	}
	return root
}

// goCommand builds an exec.Cmd for the go toolchain rooted at the repository,
// with an absolute GOMODCACHE so the test works from any working directory.
func goCommand(t *testing.T, root string, args ...string) *exec.Cmd {
	t.Helper()

	goBin := "go"
	if runtime.GOOS == "windows" {
		goBin = "go.exe"
	}
	path, err := exec.LookPath(goBin)
	if err != nil {
		t.Skipf("go toolchain not available via PATH (%v): skipping toolchain-dependent test", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), offlineEnv(root)...)
	return cmd
}

// offlineEnv keeps runs reproducible and network-free: it pins GOMODCACHE to
// the repository-local cache when that cache exists and disables the proxy so
// a missing dependency fails fast instead of reaching the network. When the
// local cache is absent (fresh CI checkout with a shared cache) the ambient
// module cache is used untouched.
func offlineEnv(root string) []string {
	cache := filepath.Join(root, ".gocache", "mod")
	if info, err := os.Stat(cache); err != nil || !info.IsDir() {
		return nil
	}
	return []string{"GOMODCACHE=" + cache, "GOPROXY=off"}
}

// runGo executes the go toolchain in root and returns combined stdout+stderr,
// failing the test with the command's output on any error.
func runGo(t *testing.T, root string, args ...string) string {
	t.Helper()

	output, err := goCommand(t, root, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

// surfaceSymbol is one exported public-surface entry that must be both
// declared and documented in `go doc -all ./pkg/slive` output.
type surfaceSymbol struct {
	name string
	// decl is a regular expression matched against the whole go doc output;
	// it must locate the declaration line for the symbol.
	decl string
}

// stableSurface is the TASK-031 frozen surface plus the TASK-032 signaling
// helpers. It mirrors the "What is stable" section of pkg/slive/doc.go and the
// table in docs/sdk.md, so adding an exported symbol without extending this
// list and VERSIONING.md is caught here.
var stableSurface = []surfaceSymbol{
	{"Client", `(?m)^type Client struct \{`},
	{"Room", `(?m)^type Room = `},
	{"Participant", `(?m)^type Participant = `},
	{"Track", `(?m)^type Track = `},
	{"TrackKind", `(?m)^type TrackKind = `},
	{"TrackSource", `(?m)^type TrackSource = `},
	{"RoomState", `(?m)^type RoomState = `},
	{"ParticipantState", `(?m)^type ParticipantState = `},
	{"TrackState", `(?m)^type TrackState = `},
	{"SDKConfig", `(?m)^type SDKConfig struct \{`},
	{"Config", `(?m)^type Config = SDKConfig`},
	{"MetricsSnapshot", `(?m)^type MetricsSnapshot = `},
	{"ForwarderConfig", `(?m)^type ForwarderConfig = `},
	{"PeerConnectionConfig", `(?m)^type PeerConnectionConfig = `},
	{"RoomManager", `(?m)^type RoomManager = `},
	{"Handler", `(?m)^type Handler = `},
	{"HandlerOption", `(?m)^type HandlerOption = `},
	{"DiagnosticsSnapshoter", `(?m)^type DiagnosticsSnapshoter interface \{`},
	{"Session", `(?m)^type Session struct \{`},
	{"NewClient", `(?m)^func NewClient\(cfg SDKConfig\) \(\*Client, error\)`},
	{"DefaultSDKConfig", `(?m)^func DefaultSDKConfig\(\) SDKConfig`},
	{"NewRoomManager", `(?m)^func NewRoomManager\(\) \*RoomManager`},
	{"NewHandler", `(?m)^func NewHandler\(rm \*RoomManager, opts \.\.\.HandlerOption\) \*Handler`},
	{"WithGCTTL", `(?m)^func WithGCTTL\(d time\.Duration\) HandlerOption`},
	{"WithMetricsSnapshot", `(?m)^func WithMetricsSnapshot\(fn func\(\) MetricsSnapshot\) HandlerOption`},
	{"WithDiagnosticsSnapshoter", `(?m)^func WithDiagnosticsSnapshoter\(s DiagnosticsSnapshoter\) HandlerOption`},
	{"DefaultQueueSize", `(?m)^const DefaultQueueSize = `},
	{"TrackKindAudio", `(?m)TrackKindAudio = domain\.TrackKindAudio`},
	{"TrackKindVideo", `(?m)TrackKindVideo = domain\.TrackKindVideo`},
	{"TrackSourceMicrophone", `(?m)TrackSourceMicrophone = domain\.TrackSourceMicrophone`},
	{"TrackSourceCamera", `(?m)TrackSourceCamera = domain\.TrackSourceCamera`},
	{"TrackSourceScreenShare", `(?m)TrackSourceScreenShare = domain\.TrackSourceScreenShare`},
	{"ErrSessionClosed", `(?m)^var ErrSessionClosed = `},
}

// clientMethods is the frozen Client method set; sessionMethods the frozen
// Session method set. Both are rendered as methods in go doc output.
var (
	clientMethods  = []string{"JoinRoom", "LeaveRoom", "PublishTrack", "SubscribeTrack", "UnsubscribeTrack", "Snapshot", "Close", "Connect", "HTTPHandler", "SignalingURL", "RoomManager", "Handler"}
	sessionMethods = []string{"PublishTrack", "SubscribeTrack", "Close", "RoomID", "ParticipantID"}
)

// errorSentinels are the stable errors that must appear with docs.
var errorSentinels = []string{
	"ErrRoomClosed", "ErrRoomAlreadyExists", "ErrRoomNotFound",
	"ErrParticipantAlreadyExists", "ErrParticipantNotFound", "ErrParticipantLeft",
	"ErrTrackAlreadyPublished", "ErrTrackAlreadySubscribed", "ErrTrackNotFound",
	"ErrInvalidTrackKind", "ErrInvalidTrackSource", "ErrTrackNotPublished",
	"ErrTrackNotReady", "ErrPeerConnectionClosed", "ErrNoPeerConnection",
	"ErrInvalidSDP", "ErrInvalidICECandidate",
}

// errSymbol matches an error-sentinel *declaration* line in go doc output: a
// single-tab indented line inside a `var ( ... )` block, or a top-level
// `var ErrName = ...`. Prose that merely mentions a sentinel — the package doc
// says "var ErrXXX = domain.ErrXXX" — never sits in declaration position, so it
// is not matched and cannot be mistaken for an exported symbol.
var errSymbol = regexp.MustCompile(`(?m)^(?:var[ \t]+|\t)(Err[A-Z]\w*) = `)

// TestSDK_PublicSurface_GoDoc runs `go doc -all ./pkg/slive` — the command a
// developer actually uses to discover the API — and asserts every frozen
// symbol is present and carries a doc comment. It also checks that README.md's
// SDK section stays copy-pasteable (install line, example commands) and that
// the slive.X identifiers it references really exist in the package, which is
// the cheap guard against doc drift called out in TASK-033's risk list.
func TestSDK_PublicSurface_GoDoc(t *testing.T) {
	root := repoRoot(t)
	out := runGo(t, root, "doc", "-all", "./pkg/slive")

	for _, sym := range stableSurface {
		t.Run("surface/"+sym.name, func(t *testing.T) {
			if !regexp.MustCompile(sym.decl).MatchString(out) {
				t.Errorf("go doc -all ./pkg/slive does not declare %s (want %s)", sym.name, sym.decl)
			}
			assertDocumented(t, out, sym.name)
		})
	}

	for _, m := range clientMethods {
		t.Run("method/Client."+m, func(t *testing.T) {
			if !strings.Contains(out, "(c *Client) "+m+"(") {
				t.Errorf("go doc -all ./pkg/slive is missing method Client.%s", m)
			}
			assertDocumented(t, out, m)
		})
	}

	for _, m := range sessionMethods {
		t.Run("method/Session."+m, func(t *testing.T) {
			if !strings.Contains(out, "(s *Session) "+m+"(") {
				t.Errorf("go doc -all ./pkg/slive is missing method Session.%s", m)
			}
			assertDocumented(t, out, m)
		})
	}

	for _, sentinel := range errorSentinels {
		t.Run("errors/"+sentinel, func(t *testing.T) {
			if !strings.Contains(out, sentinel) {
				t.Errorf("go doc -all ./pkg/slive is missing error sentinel %s", sentinel)
			}
			assertDocumented(t, out, sentinel)
		})
	}

	t.Run("errors/completeness", func(t *testing.T) {
		// Reverse direction: any Err… symbol the package exports must be
		// pinned in exportedSentinels, so a new sentinel cannot land without a
		// docs/sdk.md row and a CHANGELOG entry.
		pinned := map[string]bool{}
		for _, e := range exportedSentinels {
			pinned[e.name] = true
		}
		for _, match := range errSymbol.FindAllStringSubmatch(out, -1) {
			name := match[1]
			if !pinned[name] {
				t.Errorf("pkg/slive exports %s but test/sdk does not pin it; add it to "+
					"exportedSentinels in errors_test.go, the docs/sdk.md table and CHANGELOG.md", name)
			}
		}
	})

	t.Run("pkgdoc/stability-contract", func(t *testing.T) {
		// The package comment must state the stable-facade / unstable-internal
		// split, which is the contract VERSIONING.md and docs/sdk.md rest on.
		for _, want := range []string{
			"Package slive is the stable public Go API",
			"internal/* remains unstable",
			"github.com/sajadbayatani/slive/pkg/slive",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("package doc is missing the stability statement %q", want)
			}
		}
	})

	t.Run("readme/tracks-public-surface", func(t *testing.T) {
		assertREADMETracksPublicSurface(t, root, out)
	})
}

// TestSDK_ExamplesRun executes the three shipped examples with `go run` from
// the repository root, exactly as a reader of README.md would, and asserts
// exit 0 plus the log lines each example's own README promises. The examples
// use STUN-free SDKConfig fixtures and in-process httptest servers, so this
// test needs no network and finishes in seconds.
func TestSDK_ExamplesRun(t *testing.T) {
	root := repoRoot(t)

	tests := []struct {
		pkg  string
		want []string
	}{
		{
			pkg: "./examples/basic-room",
			want: []string{
				"room joined: room-001",
				"rooms_active: 1",
				"participants_active: 2",
				"basic-room: exit 0",
			},
		},
		{
			pkg: "./examples/publish-subscribe",
			want: []string{
				"track published",
				"track subscribed",
				"forwarder_subscribers: 1",
				"forwarder_dropped_total monotonic:",
				"publish-subscribe: exit 0",
			},
		},
		{
			pkg: "./examples/health",
			want: []string{
				"health check 1: status=ok",
				"health check 2: status=ok",
				"health check 3: status=ok",
				"rooms_active=1",
				"health: exit 0",
			},
		},
	}

	for _, tc := range tests {
		t.Run(strings.TrimPrefix(tc.pkg, "./examples/"), func(t *testing.T) {
			logs := runGo(t, root, "run", tc.pkg)
			for _, want := range tc.want {
				if !strings.Contains(logs, want) {
					t.Errorf("go run %s output is missing %q\ngot:\n%s", tc.pkg, want, logs)
				}
			}
			if strings.Contains(logs, "level=ERROR") || strings.Contains(logs, "panic:") {
				t.Errorf("go run %s logged a failure:\n%s", tc.pkg, logs)
			}
		})
	}
}

// docForName matches the four-space paragraph go doc -all prints under a
// declaration; commentForName matches a source comment that opens with the
// symbol name, which is how grouped consts and vars get documented.
func docForName(name string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^    ` + regexp.QuoteMeta(name) + `\b`)
}

func commentForName(name string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^[ \t]*//[ \t]*` + regexp.QuoteMeta(name) + `\b`)
}

// assertDocumented fails when name has no doc text in the go doc output. Go doc
// only renders a paragraph for a symbol with a doc comment, so a matching
// paragraph (or a name-leading source comment for grouped declarations) is
// proof the symbol is documented.
func assertDocumented(t *testing.T, out, name string) {
	t.Helper()

	if docForName(name).MatchString(out) || commentForName(name).MatchString(out) {
		return
	}
	t.Errorf("%s has no doc comment in go doc -all ./pkg/slive output", name)
}

// readmeFence captures ```go fenced blocks in README.md.
var readmeFence = regexp.MustCompile("(?s)```go\n(.*?)```")

// sliveRef matches `slive.Symbol` usage in the README snippet.
var sliveRef = regexp.MustCompile(`slive\.([A-Za-z_][A-Za-z0-9_]*)`)

// assertREADMETracksPublicSurface checks the README SDK section is usable and
// current: the install command, links to all three example READMEs, working
// `go run` commands, links to docs/sdk.md, VERSIONING.md and CHANGELOG.md, and
// a snippet whose slive.* identifiers all exist in the package. It also holds
// the snippet to sprint-07's shape: a complete program of at most
// readmeSnippetLineBudget lines that still walks
// NewClient → JoinRoom → PublishTrack → SubscribeTrack → Snapshot → LeaveRoom →
// Close (gap G-2).
func assertREADMETracksPublicSurface(t *testing.T, root, godoc string) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	readme := string(data)

	if !strings.Contains(readme, "go get github.com/sajadbayatani/slive/pkg/slive") {
		t.Error("README.md SDK section is missing the `go get github.com/sajadbayatani/slive/pkg/slive` install command")
	}
	for _, example := range []string{"basic-room", "publish-subscribe", "health"} {
		if !strings.Contains(readme, "go run ./examples/"+example) {
			t.Errorf("README.md does not offer a copy-pasteable `go run ./examples/%s`", example)
		}
		if !strings.Contains(readme, "examples/"+example+"/README.md") {
			t.Errorf("README.md does not link examples/%s/README.md", example)
		}
	}
	for _, link := range []string{"docs/sdk.md", "VERSIONING.md", "CHANGELOG.md"} {
		if !strings.Contains(readme, link) {
			t.Errorf("README.md does not link %s", link)
		}
	}

	blocks := readmeFence.FindAllStringSubmatch(readme, -1)
	if len(blocks) == 0 {
		t.Fatal("README.md has no ```go code fence for the SDK snippet")
	}

	// Gap G-2: sprint-07's acceptance is the helper flow in fewer than 20
	// lines. The counting rule the README states is the strict one — the fence
	// holds the whole program, package and imports included — so guard that
	// reading rather than letting the snippet creep back to 40 lines.
	snippet := strings.TrimRight(blocks[0][1], "\n")
	if lines := len(strings.Split(snippet, "\n")); lines > readmeSnippetLineBudget {
		t.Errorf("README.md SDK snippet is %d lines; the sprint-07 budget is %d (see the counting rule under “Minimal example”)",
			lines, readmeSnippetLineBudget)
	}
	for _, call := range []string{
		"slive.NewClient", "client.JoinRoom", "client.PublishTrack",
		"client.SubscribeTrack", "client.LeaveRoom", "client.Snapshot()", "client.Close()",
	} {
		if !strings.Contains(snippet, call) {
			t.Errorf("README.md SDK snippet never uses %s, so it no longer demonstrates the documented flow", call)
		}
	}
	if !strings.Contains(snippet, "func main()") {
		t.Error("README.md SDK snippet is not a complete program (no func main), so it cannot be pasted and run")
	}

	checked := 0
	for _, block := range blocks {
		for _, ref := range sliveRef.FindAllStringSubmatch(block[1], -1) {
			checked++
			if !strings.Contains(godoc, ref[1]) {
				t.Errorf("README.md snippet references slive.%s, which is absent from go doc -all ./pkg/slive", ref[1])
			}
		}
	}
	if checked == 0 {
		t.Error("README.md snippet references no slive.* symbols; it is not a real usage example")
	}
}
