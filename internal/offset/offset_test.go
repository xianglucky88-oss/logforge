package offset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := New(filepath.Join(dir, "offset.json"))

	f, info, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if info.RecoveredFromBak || info.Migrated {
		t.Fatalf("fresh load should not recover or migrate: %+v", info)
	}
	if len(f.Paths) != 0 {
		t.Fatal("expected empty paths for missing file")
	}

	f.Paths["/tmp/a.log"] = &PathState{
		Current: "1:2",
		Identities: map[FileID]*State{
			"1:2": {Offset: 1234, Pending: []byte(`{"n":1`), Size: 1240, Updated: 7},
		},
	}
	if err := s.Save(f); err != nil {
		t.Fatal(err)
	}

	got, info, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if info.RecoveredFromBak {
		t.Fatal("unexpected bak recovery")
	}
	st := got.Paths["/tmp/a.log"].Identities["1:2"]
	if st == nil || st.Offset != 1234 || string(st.Pending) != `{"n":1` || st.Size != 1240 {
		t.Fatalf("state mismatch: %+v", st)
	}
	if got.Paths["/tmp/a.log"].Current != "1:2" {
		t.Fatalf("current mismatch: %+v", got.Paths["/tmp/a.log"])
	}
	if got.Version != currentVersion {
		t.Fatalf("version mismatch: %d", got.Version)
	}
}

func TestLoadRecoversFromBak(t *testing.T) {
	dir := t.TempDir()
	s := New(filepath.Join(dir, "offset.json"))

	f := &File{Version: currentVersion, Paths: map[string]*PathState{
		"/tmp/a.log": {Current: "1:2", Identities: map[FileID]*State{"1:2": {Offset: 42}}},
	}}
	// 保存两次，让 .bak 存有上一代内容。
	if err := s.Save(f); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(f); err != nil {
		t.Fatal(err)
	}
	// 主文件写坏，.bak 还是上一次的好内容。
	if err := os.WriteFile(s.path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, info, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !info.RecoveredFromBak {
		t.Fatal("expected recovery from .bak")
	}
	if got.Paths["/tmp/a.log"].Identities["1:2"].Offset != 42 {
		t.Fatalf("unexpected state: %+v", got.Paths["/tmp/a.log"])
	}
}

func TestLoadDetectsCrcTamper(t *testing.T) {
	dir := t.TempDir()
	s := New(filepath.Join(dir, "offset.json"))

	f := &File{Version: currentVersion, Paths: map[string]*PathState{
		"/tmp/a.log": {Current: "1:2", Identities: map[FileID]*State{"1:2": {Offset: 42}}},
	}}
	if err := s.Save(f); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(f); err != nil {
		t.Fatal(err)
	}
	// 同时写坏主文件和备份（改 data 但保留结构）→ ErrCorrupt。
	raw, _ := os.ReadFile(s.path)
	tampered := []byte(string(raw)[:len(raw)/2] + `"offset": 9999,` + string(raw[len(raw)/2:]))
	for _, p := range []string{s.path, s.bakPath()} {
		if err := os.WriteFile(p, tampered, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.Load(); err != ErrCorrupt {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestLoadMigratesV1(t *testing.T) {
	dir := t.TempDir()
	s := New(filepath.Join(dir, "offset.json"))
	if err := os.WriteFile(s.path, []byte(`{"version":1,"offsets":{"/tmp/a.log":1234}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	got, info, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !info.Migrated {
		t.Fatal("expected migration flag")
	}
	ps := got.Paths["/tmp/a.log"]
	if ps == nil || ps.LegacyOffset != 1234 {
		t.Fatalf("legacy offset not migrated: %+v", ps)
	}
}

func TestTrackerResolveAndPendingAlignment(t *testing.T) {
	tr := NewTracker("/tmp/a.log", &File{Version: currentVersion, Paths: map[string]*PathState{}})

	tr.MarkCurrent("1:2")
	tr.Resolve("1:2", 100)
	tr.Resolve("1:2", 200)

	// 对齐的半行被保存，不对齐的被丢弃。
	attached, dropped, bytes := tr.AttachReports([]ReadReport{
		{ID: "1:2", ReadPos: 210, PendingStart: 200, Pending: []byte("0123456789"), Size: 210},
		{ID: "1:3", ReadPos: 55, PendingStart: 50, Pending: []byte("abcde"), Size: 55},
	})
	if attached != 1 || dropped != 1 || bytes != 10 {
		t.Fatalf("attach mismatch: %d %d %d", attached, dropped, bytes)
	}

	snap := tr.Snapshot()
	st := snap.Paths["/tmp/a.log"].Identities["1:2"]
	if st.Offset != 200 || string(st.Pending) != "0123456789" || st.Size != 210 {
		t.Fatalf("snapshot mismatch: %+v", st)
	}
	if snap.Paths["/tmp/a.log"].Current != "1:2" {
		t.Fatal("current not tracked")
	}

	// 位点推进后旧半行失效。
	tr.Resolve("1:2", 210)
	if st := tr.Snapshot().Paths["/tmp/a.log"].Identities["1:2"]; st.Pending != nil {
		t.Fatalf("pending should be cleared after resolve: %+v", st)
	}
}

func TestTrackerResetOnTruncate(t *testing.T) {
	tr := NewTracker("/tmp/a.log", &File{Version: currentVersion, Paths: map[string]*PathState{}})
	tr.Resolve("1:2", 500)
	tr.Reset("1:2")
	tr.Resolve("1:2", 30)
	st := tr.Snapshot().Paths["/tmp/a.log"].Identities["1:2"]
	if st.Offset != 30 {
		t.Fatalf("offset should regrow from 0 after reset, got %d", st.Offset)
	}
}
