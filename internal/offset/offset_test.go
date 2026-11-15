package offset

import (
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := New(filepath.Join(dir, "offset.json"))

	f, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Offsets) != 0 {
		t.Fatal("expected empty offsets for missing file")
	}

	f.Offsets["/tmp/a.log"] = 1234
	if err := s.Save(f); err != nil {
		t.Fatal(err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Offsets["/tmp/a.log"] != 1234 {
		t.Fatalf("offset mismatch: %v", got.Offsets)
	}
	if got.Version != currentVersion {
		t.Fatalf("version mismatch: %d", got.Version)
	}
}
