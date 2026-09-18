package pipeline

import (
	"os"
	"strings"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	in := "-- comment\nCREATE DATABASE IF NOT EXISTS iot;\n\nCREATE STABLE a (x INT); -- trailing\n  ;\nCREATE STREAM s AS SELECT 1\n  FROM b INTERVAL(1h)\n"
	got := SplitStatements(in)
	want := []string{"CREATE DATABASE IF NOT EXISTS iot", "CREATE STABLE a (x INT)", "CREATE STREAM s AS SELECT 1\n  FROM b INTERVAL(1h)"}
	if len(got) != len(want) {
		t.Fatalf("got %d stmts: %q", len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, got[i], want[i])
		}
	}
}

func TestSplitStatementsRealFile(t *testing.T) {
	b, err := os.ReadFile("../../sql/tdengine.sql")
	if err != nil {
		t.Skip("sql file not found")
	}
	got := SplitStatements(string(b))
	if len(got) != 4 {
		t.Fatalf("want 4 statements, got %d", len(got))
	}
	if !strings.HasPrefix(got[0], "CREATE DATABASE") || !strings.HasPrefix(got[3], "CREATE STREAM") {
		t.Fatalf("unexpected order: %q", got)
	}
}
