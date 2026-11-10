package pythondeps

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkExtractImportScan(b *testing.B) {
	dir := b.TempDir()
	var sb strings.Builder
	sb.WriteString("import numpy\nimport os\nfrom torch import nn\n")
	for i := 0; i < 500; i++ {
		sb.WriteString("x = compute(value, other)\n")
	}
	script := filepath.Join(dir, "main.py")
	if err := os.WriteFile(script, []byte(sb.String()), 0o644); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		ExtractImportScan(script)
	}
}
