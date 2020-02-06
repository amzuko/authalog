package authalog

import (
	"fmt"
	"os"
	"testing"
)

func checkFile(filename string) error {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return err
	}
	db := NewDatabase()
	cmds, err := db.Parse(f)
	if err != nil {
		return err
	}
	var results []result
	for _, c := range cmds {
		results, err = db.Apply(c)
		if err != nil {
			return err
		}
	}
	if len(results) != 1 {
		return fmt.Errorf("Expected a single result")
	}
	return nil
}

func BenchmarkClique(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := checkFile("tests/clique200.pl")
		if err != nil {
			b.Error(err)
		}
	}
}
