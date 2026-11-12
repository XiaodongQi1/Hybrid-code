package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/runner"
)

func main() {
	config := flag.String("config", "", "configuration JSON")
	output := flag.String("out", "", "output directory (new unless -resume)")
	resume := flag.Bool("resume", false, "continue the same workload and configuration in the existing output directory")
	base := flag.String("base-dir", "", "base directory for relative paths")
	flag.Parse()
	if *config == "" || *output == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *base != "" {
		if !filepath.IsAbs(*config) {
			*config = filepath.Join(*base, *config)
		}
		if !filepath.IsAbs(*output) {
			*output = filepath.Join(*base, *output)
		}
	}
	r, e := runner.RunWithOptions(*config, *output, runner.Options{Resume: *resume})
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if e = json.NewEncoder(os.Stdout).Encode(r); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
