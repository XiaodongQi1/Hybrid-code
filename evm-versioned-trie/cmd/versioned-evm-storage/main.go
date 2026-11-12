package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/storagebench"
)

func main() {
	config := flag.String("config", "", "comparison config JSON")
	out := flag.String("out", "", "new output directory")
	base := flag.String("base-dir", "", "base for relative paths")
	flag.Parse()
	if *config == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *base != "" {
		if !filepath.IsAbs(*config) {
			*config = filepath.Join(*base, *config)
		}
		if !filepath.IsAbs(*out) {
			*out = filepath.Join(*base, *out)
		}
	}
	s, e := storagebench.Run(*config, *out)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	fmt.Printf("Completed %d methods, %d blocks, %d source EVM transactions\n", len(s.Methods), s.Blocks, s.Transactions)
}
