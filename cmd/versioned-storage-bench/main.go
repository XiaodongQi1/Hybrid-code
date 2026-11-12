package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"github.com/XiaodongQi1/Hybrid-code/internal/versionedbench"
)

func main() {
	config := flag.String("config", "", "JSON configuration (defaults to small synthetic matrix)")
	out := flag.String("out", "", "new output directory; never overwritten")
	flag.Parse()
	cfg := versionedbench.DefaultConfig()
	if *config != "" {
		f, e := os.Open(*config)
		if e != nil {
			fatal(e)
		}
		d := json.NewDecoder(f)
		d.DisallowUnknownFields()
		if e = d.Decode(&cfg); e != nil {
			f.Close()
			fatal(e)
		}
		var extra interface{}
		if e = d.Decode(&extra); e != io.EOF {
			f.Close()
			fatal(fmt.Errorf("trailing JSON"))
		}
		f.Close()
	}
	if *out == "" {
		fatal(fmt.Errorf("-out is required"))
	}
	if e := versionedbench.Run(cfg, *out, os.Stdout); e != nil {
		fatal(e)
	}
}
func fatal(e error) { fmt.Fprintln(os.Stderr, e); os.Exit(1) }
