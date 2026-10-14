// Command strata is the command-line entry point for the strata storage engine.
package main

import (
	"flag"
	"fmt"
	"os"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("strata %s\n", version)
		os.Exit(0)
	}

	fmt.Printf("strata %s\n", version)
}
