// Command strata is the command-line interface to the strata storage engine.
//
// It opens a database directory, performs one operation and exits, or opens an
// interactive shell. Run "strata help" for the commands.
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
