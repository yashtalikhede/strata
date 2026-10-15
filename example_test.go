package strata_test

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/yashtalikhede/strata"
)

// tempDir makes a directory for an example and returns it with a cleanup
// function. Examples cannot take a *testing.T, so they tidy up themselves.
func tempDir() (string, func()) {
	dir, err := os.MkdirTemp("", "strata-example-*")
	if err != nil {
		log.Fatal(err)
	}
	return dir, func() { os.RemoveAll(dir) }
}

func ExampleOpen() {
	dir, cleanup := tempDir()
	defer cleanup()

	store, err := strata.Open(dir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.Put([]byte("colour"), []byte("blue")); err != nil {
		log.Fatal(err)
	}
	value, err := store.Get([]byte("colour"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("colour is %s\n", value)

	// Output:
	// colour is blue
}

// ExampleOpen_options shows the settings worth knowing about: how large the
// in-memory table may grow, and how hard a write works to reach disk.
func ExampleOpen_options() {
	dir, cleanup := tempDir()
	defer cleanup()

	store, err := strata.Open(dir, &strata.Options{
		MemtableSize: 8 << 20,
		// The default is SyncAlways, which fsyncs every write. SyncNone
		// trades power-loss durability for speed.
		Sync: strata.SyncNone,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	for _, word := range []string{"one", "two", "three"} {
		if err := store.Put([]byte(word), []byte(word)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(store.Stats().SSTables, "tables on disk so far")

	// Output:
	// 0 tables on disk so far
}

func ExampleDB_Get() {
	dir, cleanup := tempDir()
	defer cleanup()

	store, err := strata.Open(dir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.Put([]byte("present"), []byte("here")); err != nil {
		log.Fatal(err)
	}

	for _, key := range []string{"present", "absent"} {
		value, err := store.Get([]byte(key))
		switch {
		case errors.Is(err, strata.ErrNotFound):
			fmt.Printf("%s: not found\n", key)
		case err != nil:
			log.Fatal(err)
		default:
			fmt.Printf("%s: %s\n", key, value)
		}
	}

	// Output:
	// present: here
	// absent: not found
}

func ExampleDB_Delete() {
	dir, cleanup := tempDir()
	defer cleanup()

	store, err := strata.Open(dir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.Put([]byte("temporary"), []byte("value")); err != nil {
		log.Fatal(err)
	}
	if err := store.Delete([]byte("temporary")); err != nil {
		log.Fatal(err)
	}

	_, err = store.Get([]byte("temporary"))
	fmt.Println("gone:", errors.Is(err, strata.ErrNotFound))

	// Deleting a key that is not there is not an error: the delete still has
	// to be recorded, in case an older table on disk holds a value for it.
	fmt.Println("deleting an absent key:", store.Delete([]byte("never-written")))

	// Output:
	// gone: true
	// deleting an absent key: <nil>
}
