package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"github.com/xi2/xz"
)

func readgz(file io.Reader) *gzip.Reader {
	res, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	return res
}

func readxz(file io.Reader) *xz.Reader {
	res, err := xz.NewReader(file, 0)
	if err != nil {
		panic(err)
	}

	return res
}

func main() {
	tarfile := os.Args[1]

	fmt.Println("Start indexing of " + tarfile)

	f, err := os.Open(tarfile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	switch filepath.Ext(tarfile) {
	case ".gz":
		readgz(f)
	case ".xz":
		readxz(f)
	default:
		fmt.Println("Extension not recognized", filepath.Ext(tarfile))
	}
}
