package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/xi2/xz"
)

func readgz(file io.Reader) *tar.Reader {
	gz, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	t := tar.NewReader(gz)
	return t
}

func readxz(file io.Reader) *tar.Reader {
	xzip, err := xz.NewReader(file, 0)
	if err != nil {
		panic(err)
	}
	t := tar.NewReader(xzip)

	return t
}

func main() {
	var t *tar.Reader
	var line, tarfile string
	var err error

	if len(os.Args) > 1 {
		tarfile = os.Args[1]
	} else {
		fmt.Println("Usage: ", os.Args[0], "<tarfile>")
		os.Exit(-1)
	}

	fmt.Println("Start indexing of " + tarfile)

	f, err := os.Open(tarfile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	switch filepath.Ext(tarfile) {
	case ".gz":
		t = readgz(f)
	case ".xz":
		t = readxz(f)
	default:
		fmt.Println("Extension not recognized", filepath.Ext(tarfile))
		os.Exit(-1)
	}

	for {
		h, err := t.Next()
		if err == io.EOF {
			break
		}

		fmt.Println("Read ", h.Name)
		if h.Typeflag == tar.TypeDir {
			continue
		}

		reader := bufio.NewReader(t)
		for {
			line, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(line)
		}
	}
}
