package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/xi2/xz"

	_ "github.com/mattn/go-sqlite3"
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

func process(tx *sql.Tx, line string) error {
	var split []string

	if strings.Contains(line, ";") {
		split = strings.Split(line, ";")
	} else if strings.Contains(line, ",") {
		split = strings.Split(line, ",")
	} else if strings.Contains(line, ":") {
		split = strings.Split(line, ":")
	} else {
		return errors.New("Separator not found in " + line)
	}

	email := strings.Split(split[0], "@")
	password := strings.TrimSuffix(strings.TrimSuffix(split[1], "\n"), "\r")

	if len(email) < 2 {
		email = append(email, "")
	}

	err := store(tx, email, password)
	if err != nil {
		return errors.New("Store failed for record: '" + line + "': " + err.Error())
	}

	return nil
}

func opendb() (*sql.DB, error) {
	var counted int
	var err error

	query := "SELECT count(*) FROM sqlite_master"

	db, err := sql.Open("sqlite3", "leak.db")
	if err != nil {
		return nil, errors.New("Database not opened: " + err.Error())
	}

	db.Exec("PRAGMA synchronous = OFF")
	db.Exec("PRAGMA journal_mode = WAL")

	if err := db.QueryRow(query).Scan(&counted); err != nil {
		return nil, errors.New("Database not opened: " + err.Error())
	}

	if counted == 0 {
		db.Exec("CREATE TABLE leak(domain, user, password)")
	}

	return db, nil
}

func store(tx *sql.Tx, email []string, password string) error {
	var err error

	stmt, err := tx.Prepare("INSERT INTO leak VALUES($1, $2, $3)")
	if err != nil {
		return errors.New("Statement error: " + err.Error())
	}

	_, err = stmt.Exec(email[1], email[0], password)
	if err != nil {
		tx.Rollback()
		return errors.New("Cannot save record: " + err.Error())
	}

	return nil
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

	db, err := opendb()
	if err != nil {
		fmt.Println("Cannot create database schema: " + err.Error())
		os.Exit(1)
	}
	defer db.Close()

	for {
		h, err := t.Next()
		if err == io.EOF {
			break
		}

		if h.Typeflag == tar.TypeDir {
			continue
		}
		fmt.Println("Read ", h.Name)

		tx, err := db.Begin()
		if err != nil {
			fmt.Println("Transaction error: " + err.Error())
			os.Exit(2)
		}

		reader := bufio.NewReader(t)
		for {
			line, err = reader.ReadString('\n')
			if err == io.EOF {
				break
			}

			err := process(tx, line)
			if err != nil {
				fmt.Println(err)
				break
			}
		}
		tx.Commit()
	}
}
