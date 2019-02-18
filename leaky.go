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
	"gopkg.in/alecthomas/kingpin.v2"

	_ "github.com/go-sql-driver/mysql"
)

const MAX_TRANSACTIONS_PER_COMMIT = 1000000

func readtgz(file io.Reader) *tar.Reader {
	gz, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	t := tar.NewReader(gz)
	return t
}

func readtxz(file io.Reader) *tar.Reader {
	xzip, err := xz.NewReader(file, 0)
	if err != nil {
		panic(err)
	}
	t := tar.NewReader(xzip)

	return t
}

func readtar(db *sql.DB, tarfile *string) {
	var t *tar.Reader

	f, err := os.Open(*tarfile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	switch filepath.Ext(*tarfile) {
	case ".tar":
		t = tar.NewReader(f)
	case ".gz":
		t = readtgz(f)
	case ".xz":
		t = readtxz(f)
	default:
		fmt.Println("Extension not recognized", filepath.Ext(*tarfile))
		os.Exit(-1)
	}

	for {
		h, err := t.Next()
		if err == io.EOF {
			break
		}

		if h.Typeflag == tar.TypeDir {
			continue
		}
		fmt.Println("Read ", h.Name)
		reader := bufio.NewReader(t)
		scanlines(db, reader)
	}
}

func readdir(db *sql.DB, directory *string) {
	readfile := func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			fmt.Println("Read ", path)
			f, err := os.Open(path)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Cannot open "+path+":", err.Error())
			} else {
				defer f.Close()

				reader := bufio.NewReader(f)
				scanlines(db, reader)
			}
		}
		return nil
	}

	filepath.Walk(*directory, readfile)
}

func process(tx *sql.Tx, line string) error {
	var split []string

	if strings.Contains(line, ";") {
		split = strings.SplitN(line, ";", 2)
	} else if strings.Contains(line, ",") {
		split = strings.SplitN(line, ",", 2)
	} else if strings.Contains(line, ":") {
		split = strings.SplitN(line, ":", 2)
	} else if strings.Contains(line, "|") {
		split = strings.SplitN(line, "|", 2)
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

	database := os.Getenv("DATABASE")
	dbuser := os.Getenv("DBUSER")
	dbpassword := os.Getenv("DBPASSWORD")

	query := "SELECT COUNT(DISTINCT `table_name`) FROM `information_schema`.`columns` WHERE `table_schema` = ?"

	db, err := sql.Open("mysql", dbuser+":"+dbpassword+"@unix(/tmp/mysql.sock)/"+database+"?loc=local")
	if err != nil {
		return nil, errors.New("Database not opened: " + err.Error())
	}

	if err := db.QueryRow(query, database).Scan(&counted); err != nil {
		return nil, errors.New("Database not opened: " + err.Error())
	}

	if counted == 0 {
		db.Exec("CREATE TABLE leak(domain varchar(30), user varchar(30), password varchar(4096))")
	}

	return db, nil
}

func store(tx *sql.Tx, email []string, password string) error {
	var err error

	stmt, err := tx.Prepare("INSERT INTO leak VALUES(?, ?, ?)")
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

func scanlines(db *sql.DB, reader *bufio.Reader) {
	var line string
	var err error
	var tx *sql.Tx

	i := 0
	for {
		line, err = reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		if i == 0 {
			tx, err = db.Begin()
			if err != nil {
				fmt.Println("Transaction error: " + err.Error())
				break
			}
		}

		err := process(tx, line)
		if err != nil {
			fmt.Println(err)
			break
		}

		if i == MAX_TRANSACTIONS_PER_COMMIT {
			i = 0
			tx.Commit()
		} else {
			i++
		}
	}
	tx.Commit()
}

func main() {

	tarfile := kingpin.Flag("tarfile", "Set the tarfile to analyze").Short('T').String()
	directory := kingpin.Flag("directory", "Set the directory to analyze").Short('D').String()

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	if *tarfile == "" && *directory == "" {
		fmt.Fprintln(os.Stderr, "Please use -T or -D flag")
		os.Exit(1)
	} else if *tarfile != "" && *directory != "" {
		fmt.Fprintf(os.Stderr, "Flags -T and -D are mutually exclusive")
		os.Exit(1)
	}

	db, err := opendb()
	if err != nil {
		fmt.Println("Database error: " + err.Error())
		os.Exit(1)
	}
	defer db.Close()

	if *tarfile != "" {
		fmt.Println("Start indexing of " + *tarfile + " tar file")
		readtar(db, tarfile)
	} else if *directory != "" {
		fmt.Println("Start indexing of " + *directory + " directory")
		readdir(db, directory)
	} else {
		kingpin.Usage()
	}
}
