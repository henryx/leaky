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
	"strconv"
	"strings"

	"github.com/xi2/xz"
	"gopkg.in/alecthomas/kingpin.v2"

	_ "github.com/go-sql-driver/mysql"
)

const MAX_TRANSACTIONS_PER_COMMIT = 1000000

var partition *bool
var database *string

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

func opendb(database, dbuser, dbpassword, dbhost string) (*sql.DB, error) {
	var counted int
	var params, proto string
	var err error

	query := "SELECT COUNT(DISTINCT `table_name`) FROM `information_schema`.`columns` WHERE `table_schema` = ?"

	params = "charset=utf8mb4"
	if strings.HasPrefix(dbhost, "/") {
		proto = "unix"
		params = params + "&loc=local"
	} else {
		proto = "tcp"
	}

	db, err := sql.Open("mysql", dbuser+":"+dbpassword+"@"+proto+"("+dbhost+")/"+database+"?"+params)
	if err != nil {
		return nil, errors.New("Database not opened: " + err.Error())
	}

	if err := db.QueryRow(query, database).Scan(&counted); err != nil {
		return nil, errors.New("Database not opened: " + err.Error())
	}

	if counted == 0 {
		db.Exec("CREATE TABLE leak(id int not null auto_increment, domain varchar(255), user varchar(255), password text, PRIMARY KEY (id)) DEFAULT CHARSET 'utf8mb4'")
	}

	return db, nil
}

func store(tx *sql.Tx, email []string, password string) error {
	var err error

	stmt, err := tx.Prepare("INSERT INTO leak (domain, user, password) VALUES(?, ?, ?)")
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

func addpartition(db *sql.DB) {
	var lastinsert int
	if err := db.QueryRow("SELECT LAST_INSERT_ID()").Scan(&lastinsert); err != nil {
		return
	}
	query := "ALTER TABLE leak PARTITION BY RANGE(id) (PARTITION p" +
		strconv.Itoa(lastinsert) + " VALUES LESS THAN (" +
		strconv.Itoa(lastinsert+MAX_TRANSACTIONS_PER_COMMIT) +
		"))"

	db.Exec(query)
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
			if *partition {
				addpartition(db)
			}

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

func checkparams(db, usr, pwd, host *string) (*string, *string, *string, *string) {
	validate := func(variable *string, envvar string) string {
		var retval string

		// If parameter has not passed from command line, it is readed from environment variable
		if *variable != "" {
			retval = *variable
		} else {
			retval = os.Getenv(envvar)
			if retval == "" {
				fmt.Fprintln(os.Stderr, "Parameter or environment variable was not passed:", envvar)
				os.Exit(1)
			}
		}
		return retval
	}

	database := validate(db, "DATABASE")
	dbusr := validate(usr, "DBUSER")
	dbpwd := validate(pwd, "DBPASSWORD")
	dbhost := validate(host, "DBHOST")

	return &database, &dbusr, &dbpwd, &dbhost
}

func main() {

	tarfile := kingpin.Flag("tarfile", "Set the tarfile to analyze").Short('T').String()
	directory := kingpin.Flag("directory", "Set the directory to analyze").Short('D').String()
	database = kingpin.Flag("db", "Set the database name").Short('d').String()
	dbuser := kingpin.Flag("user", "Set the user").Short('u').String()
	dbpassword := kingpin.Flag("password", "Set the password").Short('W').String()
	dbhost := kingpin.Flag("host", "Set the host").Short('H').String()
	partition = kingpin.Flag("partition", "Use partitioned table in database").Short('P').Bool()

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	if *tarfile == "" && *directory == "" {
		fmt.Fprintln(os.Stderr, "Please use -T or -D flag")
		os.Exit(1)
	} else if *tarfile != "" && *directory != "" {
		fmt.Fprintf(os.Stderr, "Flags -T and -D are mutually exclusive")
		os.Exit(1)
	}

	database, dbuser, dbpassword, dbhost = checkparams(database, dbuser, dbpassword, dbhost)

	db, err := opendb(*database, *dbuser, *dbpassword, *dbhost)
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
