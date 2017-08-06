package dbutil

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

var (
	rmu, imu sync.Mutex
)

// N/A, impacts db, or multi-column -- ignore for now
//collation_list
//database_list
//foreign_key_check
//foreign_key_list
//quick_check
//wal_checkpoint

const (
	pragmaList = `
	application_id
	auto_vacuum
	automatic_index
	busy_timeout
	cache_size
	cache_spill
	cell_size_check
	checkpoint_fullfsync
	compile_options
	data_version
	defer_foreign_keys
	encoding
	foreign_keys
	freelist_count
	fullfsync
	journal_mode
	journal_size_limit
	legacy_file_format
	locking_mode
	max_page_count
	mmap_size
	page_count
	page_size
	query_only
	read_uncommitted
	recursive_triggers
	reverse_unordered_selects
	schema_version
	secure_delete
	soft_heap_limit
	synchronous
	temp_store
	threads
	user_version
	wal_autocheckpoint
	`
)

var (
	pragmas    = strings.Fields(pragmaList)
	commentC   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	commentSQL = regexp.MustCompile(`\s*--.*`)
	readline   = regexp.MustCompile(`(\.read \S+)`)

	registry    = make(map[string]*sqlite3.SQLiteConn)
	initialized = make(map[string]struct{})
)

func register(file string, conn *sqlite3.SQLiteConn) {
	file, _ = filepath.Abs(file)
	if len(file) > 0 {
		rmu.Lock()
		registry[file] = conn
		rmu.Unlock()
	}
}

func registered(file string) *sqlite3.SQLiteConn {
	rmu.Lock()
	conn := registry[file]
	rmu.Unlock()
	return conn
}

func toIPv4(ip int64) string {
	a := ip >> 24
	b := (ip >> 16) & 0xFF
	c := (ip >> 8) & 0xFF
	d := ip & 0xFF

	return fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
}

func fromIPv4(ip string) int64 {
	octets := strings.Split(ip, ".")
	if len(octets) != 4 {
		return -1
	}
	a, _ := strconv.ParseInt(octets[0], 10, 64)
	b, _ := strconv.ParseInt(octets[1], 10, 64)
	c, _ := strconv.ParseInt(octets[2], 10, 64)
	d, _ := strconv.ParseInt(octets[3], 10, 64)
	return (a << 24) + (b << 16) + (c << 8) + d
}

// SqliteFuncReg contains the fields necessary to register a custom Sqlite function
type SqliteFuncReg struct {
	Name string
	Impl interface{}
	Pure bool
}

// IPFuncs are functions to convert ipv4 to and from int32
var IPFuncs = []SqliteFuncReg{
	{"iptoa", toIPv4, true},
	{"atoip", fromIPv4, true},
}

// The only way to get access to the sqliteconn, which is needed to be able to generate
// a backup from the database while it is open. This is a less than satisfactory approach
// because there's no way to have multiple instances open associate the connection with the DSN
//
// Since our use case is to normally have one instance open this should be workable for now
func sqlInit(name, hook string, funcs ...SqliteFuncReg) {
	imu.Lock()
	defer imu.Unlock()

	if _, ok := initialized[name]; ok {
		return
	}
	initialized[name] = struct{}{}

	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			for _, fn := range funcs {
				if err := conn.RegisterFunc(fn.Name, fn.Impl, fn.Pure); err != nil {
					return err
				}
			}
			if filename, err := ConnFilename(conn); err == nil {
				register(filename, conn)
			} else {
				fmt.Println("couldn't get filename for connection:", err)
			}

			if len(hook) > 0 {
				if _, err := conn.Exec(hook, nil); err != nil {
					return errors.Wrapf(err, "connection hook failed: %s", hook)
				}
			}

			return nil
		},
	}
	sql.Register(name, drvr)
}

// OpenSqlite returns a database
func OpenSqlite(file, name string, init bool, funcs ...SqliteFuncReg) (*sql.DB, error) {
	return OpenSqliteWithHook(file, name, "", init, funcs...)
}

// OpenSqliteWithHook returns a database with a connection hok
// struct members are tagged as such, `sql:"id" key:"true" table:"servers"`
//  where key and table are used for a single entry
func OpenSqliteWithHook(file, name, hook string, init bool, funcs ...SqliteFuncReg) (*sql.DB, error) {
	sqlInit(DriverName, hook, funcs...)
	if file != ":memory:" {
		full, err := url.Parse(file)
		if err != nil {
			return nil, errors.Wrapf(err, "parse file: %s", file)
		}
		filename := full.Path
		os.Mkdir(path.Dir(filename), 0777)
		if init {
			if _, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666); err != nil {
				return nil, errors.Wrapf(err, "os file: %s", file)
			}
		} else if _, err := os.Stat(filename); os.IsNotExist(err) {
			return nil, err
		}
	}
	if len(name) == 0 {
		name = "sqlite3"
	}
	db, err := sql.Open(name, file)
	if err != nil {
		return db, errors.Wrapf(err, "sql file: %s", file)
	}
	return db, db.Ping()
}

// Filename returns the filename of the DB
func Filename(db *sql.DB) string {
	var seq, name, file string
	GetResults(db, "PRAGMA database_list", nil, &seq, &name, &file)
	return file
}

// ConnFilename returns the filename of the connection
func ConnFilename(conn *sqlite3.SQLiteConn) (string, error) {
	var filename string
	fn := func(cols []string, row int, values []driver.Value) error {
		if len(values) < 3 {
			return fmt.Errorf("only got %d values", len(values))
		}
		if values[2] == nil {
			return fmt.Errorf("nil values")
		}
		filename = string(values[2].([]uint8))
		return nil
	}
	err := ConnQuery(conn, fn, "PRAGMA database_list")
	return filename, err
}

// DBVersion returns the datafile version
func DBVersion(file string) (uint64, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	b := make([]byte, 4)
	f.ReadAt(b, 24)

	var a uint64
	a += uint64(b[0]) << 24
	a += uint64(b[1]) << 16
	a += uint64(b[2]) << 8
	a += uint64(b[3])
	return a, nil
}

// Backup backs up the open database
func Backup(db *sql.DB, dest string, logger *log.Logger) error {
	os.Remove(dest)

	destDb, err := OpenSqlite(dest, DriverName, true)
	if err != nil {
		return err
	}
	defer destDb.Close()
	err = destDb.Ping()

	fromDB := Filename(db)
	toDB := Filename(destDb)

	from := registered(fromDB)
	to := registered(toDB)

	bk, err := to.Backup("main", from, "main")
	if err != nil {
		return err
	}

	defer bk.Finish()
	for {
		logger.Println("pagecount:", bk.PageCount(), "remaining:", bk.Remaining())
		done, err := bk.Step(1024)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	return err
}

// Pragmas lists all relevant Sqlite pragmas
func Pragmas(db *sql.DB, w io.Writer) {
	for _, pragma := range pragmas {
		row := db.QueryRow("PRAGMA " + pragma)
		var value string
		row.Scan(&value)
		fmt.Fprintf(w, "pragma %s = %s\n", pragma, value)
	}
}

// File emulates ".read FILENAME"
func File(db *sql.DB, file string, echo bool) error {
	out, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return Commands(db, string(out), echo)
}

func startsWith(data, sub string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(data)), strings.ToUpper(sub))
}

// Commands emulates the client reading a series of commands
// TODO: is this available in the C api?
func Commands(db *sql.DB, buffer string, echo bool) error {

	// strip comments
	clean := commentC.ReplaceAll([]byte(buffer), []byte{})
	clean = commentSQL.ReplaceAll(clean, []byte{})

	// .read gets a fake ';' to split on
	clean = readline.ReplaceAll(clean, []byte("${1};"))

	lines := strings.Split(string(clean), ";")
	multiline := "" // triggers are multiple lines
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if 0 == len(line) {
			continue
		}
		if echo {
			fmt.Println("LINE:", line)
		}
		switch {
		case strings.HasPrefix(line, ".echo "):
			echo, _ = strconv.ParseBool(line[6:])
			continue
		case strings.HasPrefix(line, ".read "):
			name := strings.TrimSpace(line[7:])
			if err := File(db, name, echo); err != nil {
				return errors.Wrapf(err, "read file: %s", name)
			}
			continue
		case strings.HasPrefix(line, ".print "):
			fmt.Println(strings.Trim(strings.TrimSpace(line[7:]), "'"))
			continue
		case startsWith(line, "CREATE TRIGGER"):
			multiline = line
			continue
		case startsWith(line, "END"):
			line = multiline + ";\n" + line
			multiline = ""
		case len(multiline) > 0:
			multiline += ";\n" + line // restore our 'split' transaction
			continue
		}
		if _, err := db.Exec(line); err != nil {
			return errors.Wrapf(err, "EXEC QUERY: %s FILE: %s", line, Filename(db))
		}
	}
	return nil
}

// ConnQuery executes a query on a driver connection
func ConnQuery(conn *sqlite3.SQLiteConn, fn func([]string, int, []driver.Value) error, query string, args ...driver.Value) error {
	rows, err := conn.Query(query, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols := rows.Columns()
	cnt := 0
	for {
		buffer := make([]driver.Value, len(cols))
		if err := rows.Next(buffer); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		fn(cols, cnt, buffer)
		cnt++
	}
	return nil
}

// DataVersion returns the version number of the schema
func DataVersion(db *sql.DB) (int64, error) {
	var version int64
	_, err := GetResults(db, "PRAGMA data_version", nil, &version)
	return version, err
}

// Version returns the version of the sqlite library used
// libVersion string, libVersionNumber int, sourceID string) {
func Version() (string, int, string) {
	return sqlite3.Version()
}
