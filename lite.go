package dbutil

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// N/A, impacts db, or multi-column -- ignore for now
//collation_list
//database_list
//foreign_key_check
//foreign_key_list
//quick_check
//wal_checkpoint

const (
	pragma_list = `
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
	pragmas     = strings.Fields(pragma_list)
	c_comment   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sql_comment = regexp.MustCompile(`\s*--.*`)
	readline    = regexp.MustCompile(`(\.read \S+)`)
	numeric, _  = regexp.Compile("^[0-9]+(\\.[0-9])?$")
	registry    = make(map[string]*sqlite3.SQLiteConn)
	debug_db    = false
	initialized = false
)

func register(file string, conn *sqlite3.SQLiteConn) {
	rmu.Lock()
	registry[file] = conn
	rmu.Unlock()
}

func registered(file string) *sqlite3.SQLiteConn {
	rmu.Lock()
	conn := registry[file]
	rmu.Unlock()
	return conn
}

func toIPv4(ip int64) string {
	log.Println("to ipv4:", ip)
	a := ip >> 24
	b := (ip >> 16) & 0xFF
	c := (ip >> 8) & 0xFF
	d := ip & 0xFF

	return fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
}

func fromIPv4(ip string) int64 {
	log.Println("from ipv4:", ip)
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

// The only way to get access to the sqliteconn, which is needed to be able to generate
// a backup from the database while it is open. This is a less than satisfactory approach
// because there's no way to have multiple instances open associate the connection with the DSN
//
// Since our use case is to normally have one instance open this should be workable for now
func sqlInit(hook string) {
	imu.Lock()
	defer imu.Unlock()
	if initialized {
		return
	}
	initialized = true

	sql.Register("dbutil",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if err := conn.RegisterFunc("iptoa", toIPv4, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("atoip", fromIPv4, true); err != nil {
					return err
				}
				register(filename(conn), conn)

				if len(hook) > 0 {
					if _, err := conn.Exec(hook, nil); err != nil {
						fmt.Println("HOOK:", hook, "ERR:", err)
						return err
					}
				}

				return nil
			},
		})
}

// struct members are tagged as such, `sql:"id" key:"true" table:"servers"`
//  where key and table are used for a single entry
//func OpenSqlite(file, driver, hook string, init bool) (*sql.DB, error) {
func OpenSqlite(file string, init bool) (*sql.DB, error) {
	full, err := url.Parse(file)
	if err != nil {
		return nil, err
	}
	filename := full.Path
	if init {
		os.Mkdir(path.Dir(filename), 0777)
		if f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666); err != nil {
			return nil, err
		} else {
			f.Close()
		}
	}
	db, err := sql.Open("sqlite3", file)
	if err == nil {
		return db, db.Ping()
	}
	return db, err
}

// get filename of db
func filename(conn driver.Queryer) string {
	t, err := qRows(conn, "PRAGMA database_list")

	if err == nil {
		if len(t.Rows) > 0 && len(t.Rows[0]) > 2 {
			return t.Rows[0][2]
		}

	}
	return ""
}

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
