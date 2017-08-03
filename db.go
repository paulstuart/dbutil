package dbutil

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

var (
	mu sync.Mutex
)

func Debug(on bool) {
	mu.Lock()
	debug_db = on
	mu.Unlock()
}

func debugging() bool {
	mu.Lock()
	enabled := debug_db
	mu.Unlock()
	return enabled
}

func logger(q string, args ...interface{}) {
	if debugging() {
		log.Println(spew.Sprintf("Q: %s, A: %v", q, args))
	}
}

func toString(in []interface{}) []string {
	out := make([]string, 0, len(in))
	for _, col := range in {
		var s string
		switch v := col.(type) {
		case nil:
			s = ""
		case string:
			s = v
		case []uint8:
			s = string(v)
		case int32:
			s = strconv.Itoa(int(v))
		case int64:
			s = strconv.FormatInt(v, 10)
		case float64:
			s = fmt.Sprintf("%v", v)
		case time.Time:
			s = v.String()
		case sql.RawBytes:
			s = string(v)
		default:
			log.Printf("unhandled type: %T", col)
		}
		out = append(out, s)
	}
	return out
}

func slptr(arr []string) []interface{} {
	resp := make([]interface{}, 0, len(arr))
	for i := range arr {
		resp = append(resp, &arr[i])
	}
	return resp
}

func Row(db *sql.DB, query string, args ...interface{}) ([]string, []interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	buff := make([]interface{}, len(cols))
	dest := make([]interface{}, len(cols))
	if !rows.Next() {
		return cols, nil, nil
	}
	for i := range cols {
		dest[i] = &(buff[i])
	}
	if err := rows.Scan(dest...); err != nil {
		return cols, nil, err
	}

	return cols, buff, err
}

func RowStrings(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	_, row, err := Row(db, query, args...)
	if err != nil {
		return nil, err
	}
	return toString(row), nil
}

func Insert(db *sql.DB, query string, args ...interface{}) (int64, error) {
	last, _, err := Exec(db, query, args...)
	return last, err
}

func InsertMany(db *sql.DB, query string, args [][]interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, arg := range args {
		_, err = stmt.Exec(arg...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

func Exec(db *sql.DB, query string, args ...interface{}) (affected, last int64, err error) {
	query = strings.TrimSpace(query)
	if 0 == len(query) {
		return 0, 0, fmt.Errorf("empty query")
	}
	var i sql.Result
	if i, err = db.Exec(query, args...); err != nil {
		return 0, 0, err
	}
	affected, _ = i.RowsAffected()
	last, _ = i.LastInsertId()
	return affected, last, nil
}

func Run(db *sql.DB, insert bool, query string, args ...interface{}) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	var i int64
	if insert {
		i, err = result.LastInsertId()
	} else {
		i, err = result.RowsAffected()
	}
	tx.Commit()
	return i, err
}

// Inserter manages bulk inserts
type Inserter struct {
	db   *sql.DB
	args chan []interface{}
	last chan int64
}

// Insert a record into a transaction
func (i *Inserter) Insert(args ...interface{}) {
	i.args <- args
}

// Close closes the insert transaction
func (i *Inserter) Close() int64 {
	close(i.args)
	last := <-i.last
	return last
}

// NewInserter returns an Inserter for bulk inserts
func NewInserter(db *sql.DB, queue int, errFn func(error), query string, args ...interface{}) (*Inserter, error) {

	c := make(chan []interface{}, queue)
	last := make(chan int64)

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	go func() {
		var result sql.Result
		for values := range c {
			result, err = stmt.Exec(values...)
			if err != nil {
				tx.Rollback()
				if errFn != nil {
					errFn(err)
				}
				break
			}
		}
		i, err := result.LastInsertId()
		if errFn != nil {
			errFn(err)
		}

		if err := tx.Commit(); err != nil && errFn != nil {
			errFn(err)
		}
		stmt.Close()
		last <- i
	}()

	return &Inserter{
		args: c,
		last: last,
	}, nil
}

// Stream streams the query results to function fn
func Stream(db *sql.DB, fn func([]string, int, []interface{}, error), query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	i := 0
	for rows.Next() {
		buffer := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for k := 0; k < len(buffer); k++ {
			dest[k] = &buffer[k]
		}
		err = rows.Scan(dest...)
		if err != nil {
			log.Println("BAD SCAN:", rows)
		}
		fn(columns, i, buffer, err)
		i++
	}
	rows.Close()
	return err
}

// StreamCSV streams the query results as a comma separated file
func StreamCSV(db *sql.DB, w io.Writer, query string, args ...interface{}) error {
	cw := csv.NewWriter(w)
	fn := func(columns []string, count int, buffer []interface{}, err error) {
		if count == 0 {
			cw.Write(columns)
		}
		cw.Write(toString(buffer))
	}
	defer cw.Flush()
	return Stream(db, fn, query, args...)
}

// StreamTab streams the query results as a tab separated file
func StreamTab(db *sql.DB, w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}, err error) {
		if count == 0 {
			fmt.Fprintln(w, strings.Join(columns, "\t"))
		}
		fmt.Fprintln(w, strings.Join(toString(buffer), "\t"))
	}
	return Stream(db, fn, query, args...)
}

// StreamJSON streams the query results as JSON to the writer
func StreamJSON(db *sql.DB, w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}, err error) {
		if count > 0 {
			fmt.Fprintln(w, ",")
		}
		fmt.Fprintln(w, "  {")
		repl := strings.NewReplacer("\n", "\\\\n", "\t", "\\\\t", "\r", "\\\\r", `"`, `\"`)
		for i, s := range toString(buffer) {
			comma := ",\n"
			if i >= len(buffer)-1 {
				comma = "\n"
			}
			if isNumber(s) {
				fmt.Fprintf(w, `    "%s": %s%s`, columns[i], s, comma)
			} else {
				s = repl.Replace(s)
				fmt.Fprintf(w, `    "%s": "%s"%s`, columns[i], s, comma)
			}
		}
		fmt.Fprint(w, "  }")
	}
	fmt.Fprintln(w, "[")
	defer fmt.Fprintln(w, "\n]")
	return Stream(db, fn, query, args...)
}

// Open returns a db struct for the given file
func Open(file string, init bool) (*sql.DB, error) {
	return OpenWithHook(file, "", init)
}

// OpenWithHook ultimately should be multi db aware
func OpenWithHook(file, hook string, init bool) (*sql.DB, error) {
	return OpenSqliteWithHook(file, DriverName, hook, init)
}

// Iterator returns query results
type Iterator func() (values []interface{}, ok bool)

func Generator(db *sql.DB, query string, args ...interface{}) func() ([]interface{}, bool) {
	c := make(chan []interface{})
	fn := func(columns []string, row int, values []interface{}, err error) {
		c <- values
	}
	iter := func() ([]interface{}, bool) {
		values, ok := <-c
		return values, ok
	}
	go func() {
		if err := Stream(db, fn, query); err != nil {
			panic(err)
		}
		close(c)
	}()

	return iter
}

type MetaData struct {
	Column string
	Type   reflect.Type
}

func Streamer(db *sql.DB, query string, args ...interface{}) ([]MetaData, Iterator, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	meta := make([]MetaData, 0, len(columns))
	for _, c := range columns {
		m := MetaData{
			Column: c.Name(),
			Type:   c.ScanType(),
		}
		meta = append(meta, m)
	}

	c := make(chan []interface{})
	iter := func() ([]interface{}, bool) {
		values, ok := <-c
		return values, ok
	}
	go func() {
		for rows.Next() {
			buffer := make([]interface{}, len(columns))
			dest := make([]interface{}, len(columns))
			for k := 0; k < len(buffer); k++ {
				dest[k] = &buffer[k]
			}
			err = rows.Scan(dest...)
			if err != nil {
				log.Println("BAD SCAN:", rows)
			}
			c <- buffer
		}
		rows.Close()
		close(c)
	}()
	return meta, iter, nil
}

type Action struct {
	Query    string
	Args     []interface{}
	Callback func(int64, int64, error)
}

type Query struct {
	Query string
	Args  []interface{}
	Reply func([]string, int, []interface{}, error)
}

// Server provides serialized access to the database
func Server(db *sql.DB, r chan Query, w chan Action, e chan error) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for q := range r {
			if err := Stream(db, q.Reply, q.Query, q.Args...); err != nil && e != nil {
				go func() {
					e <- errors.Wrapf(err, "Stream error")
				}()
			}
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for q := range w {
			affected, last, err := Exec(db, q.Query, q.Args...)
			q.Callback(affected, last, err)
		}
		wg.Done()
	}()
	wg.Wait()
	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
}

// GetResults writes to the record slice
func GetResults(db *sql.DB, query string, args []interface{}, record ...interface{}) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, nil
	}
	cols, _ := rows.Columns()
	return cols, rows.Scan(record...)
}

// MapRow returns the results of a query as a map
func MapRow(db *sql.DB, query string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, nil
	}

	cols, _ := rows.Columns()
	buff := make([]interface{}, len(cols))
	dest := make([]interface{}, len(cols))
	for i := range buff {
		dest[i] = &buff[i]
	}

	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}

	reply := make(map[string]interface{})
	for i, col := range cols {
		reply[col] = buff[i]
	}

	rows.Close()
	return reply, err
}
