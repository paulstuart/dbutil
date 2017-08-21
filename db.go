package dbutil

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	mu sync.Mutex

	digits  = regexp.MustCompile("^[0-9]+$")
	numeric = regexp.MustCompile("^[0-9]+(\\.[0-9])?$")
	repl    = strings.NewReplacer(
		"\n", "\\\\n",
		"\t", "\\\\t",
		"\r", "\\\\r",
		`"`, `\"`,
		"_", " ",
		"-", " ",
	)
	testout = ioutil.Discard
)

const (
	// DefaultDriver is the default driver name to be registered
	DefaultDriver = "dbutil"
)

// ServerAction represents an async write request to database
type ServerAction struct {
	Query    string
	Args     []interface{}
	Callback func(int64, int64, error)
}

// RowFunc is a function called for each row by Stream
type RowFunc func([]string, int, []interface{}) error

// ServerQuery represents an async read request to database
type ServerQuery struct {
	Query string
	Args  []interface{}
	Reply RowFunc
	Error chan error
}

func toString(in []interface{}) ([]string, error) {
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
			return nil, fmt.Errorf("unhandled type: %T", col)
		}
		out = append(out, s)
	}
	return out, nil
}

// Row returns one row of the results of a query
func Row(db *sql.DB, dest []interface{}, query string, args ...interface{}) ([]string, []interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	cols, _ := Columns(rows)
	if !rows.Next() {
		return cols, nil, nil
	}
	buff, err := scanRow(rows, dest, cols...)
	return cols, buff, err
}

// RowStrings returns the row results all as strings
func RowStrings(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	_, row, err := Row(db, nil, query, args...)
	if err != nil {
		return nil, err
	}
	return toString(row)
}

// Update runs an update query and returns the count of records updated, if any
func Update(db *sql.DB, query string, args ...interface{}) (int64, error) {
	_, mods, err := Exec(db, query, args...)
	return mods, err
}

// Insert runs an insert query and returns the id of the last records inserted
func Insert(db *sql.DB, query string, args ...interface{}) (int64, error) {
	last, _, err := Exec(db, query, args...)
	return last, err
}

// InsertMany inserts multiple records as a single transaction
func InsertMany(db *sql.DB, query string, args ...[]interface{}) error {
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
		if _, err = stmt.Exec(arg...); err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

// Exec executes a query and returns the effected records info
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
				return
			}
		}
		i, err := result.LastInsertId()
		if err != nil && errFn != nil {
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

// Columns returns a slice of column names that respects aliases in the query
func Columns(row *sql.Rows) ([]string, error) {
	ctypes, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(ctypes))
	for i, c := range ctypes {
		columns[i] = c.Name()
	}
	return columns, nil
}

func scanRow(rows *sql.Rows, dest []interface{}, columns ...string) ([]interface{}, error) {
	var buffer []interface{}
	if dest == nil {
		buffer = make([]interface{}, len(columns))
		dest = make([]interface{}, len(columns))
		for k := 0; k < len(buffer); k++ {
			dest[k] = &buffer[k]
		}
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, errors.Wrapf(err, "bad scan: %v", rows)
	}
	return buffer, nil
}

// Streamer can stream queries to a writer
type Streamer struct {
	db *sql.DB
}

// NewStreamer returns a Streamer
func NewStreamer(db *sql.DB) *Streamer {
	return &Streamer{db}
}

// Stream streams the query results to function fn
func (s *Streamer) Stream(fn RowFunc, query string, args ...interface{}) error {
	return stream(s.db, nil, fn, query, args...)
}

// stream streams the query results to function fn
func stream(db *sql.DB, dest []interface{}, fn RowFunc, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := Columns(rows)
	if err != nil {
		return err
	}

	i := 0
	for rows.Next() {
		buffer, err := scanRow(rows, dest, columns...)
		if err != nil {
			return err
		}
		if err := fn(columns, i, buffer); err != nil {
			return err
		}
		i++
	}
	return err
}

// CSV streams the query results as a comma separated file
func (s *Streamer) CSV(w io.Writer, query string, args ...interface{}) error {
	cw := csv.NewWriter(w)
	fn := func(columns []string, count int, buffer []interface{}) error {
		if count == 0 {
			cw.Write(columns)
		}
		s, err := toString(buffer)
		if err == nil {
			cw.Write(s)
		}
		return err
	}
	defer cw.Flush()
	return s.Stream(fn, query, args...)
}

// Tab streams the query results as a tab separated file
func (s *Streamer) Tab(w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}) error {
		if count == 0 {
			fmt.Fprintln(w, strings.Join(columns, "\t"))
		}
		s, err := toString(buffer)
		if err == nil {
			fmt.Fprintln(w, strings.Join(s, "\t"))
		}
		return err
	}
	return s.Stream(fn, query, args...)
}

func isNumber(d interface{}) error {
	switch d := d.(type) {
	case int, int32, int64, float32, float64:
		return nil
	case string:
		// multiple leading zeros is likely a string
		if strings.HasPrefix(d, "00") {
			return fmt.Errorf("00 prefix indicates string")
		}
		if strings.HasPrefix(d, "0x") {
			var i int
			r := strings.NewReader(d)
			if _, err := fmt.Fscanf(r, "0x%x", &i); err != nil {
				return errors.Wrap(err, "hex err")
			}
			return nil
		}
		if numeric.Match([]byte(strings.TrimSpace(d))) {
			return nil
		}
		return fmt.Errorf("not a numeric match")
	default:
		return fmt.Errorf("unknown type: %v", d)
	}
}

// JSON streams the query results as JSON to the writer
func (s *Streamer) JSON(w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}) error {
		if count > 0 {
			fmt.Fprintln(w, ",")
		}
		obj := make(map[string]interface{})
		for i, c := range columns {
			obj[c] = buffer[i]
		}
		enc := json.NewEncoder(w)
		return enc.Encode(obj)
		/*
			fmt.Fprintln(w, "  {")
			for i, b := range buffer {
				comma := ",\n"
				if i >= len(buffer)-1 {
					comma = "\n"
				}
				if isNumber(b) {
					fmt.Fprintf(w, `    "%s": %v%s`, columns[i], b, comma)
				} else {
					//s := fmt.Sprintf("%v", b)
					//s = repl.Replace(s)
					//fmt.Fprintf(w, `    "%s": "%s"%s`, columns[i], s, comma)
					fmt.Fprintf(w, `    "%s": "%v"%s`, columns[i], s, comma)
				}
			}
			fmt.Fprint(w, "  }")
		*/
	}
	fmt.Fprintln(w, "[")
	defer fmt.Fprintln(w, "\n]")
	return s.Stream(fn, query, args...)
}

// Iterator returns query results
type Iterator func() (values []interface{}, ok bool)

/*
// Generator returns an iterator for a query
func Generator(db *sql.DB, query string, args ...interface{}) func() ([]interface{}, bool) {
	c := make(chan []interface{})
	fn := func(columns []string, row int, values []interface{}) error {
		c <- values
		return nil
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
*/

// Server provides serialized access to the database
func Server(db *sql.DB, r chan ServerQuery, w chan ServerAction) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for q := range r {
			err := stream(db, nil, q.Reply, q.Query, q.Args...)

			if q.Error != nil {
				// use goroutine so we don't block on sending errors
				go func() {
					if err != nil {
						err = errors.Wrapf(err, "stream error")
					}
					q.Error <- err
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
}

// Get writes to the record slice
func Get(db *sql.DB, query string, args []interface{}, dest ...interface{}) ([]string, error) {
	cols, _, err := Row(db, dest, query, args...)
	return cols, err
}

// RowMap returns the results of a query as a map
func RowMap(db *sql.DB, query string, args ...interface{}) (map[string]interface{}, error) {
	cols, buff, err := Row(db, nil, query, args...)
	if err != nil {
		return nil, err
	}
	reply := make(map[string]interface{})
	if len(buff) > 0 {
		for i, col := range cols {
			reply[col] = buff[i]
		}
	}

	return reply, nil
}

// Close cleans up the database before closing
func Close(db *sql.DB) {
	Exec(db, "PRAGMA wal_checkpoint(TRUNCATE)")
	db.Close()
}

// WriteRow writes each row directly to the writer, using the given field delimiter
func WriteRow(db *sql.DB, w io.Writer, delimiter string, header bool, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := Columns(rows)
	if err != nil {
		return err
	}

	buffer := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for k := 0; k < len(buffer); k++ {
		dest[k] = &buffer[k]
	}
	if header {
		for i, col := range columns {
			fmt.Fprint(w, col)
			if i < len(columns)-1 {
				fmt.Fprint(w, delimiter)
			} else {
				fmt.Fprintln(w)
			}
		}
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return errors.Wrapf(err, "bad scan: %v", rows)
		}
		for i, col := range buffer {
			fmt.Fprintf(w, "%v", col)
			if i < len(buffer)-1 {
				fmt.Fprint(w, delimiter)
			} else {
				fmt.Fprintln(w)
			}
		}
	}
	return nil
}
