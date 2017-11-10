// Package dbutil provides helper functions for database queries.
//
// It has no external dependencies, just stdlib.
package dbutil

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// RowFunc is a function called for each row by Stream (columns, row number, values)
// Row numbering starts at 1
type RowFunc func([]string, int, []interface{}) error

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
func Row(db *sql.DB, dest []interface{}, query string, args ...interface{}) error {
	return db.QueryRow(query, args...).Scan(dest...)
}

// Get returns a row results
func Get(db *sql.DB, query string, args ...interface{}) ([]string, []interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	if !rows.Next() {
		return nil, nil, sql.ErrNoRows
	}
	columns, _ := Columns(rows)
	buff := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for k := 0; k < len(dest); k++ {
		dest[k] = &buff[k]
	}
	return columns, buff, rows.Scan(dest...)
}

// RowStrings returns the row results as a slice of strings
func RowStrings(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	columns, _ := Columns(rows)
	dest := make([]interface{}, len(columns))

	// recycle columns slice as values buffer
	for k := 0; k < len(dest); k++ {
		dest[k] = &columns[k]
	}
	return columns, rows.Scan(dest...)
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

// Streamer streams query results
type Streamer struct {
	db    *sql.DB
	query string
	args  []interface{}
}

// NewStreamer returns a Streamer
func NewStreamer(db *sql.DB, query string, args ...interface{}) *Streamer {
	return &Streamer{db: db, query: query, args: args}
}

// Stream streams the query results to function fn
func (s *Streamer) Stream(fn RowFunc) error {
	return stream(s.db, fn, s.query, s.args...)
}

// stream streams the query results to function fn
func stream(db *sql.DB, fn RowFunc, query string, args ...interface{}) error {
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

	i := 1
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
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
func (s *Streamer) CSV(w io.Writer) error {
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
	return s.Stream(fn)
}

// TSV streams the query results as a tab separated file
func (s *Streamer) TSV(w io.Writer) error {
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
	return s.Stream(fn)
}

// JSON streams the query results as an array of JSON objects to the writer
func (s *Streamer) JSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	fn := func(columns []string, count int, buffer []interface{}) error {
		if count > 0 {
			fmt.Fprintln(w, ",")
		}
		obj := make(map[string]interface{})
		for i, c := range columns {
			obj[c] = buffer[i]
		}
		return enc.Encode(obj)
	}
	fmt.Fprintln(w, "[")
	defer fmt.Fprintln(w, "\n]")
	return s.Stream(fn)
}

// RowMap returns the results of a query as a map
func RowMap(db *sql.DB, query string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	columns, _ := Columns(rows)
	buffer := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for k := 0; k < len(dest); k++ {
		dest[k] = &buffer[k]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	reply := make(map[string]interface{})
	for i, col := range columns {
		reply[col] = buffer[i]
	}

	return reply, nil
}

type inserted struct {
	args []interface{}
	err  chan error
}

// Inserter enables inserting multiple records in a single transaction
type Inserter struct {
	c   chan inserted
	err chan error
}

// Insert inserts a record in a transaction
func (i Inserter) Insert(args ...interface{}) error {
	err := make(chan error)
	i.c <- inserted{args, err}
	return <-err
}

// Close closes the insert transaction
func (i Inserter) Close() error {
	close(i.c)
	return <-i.err
}

// NewInserter returns an Inserter that allows inserting  multiple records as a single transaction
func NewInserter(db *sql.DB, query string) (*Inserter, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	c := make(chan inserted)
	e := make(chan error)
	inserter := Inserter{c, e}
	go func() {
		for i := range c {
			if _, err = stmt.Exec(i.args...); err != nil {
				tx.Rollback()
				i.err <- err
				return
			}
			i.err <- nil
		}
		e <- tx.Commit()
	}()
	return &inserter, nil
}
