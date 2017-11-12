// Package dbutil provides helper functions for database queries.
//
// It has no external dependencies, just stdlib.
package dbutil

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

func strVal(in interface{}) string {
	switch v := in.(type) {
	case nil:
		return ""
	case string:
		return v
	case sql.RawBytes:
		return string(v)
	case []uint8:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func toString(in []interface{}) []string {
	out := make([]string, len(in))
	for i, col := range in {
		out[i] = strVal(col)
	}
	return out
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
	mods, _, err := Exec(db, query, args...)
	return mods, err
}

// Insert runs an insert query and returns the id of the last records inserted
func Insert(db *sql.DB, query string, args ...interface{}) (int64, error) {
	_, last, err := Exec(db, query, args...)
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
	r, err := db.Exec(query, args...)
	if err != nil {
		return 0, 0, err
	}
	affected, _ = r.RowsAffected()
	last, _ = r.LastInsertId()
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

// StreamFunc is a function called for each row by Stream (columns, row number, values).
//
// Row numbering starts at 1.
type StreamFunc func([]string, int, []interface{}) error

// Streamer streams rows from query results to be formatted or processed
type Streamer struct {
	db    *sql.DB
	query string
	args  []interface{}
}

// NewStreamer returns a Streamer
func NewStreamer(db *sql.DB, query string, args ...interface{}) *Streamer {
	return &Streamer{db: db, query: query, args: args}
}

// Stream sends each row the query results to a StreamFunc
func (s *Streamer) Stream(fn StreamFunc) error {
	return stream(s.db, fn, s.query, s.args...)
}

// stream streams the query results to function fn
func stream(db *sql.DB, fn StreamFunc, query string, args ...interface{}) error {
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
func (s *Streamer) CSV(w io.Writer, header bool) error {
	cw := csv.NewWriter(w)
	fn := func(columns []string, count int, buffer []interface{}) error {
		if header && count == 1 {
			cw.Write(columns)
		}
		return cw.Write(toString(buffer))
	}
	defer cw.Flush()
	return s.Stream(fn)
}

// TSV streams the query results as a tab separated values
func (s *Streamer) TSV(w io.Writer, header bool) error {
	fn := func(columns []string, count int, buffer []interface{}) error {
		if header && count == 1 {
			fmt.Fprintln(w, strings.Join(columns, "\t"))
		}
		for i, col := range buffer {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			fmt.Fprint(w, strVal(col))
		}
		fmt.Fprintln(w)
		return nil
	}
	return s.Stream(fn)
}

// JSON streams the query results as an array of JSON objects to the writer
func (s *Streamer) JSON(w io.Writer) error {
	fn := func(columns []string, count int, buffer []interface{}) error {
		if count > 1 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprint(w, "\n{")
		for i, col := range columns {
			if i > 0 {
				fmt.Fprint(w, ", ")
			}
			fmt.Fprintf(w, `"%s": `, col)
			switch v := buffer[i].(type) {
			case bool, int, int32, int64, float32, float64:
				fmt.Fprint(w, v)
			case []byte:
				fmt.Fprintf(w, `"%v"`, string(v))
			default:
				fmt.Fprintf(w, `"%v"`, v)
			}
		}
		fmt.Fprint(w, "}")
		return nil
	}
	fmt.Fprint(w, "[")
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
