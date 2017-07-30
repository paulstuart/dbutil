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

func setParams(params string) string {
	list := strings.Split(params, ",")
	for i, p := range list {
		list[i] = fmt.Sprintf("%s=?", p)
	}
	return strings.Join(list, ",")
}

// make slice of pointers to struct members for sql scanner
// expects struct value as input

func sPtrs(obj interface{}) []interface{} {
	val := reflect.ValueOf(obj)
	base := reflect.Indirect(val)
	t := reflect.TypeOf(base.Interface())
	data := make([]interface{}, 0, base.NumField())
	for i := 0; i < base.NumField(); i++ {
		tag := t.Field(i).Tag.Get("sql")
		if len(tag) > 0 {
			f := base.Field(i)
			data = append(data, f.Addr().Interface())
		}
	}
	return data
}

func deleteInfo(obj interface{}) (table, key string, id interface{}) {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if is_table := f.Tag.Get("table"); len(is_table) > 0 {
			table = is_table
		}
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("update") == "false" {
			continue
		}
		k := f.Tag.Get("sql")
		v := val.Field(i).Interface()
		is_key := f.Tag.Get("key")
		if is_key == "true" {
			key = k
			id = v
			break
		}
	}
	return
}

func keyIsSet(obj interface{}) bool {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Tag.Get("key") == "true" {
			v := val.Field(i).Interface()
			switch v.(type) {
			case int:
				return v.(int) > 0
			case int64:
				return v.(int64) > 0
			default:
				return false
			}
		}
	}
	return false
}

// generate list of sql fields for members.
// if skip_key is true, do not include the key field in the list
func dbFields(obj interface{}, skip_key bool) (table, key, fields string) {
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if is_table := f.Tag.Get("table"); len(is_table) > 0 {
			table = is_table
		}
		k := f.Tag.Get("sql")
		if f.Tag.Get("key") == "true" {
			key = k
			if skip_key {
				continue
			}
		}
		if len(k) > 0 {
			list = append(list, k)
		}
	}
	fields = strings.Join(list, ",")
	return
}

type SQLColumns map[string]bool

func GetColumns(obj interface{}) SQLColumns {
	columns := SQLColumns{}
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("sql")
		if len(tag) == 0 {
			continue
		}
		columns[tag] = (f.Tag.Get("key") == "true")
	}
	return columns
}

// marshal the object fields into an array
func objFields(obj interface{}, skip_key bool) (interface{}, []interface{}) {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	a := make([]interface{}, 0, t.NumField())
	var key interface{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if f.Tag.Get("key") == "true" {
			key = val.Field(i).Interface()
			if skip_key {
				continue
			}
		}
		a = append(a, val.Field(i).Interface())
	}
	return key, a
}

func createQuery(obj interface{}, skip_key bool) string {
	var table string
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		name := f.Tag.Get("table")
		if len(name) > 0 {
			table = name
		}
		if skip_key {
			key := f.Tag.Get("key")
			if key == "true" {
				continue
			}
		}
		list = append(list, f.Tag.Get("sql"))
	}
	if len(table) == 0 {
		return ("error: no table name specified for object:" + t.Name())
	}
	return "select " + strings.Join(list, ",") + " from " + table
}

func keyName(obj interface{}) string {
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("key")) > 0 {
			return f.Name
		}
	}
	return ""
}

func keyIndex(obj interface{}) int {
	t := reflect.TypeOf(obj)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("key")) > 0 {
			return i
		}
	}
	return 0 // TODO: error handling!
}

func toString(in []interface{}) []string {
	out := make([]string, 0, len(in))
	for _, col := range in {
		var s string
		switch t := col.(type) {
		case nil:
			s = ""
		case string:
			s = col.(string)
		case []uint8:
			s = string(col.([]uint8))
		case int32:
			s = strconv.Itoa(col.(int))
		case int64:
			s = strconv.FormatInt(col.(int64), 10)
		case time.Time:
			s = col.(time.Time).String()
		case sql.RawBytes:
			s = string(t)
		default:
			log.Println("unhandled type:", t.(string))
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

func Constrained(err error) (table, column string) {
	const pre = "UNIQUE constraint failed: "
	msg := err.Error()
	if strings.HasPrefix(msg, pre) {
		msg = msg[len(pre):]
		if i := strings.Index(msg, "."); i > 0 {
			table = msg[:i]
			column = msg[i+1:]
		}
	}
	return
}

func Row(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	buff := make([]interface{}, len(cols))
	dest := make([]interface{}, len(cols))
	for rows.Next() {
		for i := range cols {
			dest[i] = &(buff[i])
		}
		err = rows.Scan(dest...)
		return toString(buff), err
	}
	return []string{}, nil
}

func Get(db *sql.DB, members []interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return errors.Wrapf(err, "error on query: %s -- %v", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(members...)
		if err != nil {
			return errors.Wrapf(err, "scan error: %v query: %s args: %v", err, query, args)
		}
		return nil
	}
	return nil
}

func Insert(db *sql.DB, query string, args ...interface{}) (int64, error) {
	last, _, err := Exec(db, query, args...)
	return last, err
}

func Exec(db *sql.DB, query string, args ...interface{}) (affected, last int64, err error) {
	/*
		if db == nil {
			err = fmt.Errorf("db is nil")
			return
		}
	*/
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

func Run(db *sql.DB, query string, insert bool, args ...interface{}) (int64, error) {
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

type Inserter struct {
	db   *sql.DB
	args chan []interface{}
	last chan int64
}

func (i *Inserter) Insert(args ...interface{}) {
	i.args <- args
}

func (i *Inserter) Close() int64 {
	close(i.args)
	last := <-i.last
	return last
}

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

func StreamTab(db *sql.DB, w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}, err error) {
		if count == 0 {
			fmt.Fprintln(w, strings.Join(columns, "\t"))
		}
		fmt.Fprintln(w, strings.Join(toString(buffer), "\t"))
	}
	return Stream(db, fn, query, args...)
}

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

func ColWriter(rows *sql.Rows) func(...interface{}) {
	return nil
}

func Open(file string, init bool) (*sql.DB, error) {
	return OpenWithHook(file, "", init)
}

// OpenWithHook ultimately should be multi db aware
func OpenWithHook(file, hook string, init bool) (*sql.DB, error) {
	return OpenSqliteWithHook(file, DriverName, hook, true)
}

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
	t2 := reflect.TypeOf("")
	log.Printf("TESTING: SCAN TYPE: %v (%T)\n", t2, t2)
	meta := make([]MetaData, 0, len(columns))
	for _, c := range columns {
		//log.Printf("META: %+v\n", c)
		t := c.ScanType()
		log.Printf("COL: %s, SCAN TYPE: %v (%T)\n", c.Name(), t, t)
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

func GetResults(db *sql.DB, query string, args []interface{}, items ...interface{}) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, nil
	}
	cols, _ := rows.Columns()
	return cols, rows.Scan(items...)
}

func MapRow(db *sql.DB, query string, args ...interface{}) (map[string]interface{}, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, nil
	}

	cols, _ := rows.Columns()
	buff := make([]interface{}, len(cols))
	dest := make([]*interface{}, len(cols))
	for i := range buff {
		dest[i] = &buff[i]
	}

	if err := rows.Scan(dest); err != nil {
		return nil, err
	}

	reply := make(map[string]interface{})
	for i, col := range cols {
		reply[col] = buff[i]
	}

	rows.Close()
	return reply, err
}
