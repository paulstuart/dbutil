package dbutil

import (
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
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

	dbread = ".read "    // for sqlite interactive emulation
	dbpref = len(dbread) // optimize for same
)

var (
	pragmas     = strings.Fields(pragma_list)
	c_comment   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sql_comment = regexp.MustCompile(`\s*--.*`)
	readline    = regexp.MustCompile(`(\.read \S+)`)
	numeric, _  = regexp.Compile("^[0-9]+(\\.[0-9])?$")
	registry    = make(map[string]*sqlite3.SQLiteConn)
	mu, rmu     sync.Mutex
	debug_db    = false
)

var (
	ErrNoKeyField = errors.New("table has no key field")
	ErrKeyMissing = errors.New("key is not set")
	ErrNoRows     = errors.New("no rows found")
)

type DBU struct {
	BackedUp int64
	DB       *sql.DB
	logger   *log.Logger
}

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

func (db DBU) Logger(logger *log.Logger) {
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	mu.Lock()
	db.logger = logger
	mu.Unlock()
}

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

func logger(q string, args ...interface{}) {
	if debugging() {
		tmp := make([]interface{}, 0, len(args)+3)
		tmp = append(tmp, "Q:", q, "A:")
		tmp = append(tmp, args...)
		spew.Println(tmp...)
	}
}

type QueryKeys map[string]interface{}

type DBObject interface {
	TableName() string
	KeyField() string
	KeyName() string
	Names() []string
	SelectFields() string
	InsertFields() string
	Key() int64
	SetID(int64)
	InsertValues() []interface{}
	UpdateValues() []interface{}
	MemberPointers() []interface{}
	ModifiedBy(int64, time.Time)
}

type DBGen interface {
	NewObj() interface{} //DBObject
}

func setParams(params string) string {
	list := strings.Split(params, ",")
	for i, p := range list {
		list[i] = fmt.Sprintf("%s=?", p)
	}
	return strings.Join(list, ",")
}

func InsertFields(o DBObject) string {
	list := strings.Split(o.InsertFields(), ",")
	keep := make([]string, 0, len(list))
	for _, p := range list {
		if p != o.KeyField() {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, ",")
}

func SelectQuery(o DBObject) string {
	return fmt.Sprintf("select %s from %s where %s=?", o.SelectFields(), o.TableName(), o.KeyField())
}

func InsertQuery(o DBObject) string {
	p := Placeholders(len(o.InsertValues()))
	return fmt.Sprintf("insert into %s (%s) values(%s)", o.TableName(), InsertFields(o), p)
}

func ReplaceQuery(o DBObject) string {
	p := Placeholders(len(o.InsertValues()))
	return fmt.Sprintf("replace into %s (%s) values(%s)", o.TableName(), InsertFields(o), p)
}

func UpdateQuery(o DBObject) string {
	return fmt.Sprintf("update %s set %s where %s=?", o.TableName(), setParams(InsertFields(o)), o.KeyField())
}

func DeleteQuery(o DBObject) string {
	return fmt.Sprintf("delete from %s where %s=?", o.TableName(), o.KeyField())
}

// Add new object to datastore
func (db DBU) Add(o DBObject) error {
	args := o.InsertValues()
	logger(InsertQuery(o), args)
	result, err := db.DB.Exec(InsertQuery(o), args...)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Add new or replace existing object in datastore
func (db DBU) Replace(o DBObject) error {
	args := o.InsertValues()
	result, err := db.DB.Exec(ReplaceQuery(o), args)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Save modified object in datastore
func (db DBU) Save(o DBObject) error {
	_, err := db.Update(UpdateQuery(o), o.UpdateValues()...)
	return err
}

// Delete object from datastore
func (db DBU) Delete(o DBObject) error {
	logger(DeleteQuery(o), o.Key())
	_, err := db.DB.Exec(DeleteQuery(o), o.Key())
	return err
}

// Delete object from datastore by id
func (db DBU) DeleteByID(o DBObject, id interface{}) error {
	logger(DeleteQuery(o), id)
	_, err := db.DB.Exec(DeleteQuery(o), id)
	return err
}

// List objects from datastore
func (db DBU) List(o DBObject) interface{} {
	list, _ := db.ListQuery(o, "")
	return list
}

func (db DBU) Find(o DBObject, keys QueryKeys) error {
	where := make([]string, 0, len(keys))
	what := make([]interface{}, 0, len(keys))
	for k, v := range keys {
		where = append(where, k+"=?")
		what = append(what, v)
	}
	query := fmt.Sprintf("select %s from %s where %s", o.SelectFields(), o.TableName(), strings.Join(where, " and "))
	return db.Get(o.MemberPointers(), query, what...)
}

func (db DBU) FindBy(o DBObject, key string, value interface{}) error {
	query := fmt.Sprintf("select %s from %s where %s=?", o.SelectFields(), o.TableName(), key)
	return db.Get(o.MemberPointers(), query, value)
}

func (db DBU) FindByID(o DBObject, value interface{}) error {
	return db.FindBy(o, o.KeyField(), value)
}

func (db DBU) FindSelf(o DBObject) error {
	if len(o.KeyField()) == 0 {
		return ErrNoKeyField
	}
	if o.Key() == 0 {
		return ErrKeyMissing
	}
	return db.FindBy(o, o.KeyField(), o.Key())
}

func (db DBU) ListQuery(obj DBObject, extra string, args ...interface{}) (interface{}, error) {
	query := fmt.Sprintf("select %s from %s ", obj.SelectFields(), obj.TableName())
	if len(extra) > 0 {
		query += " " + extra
	}
	logger(query, args)
	val := reflect.ValueOf(obj)
	base := reflect.Indirect(val)
	t := reflect.TypeOf(base.Interface())
	results := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		log.Println("error on query: " + query + " -- " + err.Error())
		return nil, err
	}
	for rows.Next() {
		v := reflect.New(t)
		dest := v.Interface().(DBObject).MemberPointers()
		if err = rows.Scan(dest...); err != nil {
			fmt.Println("OOOPSIE", err)
			continue
		}
		results = reflect.Append(results, v.Elem())
	}
	err = rows.Err()
	rows.Close()
	//fmt.Println("LIST LEN:", results.Len())
	return results.Interface(), err
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

// The only way to get access to the sqliteconn, which is needed to be able to generate
// a backup from the database while it is open. This is a less than satisfactory approach
// because there's no way to have multiple instances open associate the connection with the DSN
//
// Since our use case is to normally have one instance open this should be workable for now
func init() {
	sql.Register("dbutil",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if err := conn.RegisterFunc("toIPv4", toIPv4, true); err != nil {
					return err
				}
				if err := conn.RegisterFunc("fromIPv4", fromIPv4, true); err != nil {
					return err
				}
				register(filename(conn), conn)
				return nil
			},
		})
}

func qRows(conn driver.Queryer, query string, args ...driver.Value) (Table, error) {
	t := Table{}
	rows, err := conn.Query(query, args)
	if err != nil {
		return t, err
	}
	defer rows.Close()
	t.Columns = rows.Columns()
	buffer := make([]interface{}, len(t.Columns))
	dest := make([]driver.Value, len(buffer))
	for i := 0; i < len(buffer); i++ {
		dest[i] = &buffer[i]
	}
	cnt := 0
	for {
		err := rows.Next(dest)
		if err != nil {
			if err != io.EOF {
				return t, err
			}
			break
		}
		t.Rows = append(t.Rows, make(Row, len(buffer)))
		for i, d := range dest {
			switch d := d.(type) {
			case []uint8:
				t.Rows[cnt][i] = string(d)
			case string:
			case int64:
				t.Rows[cnt][i] = fmt.Sprint(d)
			default:
				log.Printf("unexpected type %T", d)
			}
		}
		cnt++
	}
	return t, nil
}

// struct members are tagged as such, `sql:"id" key:"true" table:"servers"`
//  where key and table are used for a single entry
func Open(file string, init bool) (DBU, error) {
	dbu := DBU{}
	full, err := url.Parse(file)
	if err != nil {
		return dbu, err
	}
	filename := full.Path
	if init {
		os.Mkdir(path.Dir(filename), 0777)
		if f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666); err != nil {
			return dbu, err
		} else {
			f.Close()
		}
	}
	db, err := sql.Open("dbutil", file)
	/*
		log.Println("db sleep")
		time.Sleep(1 * time.Hour)
		log.Println("db slept")
	*/
	if err == nil {
		if err = db.Ping(); err != nil {
			return dbu, err
		}
		dbu.DB = db
	}
	return dbu, err
}

func CreateIfMissing(name, schema string) DBU {
	var fresh bool
	if _, err := os.Stat(name); os.IsNotExist(err) {
		fresh = true
	}
	db, err := Open(name, true)
	if err != nil {
		panic(err)
	}
	if fresh {
		err = db.File(schema)
		if err != nil {
			panic(err)
		}
	}
	return db
}

// helper to generate sql values placeholders
func Placeholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return strings.Join(a, ",")
}

func (db DBU) ObjectInsert(obj interface{}) (int64, error) {
	skip := !keyIsSet(obj) // if we have a key, we should probably use it
	_, a := objFields(obj, skip)
	table, _, fields := dbFields(obj, skip)
	if len(table) == 0 {
		return -1, fmt.Errorf("no table defined for object: %v (fields: %s)", reflect.TypeOf(obj), fields)
	}
	query := fmt.Sprintf("insert into %s (%s) values (%s)", table, fields, Placeholders(len(a)))
	result, err := db.DB.Exec(query, a...)
	if result != nil {
		id, _ := result.LastInsertId()
		return id, err
	}
	return -1, err
}

func (db DBU) ObjectUpdate(obj interface{}) error {
	var table, key string
	var id interface{}
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	args := make([]interface{}, 0, t.NumField())
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
			continue
		}
		args = append(args, val.Field(i).Interface())
		list = append(list, fmt.Sprintf("%s=?", k))
	}
	if len(key) == 0 {
		return ErrNoKeyField
	}
	args = append(args, id)
	query := fmt.Sprintf("update %s set %s where %s=?", table, strings.Join(list, ","), key)

	_, err := db.Update(query, args...)
	return err
}

func (db DBU) ObjectDelete(obj interface{}) error {
	table, key, id := deleteInfo(obj)
	if len(key) == 0 {
		return ErrNoKeyField
	}
	query := fmt.Sprintf("delete from %s where %s=?", table, key)
	rec, err := db.Update(query, id)
	if err != nil {
		return fmt.Errorf("BAD QUERY:%s ID:%v ERROR:%v", query, id, err)
	}
	if rec == 0 {
		return fmt.Errorf("No record deleted for id: %v", id)
	}
	return nil
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

func (db DBU) InsertMany(query string, args [][]interface{}) (err error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return
	}
	defer stmt.Close()
	for _, arg := range args {
		_, err = stmt.Exec(arg...)
		if err != nil {
			tx.Rollback()
			return
		}
	}
	tx.Commit()
	return
}

func (db DBU) Update(sqltext string, args ...interface{}) (i int64, e error) {
	return db.Run(sqltext, false, args...)
}

func (db DBU) Insert(sqltext string, args ...interface{}) (i int64, e error) {
	return db.Run(sqltext, true, args...)
}

func (db DBU) Run(sqltext string, insert bool, args ...interface{}) (i int64, err error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return
	}
	logger(sqltext, args)
	stmt, err := tx.Prepare(sqltext)
	if err != nil {
		tx.Rollback()
		return
	}
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		tx.Rollback()
		return
	}
	if insert {
		i, err = result.LastInsertId()
	} else {
		i, err = result.RowsAffected()
	}
	tx.Commit()
	return
}

func (db DBU) Print(Query string, args ...interface{}) {
	s, err := db.GetString(Query, args...)
	if err != nil {
		log.Println("ERROR:", err)
	} else {
		log.Println(s)
	}
}

func (db DBU) GetString(query string, args ...interface{}) (reply string, err error) {
	err = db.GetType(query, &reply, args...)
	return
}

func (db DBU) GetInt(Query string, args ...interface{}) (reply int, err error) {
	err = db.GetType(Query, &reply, args...)
	return
}

func (db DBU) GetType(query string, reply interface{}, args ...interface{}) (err error) {
	logger(query, args)
	row := db.DB.QueryRow(query, args...)
	err = row.Scan(reply)
	return
}

func (db DBU) Load(query string, reply *[]interface{}, args ...interface{}) (err error) {
	row := db.DB.QueryRow(query, args...)
	err = row.Scan(*reply...)
	return
}

// return list of IDs
func (db DBU) GetIDs(query string, args ...interface{}) ([]int64, error) {
	logger(query, args)
	ids := make([]int64, 0, 32)
	rows, err := db.DB.Query(query, args...)
	if err == nil {
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				break
			}
			ids = append(ids, id)
		}
	}
	rows.Close()
	return ids, err
}

func (db DBU) ObjectLoad(obj interface{}, extra string, args ...interface{}) (err error) {
	r := reflect.Indirect(reflect.ValueOf(obj)).Interface()
	query := createQuery(r, false)
	if len(extra) > 0 {
		query += " " + extra
	}
	logger(query, args)
	row := db.DB.QueryRow(query, args...)
	dest := sPtrs(obj)
	err = row.Scan(dest...)
	return
}

func (db DBU) LoadMany(query string, Kind interface{}, args ...interface{}) (error, interface{}) {
	t := reflect.TypeOf(Kind)
	s2 := reflect.Zero(reflect.SliceOf(t))
	logger(query, args)
	rows, err := db.DB.Query(query, args...)
	if err == nil {
		for rows.Next() {
			v := reflect.New(t)
			dest := sPtrs(v.Interface())
			err = rows.Scan(dest...)
			s2 = reflect.Append(s2, v.Elem())
		}
	}
	rows.Close()
	return err, s2.Interface()
}

func (db DBU) Stream(fn func([]string, int, []interface{}), query string, args ...interface{}) error {
	logger(query, args)
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	buffer := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for i := 0; i < len(buffer); i++ {
		dest[i] = &buffer[i]
	}
	i := 0
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			fmt.Println("BAD SCAN:", rows)
		}
		fn(columns, i, buffer)
		i++
	}
	rows.Close()
	return err
}

func (db DBU) StreamCSV(w io.Writer, query string, args ...interface{}) error {
	cw := csv.NewWriter(w)
	fn := func(columns []string, count int, buffer []interface{}) {
		if count == 0 {
			cw.Write(columns)
		}
		cw.Write(toString(buffer))
	}
	defer cw.Flush()
	return db.Stream(fn, query, args...)
}

func (db DBU) StreamTab(w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}) {
		if count == 0 {
			fmt.Fprintln(w, strings.Join(columns, "\t"))
		}
		fmt.Fprintln(w, strings.Join(toString(buffer), "\t"))
	}
	return db.Stream(fn, query, args...)
}

func isNumber(s string) bool {
	// leading zeros is likely a string
	if strings.HasPrefix(s, "00") {
		return false
	}
	return numeric.Match([]byte(strings.TrimSpace(s)))
}

func (db DBU) StreamJSON(w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []interface{}) {
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
	return db.Stream(fn, query, args...)
}

func (db DBU) StreamObjects(w io.Writer, o DBObject) error {
	query := fmt.Sprintf("select %s from %s", o.SelectFields(), o.TableName())
	return db.StreamJSON(w, query)
}

func (db DBU) ObjectListQuery(Kind interface{}, extra string, args ...interface{}) (interface{}, error) {
	Query := createQuery(Kind, false)
	if len(extra) > 0 {
		Query += " " + extra
	}
	if debugging() {
		fmt.Fprintln(os.Stderr, "Q:", Query, "A:", args)
	}
	t := reflect.TypeOf(Kind)
	results := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.DB.Query(Query, args...)
	if err != nil {
		log.Println("error on Query: " + Query + " -- " + err.Error())
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		v := reflect.New(t)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
		if err != nil {
			log.Println("scan error: " + err.Error())
			log.Println("scan query: "+Query+" args:", args)
			return nil, err
		}
		results = reflect.Append(results, v.Elem())
	}
	return results.Interface(), nil
}

func (db DBU) ObjectList(Kind interface{}) (interface{}, error) {
	return db.ObjectListQuery(Kind, "")
}

func (db DBU) LoadMap(what interface{}, Query string, args ...interface{}) interface{} {
	maptype := reflect.TypeOf(what)
	elem := maptype.Elem()
	themap := reflect.MakeMap(maptype)
	index := keyIndex(reflect.Zero(elem).Interface())
	rows, err := db.DB.Query(Query, args...)
	if err != nil {
		log.Println("LoadMap error:" + err.Error())
		return nil
	}
	for rows.Next() {
		v := reflect.New(elem)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
		k1 := dest[index]
		k2 := reflect.ValueOf(k1)
		key := reflect.Indirect(k2)
		themap.SetMapIndex(key, v.Elem())
	}
	rows.Close()
	return themap.Interface()
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

func (db DBU) Row(Query string, args ...interface{}) ([]string, error) {
	rows, err := db.DB.Query(Query, args...)
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
	return []string{}, ErrNoRows
}

func (db DBU) Get(members []interface{}, query string, args ...interface{}) error {
	logger(query, args)
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		log.Println("error on query: " + query + " -- " + err.Error())
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(members...)
		if err != nil {
			log.Println("scan error: " + err.Error())
			log.Println("scan query: "+query+" args:", args)
			return err
		}
		return nil
	}
	return ErrNoRows
}

func (db DBU) GetRow(Query string, args ...interface{}) (reply map[string]string, err error) {
	rows, err := db.DB.Query(Query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		cols, _ := rows.Columns()
		temp := make([]string, len(cols))
		dest := make([]*string, len(temp))
		for i := range temp {
			dest[i] = &temp[i]
		}
		err = rows.Scan(dest)
		reply = make(map[string]string)
		for i, col := range cols {
			reply[col] = temp[i]
		}
		break
	}
	return
}

func (db DBU) Table(query string, args ...interface{}) (*Table, error) {
	logger(query, args)
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	t := &Table{}
	t.Columns, err = rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		row := make([]interface{}, len(t.Columns))
		dest := make([]interface{}, len(row))
		for i := range t.Columns {
			dest[i] = &row[i]
		}
		err = rows.Scan(dest...)
		if err != nil {
			log.Println("SCAN ERROR: ", err, "QUERY:", query)
		}
		t.Rows = append(t.Rows, toString(row)) //final)
	}
	return t, nil
}

func (db DBU) Rows(Query string, args ...interface{}) (results []string, err error) {
	logger(Query, args)
	rows, err := db.DB.Query(Query, args...)
	if err != nil {
		return
	}
	results = make([]string, 0)
	defer rows.Close()
	for rows.Next() {
		var dest string
		err = rows.Scan(&dest)
		if err != nil {
			log.Println("SCAN ERR:", err, "QUERY:", Query)
			return
		}
		results = append(results, dest)
	}
	return
}

func startsWith(data, sub string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(data)), strings.ToUpper(sub))
}

// emulate ".read FILENAME"
func (db DBU) File(file string) error {
	out, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	// strip comments
	clean := c_comment.ReplaceAll(out, []byte{})
	clean = sql_comment.ReplaceAll(clean, []byte{})
	clean = readline.ReplaceAll(clean, []byte("${1};")) // .read gets a fake ';' to split on
	lines := strings.Split(string(clean), ";")
	multiline := "" // triggers are multiple lines
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if 0 == len(line) {
			continue
		}
		if len(line) >= dbpref && dbread == line[:dbpref] {
			err = db.File(line[dbpref:])
			if err != nil {
				log.Println("READ FILE:", line[dbpref:], "ERR:", err)
			}
			continue
		} else if startsWith(line, "CREATE TRIGGER") {
			multiline = line
			continue
		} else if startsWith(line, "END") {
			line = multiline + ";\n" + line
			multiline = ""
		} else if len(multiline) > 0 {
			multiline += ";\n" + line // restore our 'split' transaction
			continue
		}
		if _, err := db.DB.Exec(line); err != nil {
			log.Println("EXEC QUERY:", line, "\nFILE:", db.Filename(), "\nERR:", err)
			return err
		} else if debugging() {
			log.Println("QUERY:", line)
		}
	}
	return nil
}

func (db DBU) Cmd(Query string) (affected, last int64, err error) {
	Query = strings.TrimSpace(Query)
	if 0 == len(Query) {
		return
	}
	i, dberr := db.DB.Exec(Query)
	if dberr != nil {
		err = dberr
		return
	}
	affected, _ = i.RowsAffected()
	last, _ = i.LastInsertId()
	return
}

func (db DBU) pragmatic(pragma string, dest ...interface{}) error {
	row := db.DB.QueryRow("PRAGMA " + pragma)
	return row.Scan(dest...)
}

func (db DBU) Pragma(pragma string) (string, error) {
	var status string
	/*
			if debugging() {
				log.Println("PRAGMA ", pragma)
			}
		row := db.DB.QueryRow("PRAGMA " + pragma)
		err := row.Scan(&status)
	*/
	err := db.pragmatic(pragma, &status)
	return status, err
}

func (db DBU) Pragmas() (map[string]string, error) {
	var err error
	status := make(map[string]string, 0)
	for _, pragma := range pragmas {
		if status[pragma], err = db.Pragma(pragma); err != nil {
			break
		}
	}
	return status, err
}

func (db DBU) Stats() []string {
	status, _ := db.Pragmas()
	stats := make([]string, 0, len(status))
	for _, pragma := range pragmas {
		stats = append(stats, pragma+": "+status[pragma])
	}
	return stats
}

func (db DBU) Databases() *Table {
	t, _ := db.Table("PRAGMA database_list")
	return t
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

func slptr(arr []string) []interface{} {
	resp := make([]interface{}, 0, len(arr))
	for i := range arr {
		resp = append(resp, &arr[i])
	}
	return resp
}

func (db DBU) Filename() string {
	buff := make([]string, 3)
	dest := slptr(buff)
	if err := db.pragmatic("database_list", dest...); err != nil {
		return "filename error:" + err.Error()
	}
	return buff[2]
}

func (db *DBU) Backup(dest string, logger *log.Logger) error {
	os.Remove(dest)

	v, _ := db.Version()
	destDb, err := Open(dest, true)
	if err != nil {
		return err
	}
	defer destDb.DB.Close()
	err = destDb.DB.Ping()

	logger.Println("FROM:", db.Filename())
	logger.Println("TO  :", destDb.Filename())

	from := registered(db.Filename())
	to := registered(destDb.Filename())

	//bk, err := from.Backup("main", to, "main")
	bk, err := to.Backup("main", from, "main")
	if err != nil {
		logger.Println("BACKUP ERR:", err)
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
	db.BackedUp = v
	return err
}

func (db DBU) Changed() bool {
	v, _ := db.Version()
	return v != db.BackedUp
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

/*
 */

func (db DBU) Version() (int64, error) {
	var version int64
	err := db.pragmatic("data_version", &version)
	return version, err
}
