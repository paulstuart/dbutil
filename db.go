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
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	pragma_list = "journal_mode locking_mode page_size page_count read_uncommitted busy_timeout temp_store cache_size freelist_count compile_options"
	dbread      = ".read "    // for sqlite interactive emulation
	dbpref      = len(dbread) // optimize for same
)

var (
	pragmas     = strings.Split(pragma_list, " ")
	c_comment   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sql_comment = regexp.MustCompile(`\s*--.*`)
	readline    = regexp.MustCompile(`(\.read \S+)`)
	numeric, _  = regexp.Compile("^[0-9]+(\\.[0-9])?$")
	registry    = make(map[string]*sqlite3.SQLiteConn)
)

var (
	ErrNoKeyField = errors.New("table has no key field")
	ErrKeyMissing = errors.New("key is not set")
)

type DBU struct {
	*sql.DB
	fileName string
	Debug    bool
}

type QueryKeys map[string]interface{}

type DBObject interface {
	TableName() string
	KeyField() string
	SelectFields() string
	Key() int64
	SetID(int64)
	InsertValues() []interface{}
	UpdateValues() []interface{}
	MemberPointers() []interface{}
}

func setParams(params string) string {
	list := strings.Split(params, ",")
	for i, p := range list {
		list[i] = fmt.Sprintf("%s=?", p)
	}
	return strings.Join(list, ",")
}

func InsertFields(o DBObject) string {
	list := strings.Split(o.SelectFields(), ",")
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
	if db.Debug {
		fmt.Println("Q:", InsertQuery(o), "A:", args)
	}
	result, err := db.Exec(InsertQuery(o), args...)
	if result != nil {
		id, _ := result.LastInsertId()
		o.SetID(id)
	}
	return err
}

// Add new or replace existing object in datastore
func (db DBU) Replace(o DBObject) error {
	args := o.InsertValues()
	result, err := db.Exec(ReplaceQuery(o), args...)
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
	_, err := db.Exec(DeleteQuery(o), o.Key())
	return err
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

func (db DBU) FindSelf(o DBObject) error {
	if len(o.KeyField()) == 0 {
		return ErrNoKeyField
	}
	if o.Key() == 0 {
		return ErrKeyMissing
	}
	return db.FindBy(o, o.KeyField(), o.Key())
}

// The only way to get access to the sqliteconn, which is needed to be able to generate
// a backup from the database while it is open. This is a less than satisfactory approach
// because there's no way to have multiple instances open associate the connection with the DSN
//
// Since our use case is to normally have one instance open this should be workable for now
func init() {
	sql.Register("s3",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				t, err := qRows(conn, "PRAGMA database_list")
				if err == nil {
					if len(t.Rows) > 0 && len(t.Rows[0]) > 2 {
						file := t.Rows[0][2]
						m := sync.Mutex{}
						m.Lock()
						registry[file] = conn
						m.Unlock()
					}
				} else {
					fmt.Println("Q ERR:", err)
				}

				return nil
			},
		})
}

func qRows(conn *sqlite3.SQLiteConn, query string, args ...driver.Value) (Table, error) {
	t := Table{}
	rows, err := conn.Query(query, args)
	if err != nil {
		return t, err
	}
	t.Columns = rows.Columns()
	buffer := make([]sql.RawBytes, len(t.Columns))
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
				fmt.Println("*** S D:", d)
			case int64:
				t.Rows[cnt][i] = fmt.Sprint(d)
			default:
				fmt.Printf("unexpected type %T", d)
			}
		}
		cnt++
	}
	return t, nil
}

// struct members are tagged as such, `sql:"id" key:"true" table:"servers"`
//  where key and table are used for a single entry
func Open(file string, init bool) (DBU, error) {
	dbu := DBU{nil, file, false}
	if init {
		os.Mkdir(path.Dir(file), 0777)
		if f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666); err != nil {
			return DBU{}, err
		} else {
			f.Close()
		}
	}
	db, err := sql.Open("s3", file)
	db.Ping()
	dbu.DB = db
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
	result, err := db.Exec(query, a...)
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
	table := ""
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

func (db DBU) InsertMany(sqltext string, args [][]interface{}) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	stmt, err := tx.Prepare(sqltext)
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
	tx, err := db.Begin()
	if err != nil {
		return
	}
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", sqltext, "ARGS:", args)
	}
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
		fmt.Println("ERROR:", err)
	} else {
		fmt.Println(s)
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
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	row := db.QueryRow(query, args...)
	err = row.Scan(reply)
	return
}

func (db DBU) Load(query string, reply *[]interface{}, args ...interface{}) (err error) {
	row := db.QueryRow(query, args...)
	err = row.Scan(*reply...)
	return
}

func (db DBU) ObjectLoad(obj interface{}, extra string, args ...interface{}) (err error) {
	r := reflect.Indirect(reflect.ValueOf(obj)).Interface()
	query := createQuery(r, false)
	if len(extra) > 0 {
		query += " " + extra
	}
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	row := db.QueryRow(query, args...)
	dest := sPtrs(obj)
	err = row.Scan(dest...)
	return
}

func (db DBU) LoadMany(query string, Kind interface{}, args ...interface{}) (error, interface{}) {
	t := reflect.TypeOf(Kind)
	s2 := reflect.Zero(reflect.SliceOf(t))
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	rows, err := db.Query(query, args...)
	for rows.Next() {
		v := reflect.New(t)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
		s2 = reflect.Append(s2, v.Elem())
	}
	return err, s2.Interface()
}

func (db DBU) Stream(fn func([]string, int, []sql.RawBytes), query string, args ...interface{}) error {
	if db.Debug {
		fmt.Fprintln(os.Stderr, "STREAM QUERY:", query, "ARGS:", args)
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	buffer := make([]sql.RawBytes, len(columns))
	dest := make([]interface{}, len(columns))
	for i := 0; i < len(buffer); i++ {
		dest[i] = &buffer[i]
	}
	i := 0
	for rows.Next() {
		err = rows.Scan(dest...)
		fn(columns, i, buffer)
		i++
	}
	return err
}

func rawStrings(raw []sql.RawBytes) []string {
	s := make([]string, len(raw))
	for i, v := range raw {
		s[i] = string(v)
	}
	return s
}

func (db DBU) StreamCSV(w io.Writer, query string, args ...interface{}) error {
	cw := csv.NewWriter(w)
	fn := func(columns []string, count int, buffer []sql.RawBytes) {
		if count == 0 {
			cw.Write(columns)
		}
		cw.Write(rawStrings(buffer))
	}
	defer cw.Flush()
	return db.Stream(fn, query, args...)
}

func (db DBU) StreamTab(w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []sql.RawBytes) {
		if count == 0 {
			fmt.Fprintln(w, strings.Join(columns, "\t"))
		}
		fmt.Fprintln(w, strings.Join(rawStrings(buffer), "\t"))
	}
	return db.Stream(fn, query, args...)
}

func isNumber(s string) bool {
	return numeric.Match([]byte(strings.TrimSpace(s)))
}

func (db DBU) StreamJSON(w io.Writer, query string, args ...interface{}) error {
	fn := func(columns []string, count int, buffer []sql.RawBytes) {
		if count > 0 {
			fmt.Fprintln(w, ",")
		}
		fmt.Fprintln(w, "  {")
		for i, s := range rawStrings(buffer) {
			comma := ",\n"
			if i >= len(buffer)-1 {
				comma = "\n"
			}
			if isNumber(s) {
				fmt.Fprintf(w, `    "%s": %s%s`, columns[i], s, comma)
			} else {
				s = strings.Replace(s, `"`, `\"`, -1)
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
	t := reflect.TypeOf(Kind)
	results := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.Query(Query, args...)
	if err != nil {
		log.Println("error on Query: " + Query + " -- " + err.Error())
		return nil, err
	}
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
	rows, err := db.Query(Query, args...)
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
		default:
			fmt.Println("unhandled type:", t.(string))
		}
		out = append(out, s)
	}
	return out
}

func (db DBU) Row(Query string, args ...interface{}) ([]string, error) {
	var reply []string
	rows, err := db.Query(Query, args...)
	if err != nil {
		return reply, err
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
		break
	}
	return toString(buff), err
}

func (db DBU) Get(members []interface{}, query string, args ...interface{}) error {
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	rows, err := db.Query(query, args...)
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
		break
	}
	return nil
}

func (db DBU) GetRow(Query string, args ...interface{}) (reply map[string]string, err error) {
	rows, err := db.Query(Query, args...)
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
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	rows, err := db.Query(query, args...)
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
			fmt.Println("SCAN ERROR: ", err, "QUERY:", query)
		}
		t.Rows = append(t.Rows, toString(row)) //final)
	}
	return t, nil
}

func (db DBU) Rows(Query string, args ...interface{}) (results []string, err error) {
	rows, err := db.Query(Query, args...)
	if err != nil {
		return
	}
	results = make([]string, 0)
	defer rows.Close()
	for rows.Next() {
		var dest string
		err = rows.Scan(&dest)
		if err != nil {
			fmt.Println("SCAN ERR:", err, "QUERY:", Query)
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
		if _, err := db.Exec(line); err != nil {
			log.Println("EXEC QUERY:", line, "\nFILE:", db.fileName, "\nERR:", err)
			return err
		} else if db.Debug {
			log.Println(os.Stderr, "QUERY:", line)
		}
	}
	return nil
}

func (db DBU) Cmd(Query string) (affected, last int64, err error) {
	Query = strings.TrimSpace(Query)
	if 0 == len(Query) {
		return
	}
	i, dberr := db.Exec(Query)
	if dberr != nil {
		err = dberr
		return
	}
	affected, _ = i.RowsAffected()
	last, _ = i.LastInsertId()
	return
}

func (db DBU) Pragma(pragma string) (string, error) {
	var status string
	row := db.QueryRow("PRAGMA " + pragma)
	err := row.Scan(&status)
	return status, err
}

func (db DBU) Pragmas() map[string]string {
	status := make(map[string]string, 0)
	for _, pragma := range pragmas {
		status[pragma], _ = db.Pragma(pragma)
	}
	return status
}

func (db DBU) Stats() []string {
	status := db.Pragmas()
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

func (db DBU) Backup(to string) error {
	os.Remove(to)

	destDb, err := sql.Open("s3", to)
	if err != nil {
		return err
	}
	defer destDb.Close()
	destDb.Ping()

	file, err := filepath.Abs(db.fileName)
	if err != nil {
		return err
	}
	activeConn, ok := registry[file]
	if !ok {
		return fmt.Errorf("no connection found for file:", db.fileName)
	}
	file, err = filepath.Abs(to)
	if err != nil {
		return err
	}
	backupConn, ok := registry[file]
	if !ok {
		return fmt.Errorf("no connection found for file:", to)
	}
	bk, err := backupConn.Backup("main", activeConn, "main")
	if err != nil {
		return err
	}

	for {
		log.Println("pagecount:", bk.PageCount(), "remaining:", bk.Remaining())
		done, err := bk.Step(1024)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	bk.Finish()
	return nil
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

func (db DBU) Version() (uint64, error) {
	f, err := os.Open(db.fileName)
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
