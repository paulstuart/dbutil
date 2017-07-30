package dbutil

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	DriverName = "dbutil"
)

var (
	ErrNoKeyField = fmt.Errorf("table has no key field")
	ErrKeyMissing = fmt.Errorf("key is not set")
	ErrNilDB      = fmt.Errorf("db is nil")
)

type DBU struct {
	BackedUp int64
	DB       *sql.DB
	logger   *log.Logger
}

func (db DBU) Logger(logger *log.Logger) {
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	mu.Lock()
	db.logger = logger
	mu.Unlock()
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
func (db DBU) List(o DBObject) (interface{}, error) {
	return db.ListQuery(o, "")
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
			fmt.Println("query:", query, "error:", err)
			continue
		}
		results = reflect.Append(results, v.Elem())
	}
	err = rows.Err()
	rows.Close()
	//fmt.Println("LIST LEN:", results.Len())
	return results.Interface(), err
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
	return Backup(db.DB, dest, logger)
}

func (db DBU) Changed() bool {
	v, _ := db.Version()
	return v != db.BackedUp
}

func NewDBU(file string, init bool) (DBU, error) {
	return NewDBUWithHook(file, "", init)
}

func NewDBUWithHook(file, hook string, init bool) (DBU, error) {
	db, err := OpenWithHook(file, hook, init)
	return DBU{DB: db}, err
}

// helper to generate sql values placeholders
func Placeholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return strings.Join(a, ",")
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
	logger(sqltext, args)
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

func (db DBU) GetString(query string, args ...interface{}) (string, error) {
	var reply string
	return reply, db.GetType(query, &reply, args...)
}

func (db DBU) GetInt(query string, args ...interface{}) (int, error) {
	var reply int
	return reply, db.GetType(query, &reply, args...)
}

func (db DBU) GetType(query string, reply interface{}, args ...interface{}) error {
	logger(query, args)
	_, err := GetResults(db.DB, query, args, reply)
	return err
	/*
		row := db.DB.QueryRow(query, args...)
		err := row.Scan(reply)
		row.Close()
		return err
	*/
}

func (db DBU) Load(query string, reply []interface{}, args ...interface{}) error {
	_, err := GetResults(db.DB, query, args, reply...)
	return err
	/*
		row := db.DB.QueryRow(query, args...)
		err := row.Scan(*reply...)
		row.Close()
		return err
	*/
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
	return row.Scan(dest...)
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

func (db DBU) Stream(fn func([]string, int, []interface{}, error), query string, args ...interface{}) error {
	logger(query, args)
	return Stream(db.DB, fn, query, args...)
}

func (db DBU) StreamCSV(w io.Writer, query string, args ...interface{}) error {
	return StreamCSV(db.DB, w, query, args...)
}

func (db DBU) StreamTab(w io.Writer, query string, args ...interface{}) error {
	return StreamTab(db.DB, w, query, args...)
}

func isNumber(s string) bool {
	// leading zeros is likely a string
	if strings.HasPrefix(s, "00") {
		return false
	}
	return numeric.Match([]byte(strings.TrimSpace(s)))
}

func (db DBU) StreamJSON(w io.Writer, query string, args ...interface{}) error {
	return StreamJSON(db.DB, w, query, args...)
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

func (db DBU) Row(query string, args ...interface{}) ([]string, error) {
	logger(query, args)
	return RowStrings(db.DB, query, args...)
}

func (db DBU) Get(members []interface{}, query string, args ...interface{}) error {
	logger(query, args)
	if db.DB == nil {
		return ErrNilDB
	}
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
	return nil
}

func (db DBU) GetRow(query string, args ...interface{}) (map[string]string, error) {
	row, err := MapRow(db.DB, query, args...)
	if err != nil {
		return nil, err
	}

	record := make(map[string]string)
	for k, v := range row {
		record[k] = fmt.Sprintf("%v", v)
	}
	return record, nil
}

func (db DBU) Table(query string, args ...interface{}) (*Table, error) {
	logger(query, args)
	if db.DB == nil {
		return nil, ErrNilDB
	}
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

func (db DBU) Rows(query string, args ...interface{}) ([]string, error) {
	logger(query, args)
	if db.DB == nil {
		return nil, ErrNilDB
	}
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	results := make([]string, 0)
	defer rows.Close()
	for rows.Next() {
		var dest string
		err = rows.Scan(&dest)
		if err != nil {
			return nil, errors.Wrapf(err, "query: %s args: %v", query, args)
		}
		results = append(results, dest)
	}
	return results, nil
}

func startsWith(data, sub string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(data)), strings.ToUpper(sub))
}

// emulate ".read FILENAME"
func (db DBU) File(file string) error {
	if db.DB == nil {
		return ErrNilDB
	}
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
		if strings.HasPrefix(line, ".read ") {
			name := strings.TrimSpace(line[7:])
			err = db.File(name)
			if err != nil {
				log.Println("READ FILE:", name, "ERR:", err)
			}
			continue
		} else if strings.HasPrefix(line, ".print ") {
			fmt.Println(strings.Trim(strings.TrimSpace(line[7:]), "'"))
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
	return Exec(db.DB, Query)
}

func (db DBU) pragmatic(pragma string, dest ...interface{}) error {
	row := db.DB.QueryRow("PRAGMA " + pragma)
	return row.Scan(dest...)
}

func (db DBU) Pragma(pragma string) (string, error) {
	var status string
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

func (db DBU) Version() (int64, error) {
	var version int64
	err := db.pragmatic("data_version", &version)
	return version, err
}

func (d DBU) Exec(query string, args ...interface{}) (affected, last int64, err error) {
	return Exec(d.DB, query, args...)
}
