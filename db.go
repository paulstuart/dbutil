package dbutil

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
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
	activeConn  *sqlite3.SQLiteConn
)

type DBU struct {
	*sql.DB
	fileName string
	Debug    bool
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
				activeConn = conn
				return nil
			},
		})
}

// struct members are tagged as such, `sql:"id" key:"true" table:"servers"`
//  where key and table are used for a single entry
func DBOpen(file string, init bool) (DBU, error) {
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

// helper to generate sql values placeholders
func valuePlaceholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return "(" + strings.Join(a, ",") + ")"
}

func (db DBU) ObjectInsert(obj interface{}) (int64, error) {
	skip := !keyIsSet(obj) // if we have a key, we should probably use it
	_, a := objFields(obj, skip)
	table, _, fields := dbFields(obj, skip)
	if len(table) == 0 {
		return -1, errors.New(fmt.Sprintf("no table defined for object: %v (fields: %s)", reflect.TypeOf(obj), fields))
	}
	v := valuePlaceholders(len(a))
	query := "insert into " + table + " (" + fields + ") values " + v
	return db.Insert(query, a...)
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
		return fmt.Errorf("No key specified for object table: %s", table)
	}
	args = append(args, id)
	query := fmt.Sprintf("update %s set %s where %s=?", table, strings.Join(list, ","), key)
	fmt.Println("UPDATE QUERY:", query, "ARGS:", args)

	_, err := db.Update(query, args...)
	return err
}

func (db DBU) ObjectDelete(obj interface{}) error {
	table, key, id := deleteInfo(obj)
	if len(key) == 0 {
		return errors.New("No primary key for table: " + table)
	}
	query := fmt.Sprintf("delete from %s where %s=?", table, key)
	rec, err := db.Update(query, id)
	if err != nil {
		fmt.Println("BAD QUERY:", query, "\nID:", id)
		return err
	}
	if rec == 0 {
		return errors.New(fmt.Sprintf("No record deleted for id: %v", id))
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
			//fmt.Println("KEY FIELD:", f.Name)
			v := val.Field(i).Interface()
			switch v.(type) {
			case int:
				//fmt.Println("i KEY:", v.(int))
				return v.(int) > 0
			case int64:
				return v.(int64) > 0
				//fmt.Println("d KEY:", v.(int64))
			default:
				//fmt.Println("a KEY:", v)
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

/* TODO: fix this!
func (db DBU) ObjectLoadByID(reply interface{}) (err error) {
    obj := reflect.Indirect(reflect.ValueOf(reply)).Interface()
    query := createQuery(obj, false)
    if len(extra) > 0 {
        query += " " + extra
    }
    //fmt.Println("ObjectLoad Query:",Query)
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	row := db.QueryRow(Query, args...)
	dest := sPtrs(reply)
	err = row.Scan(dest...)
	return
}
*/

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

func (db DBU) objListQuery(Kind interface{}, extra string, args ...interface{}) (interface{}, error) {
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

func (db DBU) objList(Kind interface{}) (interface{}, error) {
	return db.objListQuery(Kind, "")
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

func (db DBU) Table(query string, args ...interface{}) (t Table, err error) {
	if db.Debug {
		fmt.Fprintln(os.Stderr, "QUERY:", query, "ARGS:", args)
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	t.Columns, err = rows.Columns()
	if err != nil {
		return
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
	return
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
				fmt.Println("READ FILE:", line[dbpref:], "ERR:", err)
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
			fmt.Println("EXEC QUERY:", line, "\nFILE:", db.fileName, "\nERR:", err)
			return err
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

func (db DBU) Databases() (t Table) {
	t, _ = db.Table("PRAGMA database_list")
	return t
}

var (
	backupConn []*sqlite3.SQLiteConn
)

func init() {
	sql.Register("backup_hook",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				backupConn = append(backupConn, conn)
				return nil
			},
		})
}

func Backup(src, dest string) {
	backupConn = []*sqlite3.SQLiteConn{}
	os.Remove(dest)

	destDb, err := sql.Open("backup_hook", dest)
	if err != nil {
		log.Fatal(err)
	}
	defer destDb.Close()
	destDb.Ping()

	srcDb, err := sql.Open("backup_hook", src)
	if err != nil {
		log.Fatal(err)
	}
	defer srcDb.Close()
	srcDb.Ping()

	bk, err := backupConn[0].Backup("main", backupConn[1], "main")
	if err != nil {
		log.Fatal(err)
	}

	for {
		fmt.Println("pagecount:", bk.PageCount(), "remaining:", bk.Remaining())
		done, err := bk.Step(1024)
		if err != nil {
			log.Fatal(err)
		}
		if done {
			break
		}
	}
	bk.Finish()
}

// calling this other than Backup for distinction
func (db DBU) Save(to string) error {
	dest := &sqlite3.SQLiteConn{}
	sql.Register("backup",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				dest = conn
				return nil
			},
		})
	os.Remove(to)

	destDb, err := sql.Open("backup", to)
	if err != nil {
		return err
	}
	defer destDb.Close()
	destDb.Ping()

	bk, err := dest.Backup("main", activeConn, "main")
	if err != nil {
		return err
	}

	for {
		fmt.Println("pagecount:", bk.PageCount(), "remaining:", bk.Remaining())
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
