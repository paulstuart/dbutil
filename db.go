package dbutil

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	pragma_list    = "journal_mode locking_mode page_size page_count read_uncommitted busy_timeout temp_store cache_size freelist_count compile_options"
	dbread         = ".read "             // for sqlite interactive emulation
	dbpref         = len(dbread)          // optimize for same
)

var (
	pragmas     = strings.Split(pragma_list, " ")
	c_comment   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sql_comment = regexp.MustCompile(`\s*--.*`)
	readline    = regexp.MustCompile(`(\.read \S+)`)
)

type DBU struct {
    *sql.DB
}

func dbOpen(file string) (DBU, error) {
    db, err := sql.Open("sqlite3", file)
    return DBU{db}, err
}

// helper to generate sql values placeholders
func valuePlaceholders(n int) string {
	a := make([]string, n)
	for i := range a {
		a[i] = "?"
	}
	return "(" + strings.Join(a, ",") + ")"
}

func (db DBU) InsertObj(table string, obj interface{}) (int64, error) {
	a := objFields(obj, true)
	f := dbFields(obj, true)
	v := valuePlaceholders(len(a))
	query := "insert into " + table + " (" + f + ") values " + v
	return db.Insert(query, a...)
}

func (db DBU) UpdateObj(obj interface{}) (rec int64, err error) {
	var query string
	table, fields, key, id := dbSetFields(obj)
	if len(key) > 0 {
		query = fmt.Sprintf("update %s set %s where %s=?", table, fields, key)
		rec, err = db.Update(query, id)
	} else {
		query = fmt.Sprintf("update %s set %s", table, fields)
		rec, err = db.Update(query)
	}
	if err != nil {
		fmt.Println("BAD QUERY:", query, "\nID:", id)
	}
	return
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

// generate update statement data
func dbSetFields(obj interface{}) (table, fields, key string, id int64) {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
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
			id = v.(int64)
			continue
		}
		switch v.(type) {
		case string:
			list = append(list, fmt.Sprintf("%s='%s'", k, v))
		case time.Time:
			list = append(list, fmt.Sprintf("%s='%s'", k, v))
		case bool:
			if v.(bool) {
				list = append(list, fmt.Sprintf("%s=1", k))
			} else {
				list = append(list, fmt.Sprintf("%s=0", k))
			}
		default:
			list = append(list, fmt.Sprintf("%s=%v", k, v))
		}
	}
	fields = strings.Join(list, ",")
	return
}

func objFields(obj interface{}, skip_key bool) []interface{} {
	val := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)
	a := make([]interface{}, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if skip_key {
			key := f.Tag.Get("key")
			if key == "true" {
				continue
			}
		}
		a = append(a, val.Field(i).Interface())
	}
	return a
}

func dbFields(obj interface{}, skip_key bool) string {
	t := reflect.TypeOf(obj)
	list := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(f.Tag.Get("sql")) == 0 {
			continue
		}
		if skip_key {
			key := f.Tag.Get("key")
			if key == "true" {
				continue
			}
		}
		list = append(list, f.Tag.Get("sql"))
	}
	return strings.Join(list, ",")
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

func (db DBU) String(query string, args ...interface{}) string {
	s, err := db.GetString(query, args...)
	if err == nil {
		return s
	}
	return err.Error()
}

func (db DBU) Print(query string, args ...interface{}) {
	s, err := db.GetString(query, args...)
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

func (db DBU) GetInt(query string, args ...interface{}) (reply int, err error) {
	err = db.GetType(query, &reply, args...)
	return
}

func (db DBU) GetType(query string, reply interface{}, args ...interface{}) (err error) {
	row := db.QueryRow(query, args...)
	err = row.Scan(reply)
	return
}

func (db DBU) Load(query string, reply *[]interface{}, args ...interface{}) (err error) {
	row := db.QueryRow(query, args...)
	err = row.Scan(*reply...)
	return
}

func (db DBU) LoadObj(reply interface{}, query string, args ...interface{}) (err error) {
	row := db.QueryRow(query, args...)
	dest := sPtrs(reply)
	err = row.Scan(dest...)
	return
}

func (db DBU) LoadMany(query string, kind interface{}, args ...interface{}) (error, interface{}) {
	t := reflect.TypeOf(kind)
	s2 := reflect.Zero(reflect.SliceOf(t))
	rows, err := db.Query(query, args...)
	for rows.Next() {
		v := reflect.New(t)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
		s2 = reflect.Append(s2, v.Elem())
	}
	return err, s2.Interface()
}

func (db DBU) LoadMap(what interface{}, query string, args ...interface{}) (interface{}) {
    maptype := reflect.TypeOf(what)
    elem := maptype.Elem()
    themap := reflect.MakeMap(maptype)
    index := keyIndex(reflect.Zero(elem).Interface())
	rows, err := db.Query(query, args...)
    if err != nil {
        panic("DB ERROR:" + err.Error())
    }
	for rows.Next() {
		v := reflect.New(elem)
		dest := sPtrs(v.Interface())
		err = rows.Scan(dest...)
        k1 := dest[index]
        k2 := reflect.ValueOf(k1)
        key := reflect.Indirect(k2)
        themap.SetMapIndex(key,v.Elem())
	}
    return themap.Interface()
}


func (db DBU) Row(query string, args ...interface{}) (reply []string, err error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		cols, _ := rows.Columns()
		reply := make([]string, len(cols))
		dest := make([]*string, len(cols))
		for i := range reply {
			dest[i] = &reply[i]
		}
		err = rows.Scan(dest)
		break
	}
	return
}

func (db DBU) GetRow(query string, args ...interface{}) (reply map[string]string, err error) {
	rows, err := db.Query(query, args...)
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
		row := make([]sql.NullString, len(t.Columns))
		final := make([]string, len(t.Columns))
		dest := make([]interface{}, len(row))
		for i := range t.Columns {
			dest[i] = &row[i]
		}
		err = rows.Scan(dest...)
		if err != nil {
			fmt.Println("SCAN ERROR: ", err, "QUERY:", query)
		}
		for i := range row {
			final[i] = row[i].String
		}
		t.Rows = append(t.Rows, final)
	}
	return
}

func (db DBU) Rows(query string, args ...interface{}) (results []string, err error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return
	}
	results = make([]string, 0)
	defer rows.Close()
	for rows.Next() {
		var dest string
		err = rows.Scan(&dest)
		if err != nil {
			fmt.Println("SCAN ERR:", err, "QUERY:", query)
			return
		}
		results = append(results, dest)
	}
	return
}

func startsWith(data, sub string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(data)), strings.ToUpper(sub))
}

func (db DBU) File(file string) (err error) {
	out, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	// strip comments
	clean := c_comment.ReplaceAll(out, []byte{})
	clean = sql_comment.ReplaceAll(clean, []byte{})
	clean = readline.ReplaceAll(clean, []byte("${1};")) // .read gets a fake ';' to split on
	lines := strings.Split(string(clean), ";")
	multi := "" // triggers are multiple lines
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
			multi = line
			continue
		} else if startsWith(line, "END") {
			line = multi + ";\n" + line
			multi = ""
		} else if len(multi) > 0 {
			multi += ";\n" + line // restore our 'split' transaction
			continue
		}
		if _, err = db.Exec(line); err != nil {
			fmt.Println("EXEC QUERY:", line, "ERR:", err)
			return
		}
	}
	return
}

func (db DBU) Cmd(query string) (affected, last int64, err error) {
	query = strings.TrimSpace(query)
	if 0 == len(query) {
		return
	}
	i, dberr := db.Exec(query)
	if dberr != nil {
		fmt.Println("ERR CMD QUERY:", query, "ERR:", dberr)
		err = dberr
		return
	}
	affected, _ = i.RowsAffected()
	last, _ = i.LastInsertId()
	return
}

func (db DBU) getPragma(pragma string) (status string) {
	row := db.QueryRow("PRAGMA " + pragma)
	err := row.Scan(&status)
	if err != nil {
		fmt.Println("pragma:", pragma, "error:", err)
		return
	}
	return
}

func (db DBU) Pragmas() (status map[string]string) {
	status = make(map[string]string, 0)
	for _, pragma := range pragmas {
		status[pragma] = db.getPragma(pragma)
	}
	return
}

func (db DBU) Stats() (stats []string) {
	status := db.Pragmas()
	stats = make([]string, 0, len(status))
	for _, pragma := range pragmas {
		stats = append(stats, pragma+": "+status[pragma])
	}
	return
}

func (db DBU) Databases() (t Table) {
	t, _ = db.Table("PRAGMA database_list")
	return
}

func DBInit(dbfile, script string) (db DBU, err error) {
	os.Mkdir(path.Dir(dbfile), 0777)
	var file *os.File
	if file, err = os.OpenFile(dbfile, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return
	}
	file.Close()
	if db, err = dbOpen(dbfile); err != nil {
		return
	}
	err = db.File(script)
	return
}

func OpenDatabase(db_file, db_script string) (db DBU) {
	if _, err := os.Stat(db_file); err != nil {
		panic("DB does not exist.")
	}
	db, err := dbOpen(db_file)
	if err != nil {
		panic("DATABASE ERROR:" + err.Error())
	}
    if len(db_script) > 0 {
        db.File(db_script)
    }
	return
}
