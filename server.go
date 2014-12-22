package dbutil

import (
	"fmt"
	"io"
	"os"
	"time"
)

type QueryType int

const (
	Q_TABLE QueryType = iota
	Q_LIST
	Q_STRING
	Q_INSERT
	Q_DBG_ON
	Q_DBG_OFF
	Q_OBJ_GET
	Q_OBJ_UPDATE
	Q_OBJ_INSERT
	Q_OBJ_DELETE
	Q_OBJ_LIST
	Q_OBJ_QUERY
	Q_EXEC
	Q_STREAM_CSV
	Q_STREAM_TAB
	Q_STATS
	Q_PRAGMAS
)

type Reply struct {
	Obj interface{}
	Err error
}

type DBQuery struct {
	Kind   QueryType
	Query  string
	Args   []interface{}
	Obj    interface{}
	Reply  chan Reply
	Writer io.Writer
}

type DBC chan DBQuery

func Server(db_file, backup_dir string, backupFreq int) (DBC, error) {
	var modified time.Time
	dbc := make(chan DBQuery)
	db, err := Open(db_file, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Can't start DB server: ", err)
		return dbc, err
	}
	go func() {
		for {
			var err error
			var obj interface{}
			req := <-dbc
			if db.Debug {
				fmt.Fprintln(os.Stderr, "START:", req.Kind)
			}
			switch {
			case req.Kind == Q_DBG_ON:
				db.Debug = true
			case req.Kind == Q_DBG_OFF:
				db.Debug = false
			case req.Kind == Q_OBJ_UPDATE:
				err = db.ObjectUpdate(req.Obj)
				modified = time.Now()
			case req.Kind == Q_OBJ_INSERT:
				obj, err = db.ObjectInsert(req.Obj)
				modified = time.Now()
			case req.Kind == Q_OBJ_DELETE:
				err = db.ObjectDelete(req.Obj)
				modified = time.Now()
			case req.Kind == Q_EXEC:
				obj, err = db.Update(req.Query, req.Args...)
				modified = time.Now()
			case req.Kind == Q_INSERT:
				obj, err = db.Insert(req.Query, req.Args...)
				modified = time.Now()

			case req.Kind == Q_TABLE:
				obj, err = db.Table(req.Query, req.Args...)
			case req.Kind == Q_LIST:
				obj, err = db.Rows(req.Query, req.Args...)
			case req.Kind == Q_OBJ_GET:
				err = db.ObjectLoad(req.Obj, req.Query, req.Args...)
			case req.Kind == Q_OBJ_LIST:
				obj, err = db.ObjectList(req.Obj)
			case req.Kind == Q_OBJ_QUERY:
				obj, err = db.ObjectListQuery(req.Obj, req.Query, req.Args...)
			case req.Kind == Q_STREAM_CSV:
				err = db.StreamCSV(req.Writer, req.Query, req.Args...)
			case req.Kind == Q_STREAM_TAB:
				err = db.StreamTab(req.Writer, req.Query, req.Args...)
			case req.Kind == Q_PRAGMAS:
				obj = db.Pragmas()
			case req.Kind == Q_STATS:
				obj = db.Stats()
			case req.Kind == Q_STRING:
				obj, err = db.GetString(req.Query, req.Args...)
			}
			req.Reply <- Reply{obj, err}
			if db.Debug {
				fmt.Fprintln(os.Stderr, "DONE:", req.Kind)
			}
		}
	}()

	go func() {
		os.MkdirAll(backup_dir, 0755)
		f := time.Second * time.Duration(backupFreq)
		ticker := time.NewTicker(f)
		const ts = "20060102_0304"
		for t := range ticker.C {
			if modified.Add(f).After(time.Now()) {
				name := fmt.Sprintf("%s/backup_%s.db", backup_dir, t.Format(ts))
				if err = db.Backup(name); err != nil {
					fmt.Fprintln(os.Stderr, "Backup error:", err)
				}
			}
		}
	}()

	return dbc, nil
}

func NewDBQuery(kind QueryType, where string, args ...interface{}) DBQuery {
	return DBQuery{
		Kind:  kind,
		Query: where,
		Args:  args,
		Reply: make(chan Reply),
	}
}

func (d DBC) Debug(on bool) {
	k := Q_DBG_OFF
	if on {
		k = Q_DBG_ON
	}
	c := NewDBQuery(k, "")
	d <- c
	<-c.Reply
}

func (d DBC) StreamCSV(w io.Writer, query string, args ...interface{}) error {
	c := DBQuery{
		Kind:   Q_STREAM_CSV,
		Query:  query,
		Args:   args,
		Reply:  make(chan Reply),
		Writer: w,
	}
	d <- c
	r := <-c.Reply
	return r.Err
}

func (d DBC) StreamTab(w io.Writer, query string, args ...interface{}) error {
	c := DBQuery{
		Kind:   Q_STREAM_TAB,
		Query:  query,
		Args:   args,
		Reply:  make(chan Reply),
		Writer: w,
	}
	d <- c
	r := <-c.Reply
	return r.Err
}

func (d DBC) Table(where string, args ...interface{}) (Table, error) {
	c := NewDBQuery(Q_TABLE, where, args...)
	d <- c
	r := <-c.Reply
	return r.Obj.(Table), r.Err
}

func (d DBC) Pragmas() map[string]string {
	c := NewDBQuery(Q_PRAGMAS, "")
	d <- c
	r := <-c.Reply
	return r.Obj.(map[string]string)
}

func (d DBC) Stats() []string {
	c := NewDBQuery(Q_STATS, "")
	d <- c
	r := <-c.Reply
	return r.Obj.([]string)
}

func (d DBC) StringList(where string, args ...interface{}) []string {
	c := NewDBQuery(Q_LIST, where, args...)
	d <- c
	r := <-c.Reply
	return r.Obj.([]string)
}

func (d DBC) ObjectUpdate(o interface{}) error {
	c := NewDBQuery(Q_OBJ_UPDATE, "")
	c.Obj = o
	d <- c
	r := <-c.Reply
	return r.Err
}

func (d DBC) ObjectDelete(o interface{}) error {
	c := NewDBQuery(Q_OBJ_DELETE, "")
	c.Obj = o
	d <- c
	r := <-c.Reply
	return r.Err
}

func (d DBC) ObjectInsert(o interface{}) (int64, error) {
	c := NewDBQuery(Q_OBJ_INSERT, "")
	c.Obj = o
	d <- c
	r := <-c.Reply
	return r.Obj.(int64), r.Err
}

func (d DBC) ObjectList(o interface{}) (interface{}, error) {
	c := NewDBQuery(Q_OBJ_LIST, "")
	c.Obj = o
	d <- c
	r := <-c.Reply
	return r.Obj, r.Err
}

func (d DBC) GetString(query string, args ...interface{}) (string, error) {
	c := NewDBQuery(Q_STRING, query, args...)
	d <- c
	r := <-c.Reply
	return r.Obj.(string), r.Err
}

func (d DBC) Insert(query string, args ...interface{}) (int64, error) {
	c := NewDBQuery(Q_INSERT, query, args...)
	d <- c
	r := <-c.Reply
	return r.Obj.(int64), r.Err
}

func (d DBC) ObjectLoad(o interface{}, where string, args ...interface{}) error {
	c := NewDBQuery(Q_OBJ_GET, where, args...)
	c.Obj = o
	d <- c
	r := <-c.Reply
	return r.Err
}

func (d DBC) ObjectsWhere(o interface{}, where string, args ...interface{}) (interface{}, error) {
	c := NewDBQuery(Q_OBJ_QUERY, where, args...)
	c.Obj = o
	d <- c
	r := <-c.Reply
	return r.Obj, r.Err
}

func (d DBC) Exec(query string, args ...interface{}) (int64, error) {
	c := NewDBQuery(Q_EXEC, query, args...)
	d <- c
	r := <-c.Reply
	return r.Obj.(int64), r.Err
}
