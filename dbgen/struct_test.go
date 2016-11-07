package main

import (
	"time"
)

type testStruct struct {
	ID      int64     `sql:"id" key:"true" table:"teststruct"`
	Name    string    `sql:"name"`
	Kind    int       `sql:"kind"`
	Data    []byte    `sql:"data"`
	Created time.Time `sql:"created" update:"false" audit:"time"`
}

const testSchema = `create table teststruct (
	id integer not null primary key,
	name text,
	kind int,
	data blob,
	created     DATETIME DEFAULT CURRENT_TIMESTAMP
);`
