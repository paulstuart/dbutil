package dbutil

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

var (
	repl = strings.NewReplacer(
		"\n", "\\\\n",
		"\t", "\\\\t",
		"\r", "\\\\r",
		`"`, `\"`,
		"_", " ",
		"-", " ",
	)
)

func strlen(v interface{}) int {
	switch v := v.(type) {
	case string:
		return len(v)
	default:
		return 0
	}
}

func underlines(cols []interface{}) []interface{} {
	u := make([]interface{}, len(cols))
	for i, c := range cols {
		u[i] = strings.Repeat("=", strlen(c))
	}
	return u
}

// Tabular returns a Writer and a RowFunc for using with Stream()
func Tabular(w io.Writer, header bool) (*tabwriter.Writer, RowFunc) {
	if nil == w {
		w = testout
	}

	// tabwriter.NewWriter(output io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint) *Writer
	// Format in tab-separated columns with a tab stop of 8.
	tw := tabwriter.NewWriter(w, 0, 8, 1, ' ', 0)

	rower := func(values ...interface{}) {
		tabs := len(values) - 1
		for i, v := range values {
			switch v := v.(type) {
			case []uint8:
				fmt.Fprint(tw, string(v))
			default:
				fmt.Fprint(tw, v)
			}
			if i < tabs {
				fmt.Fprint(tw, "\t")
			} else {
				fmt.Fprint(tw, "\n")
			}
		}
	}

	return tw, func(columns []string, row int, values []interface{}) error {
		if header && row == 0 {
			head := make([]interface{}, len(columns))
			for i, col := range columns {
				head[i] = repl.Replace(col)
			}
			rower(head...)
			rower(underlines(head)...)
		}
		rower(values...)
		return nil
	}
}

// PrintTable prints a tabular format to the writer
func PrintTable(db *sql.DB, w io.Writer, header bool, query string, args ...interface{}) error {
	tw, table := Tabular(w, true)
	if err := NewStreamer(db).Stream(table, query, args...); err != nil {
		return err
	}
	tw.Flush()
	return nil
}
