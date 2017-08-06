package dbutil

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
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

func reverse(s []string) {
	for i := 0; i < len(s)/2; i++ {
		x := len(s) - i - 1
		s[i], s[x] = s[x], s[i]
	}
}

// Tabular returns a Writer and a RowFunc for using with Stream()
func Tabular(w io.Writer, header bool) (*tabwriter.Writer, RowFunc) {
	if nil == w {
		w = os.Stdout
	}

	//tw := tabwriter.NewWriter(output io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint) *Writer
	// Format in tab-separated columns with a tab stop of 8.
	tw := tabwriter.NewWriter(w, 0, 8, 1, ' ', 0)
	//tw := tabwriter.NewWriter(w, 0, 6, 3, ' ', 0)

	rower := func(values ...interface{}) {
		tabs := len(values) - 1
		for i, v := range values {
			//fmt.Printf("V (%T) %v\n", v, v)
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
			fmt.Fprint(tw, "rowid", "\t")
			rower(head...)
		}
		fmt.Fprint(tw, row, "\t")
		rower(values...)
		return nil
	}
}
