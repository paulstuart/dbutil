package dbutil

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
)

type adjustment struct {
	index  int
	filter func(string) string
}

type Row []string

type Table struct {
	Columns []string
	Rows    []Row
	Adjust  []adjustment
	SortCol int
}

// for testing if last row in whilst in a template
func (t Table) LoopLen() int {
	return len(t.Rows) - 1
}

func underlines(cols []string) []string {
	u := make([]string, len(cols))
	for i, c := range cols {
		u[i] = strings.Repeat("=", len(c))
	}
	return u
}

func reverse(s []string) {
	for i := 0; i < len(s)/2; i++ {
		x := len(s) - i - 1
		s[i], s[x] = s[x], s[i]
	}
}

func (t Table) Dumper(h io.Writer, header bool) {
	w := new(tabwriter.Writer)
	// reformat as specified
	for _, a := range t.Adjust {
		for i := range t.Rows {
			t.Rows[i][a.index] = a.filter(t.Rows[i][a.index])
		}
	}
	// Format in tab-separated columns with a tab stop of 8.
	if nil == h {
		h = os.Stdout
	}
	w.Init(h, 0, 8, 1, ' ', 0)
	rows := 0
	if header {
		// split column headers
		c := make([][]string, 0)
		for i, v := range t.Columns {
			cols := strings.Split(v, "_")
			reverse(cols)
			c = append(c, cols)
			if cnt := len(c[i]); cnt > rows {
				rows = cnt
			}
		}
		// reorganize into header rows
		head := make([][]string, 0)
		for i := rows - 1; i >= 0; i-- {
			r := make([]string, 0)
			val := ""
			for j := 0; j < len(t.Columns); j++ {
				col := c[j]
				if i < len(col) {
					val = col[i]
				} else {
					val = ""
				}
				r = append(r, val)
			}
			head = append(head, r)
		}
		// now print 'em
		for i := 0; i < rows; i++ {
			fmt.Fprintln(w, strings.Join(head[i], "\t"))
		}
		if rows > 0 {
			fmt.Fprintln(w, strings.Join(underlines(head[rows-1]), "\t"))
		}
	}
	for _, r := range t.Rows {
		fmt.Fprintln(w, strings.Join(r, "\t"))
	}
	w.Flush()
}

func (t Table) Print(header bool) {
	t.Dumper(os.Stdout, header)
}

func inSet(i int, cols ...int) bool {
	for _, col := range cols {
		if col == i {
			return true
		}
	}
	return false
}

func columns(r Row, cols ...int) []string {
	reply := make([]string, len(cols))
	for i, col := range cols {
		reply[i] = r[col]
	}
	return reply
}

func indicies(row []string, columns ...string) []int {
	indx := make([]int, len(columns))
	for i, col := range columns {
		for x, name := range row {
			if col == name {
				indx[i] = x
				break
			}
		}
	}
	return indx
}

func (r Row) diff(prior Row, cols ...int) Row {
	reply := make([]string, len(r))
	for i := range r {
		switch {
		case inSet(i, cols...):
		case len(r[i]) == 0 && len(prior[i]) == 0:
		case len(r[i]) == 0 && len(prior[i]) > 0:
			reply[i] = "deleted: " + prior[i]
		case len(r[i]) > 0 && len(prior[i]) == 0:
			reply[i] = "added: " + r[i]
		case r[i] != prior[i]:
			reply[i] = "changed: " + prior[i] + " ==> " + r[i]
		}
	}
	return reply
}

// generate table containing differences, cols are columns
// that are ignored but retained, e.g., timestamps
func (t Table) Diff(reversed bool, cols ...string) Table {
	indx := indicies(t.Columns, cols...)
	delta := Table{Columns: append(cols, "field", "action"), Rows: []Row{}}
	last := Row{}
	for i, row := range t.Rows {
		if i > 0 {
			var diffs Row
			if reversed {
				diffs = last.diff(row, indx...)
			} else {
				diffs = row.diff(last, indx...)
			}
			pref := columns(last, indx...)
			for c, diff := range diffs {
				if len(diff) > 0 {
					changed := append(pref, t.Columns[c], diff)
					delta.Rows = append(delta.Rows, changed)
				}
			}
		}
		last = row
	}
	return delta
}
