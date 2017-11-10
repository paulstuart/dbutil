package dbutil

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

const (
	// Flags copied from tabwriter as a convenience

	// FilterHTML ignores html tags and treat entities (starting with '&'
	// and ending in ';') as single characters (width = 1).
	FilterHTML = tabwriter.FilterHTML

	// StripEscape strips escape characters bracketing escaped text segments
	// instead of passing them through unchanged with the text.
	StripEscape = tabwriter.StripEscape

	// AlignRight forces right-alignment of cell content.
	// Default is left-alignment.
	AlignRight = tabwriter.AlignRight

	// DiscardEmptyColumns handles empty columns as if they were not present in
	// the input in the first place.
	DiscardEmptyColumns = tabwriter.DiscardEmptyColumns

	// TabIndent always use tabs for indentation columns (i.e., padding of
	// leading empty cells on the left) independent of padchar.
	TabIndent = tabwriter.TabIndent

	// Debug prints a vertical bar ('|') between columns (after formatting).
	// Discarded columns appear as zero-width columns ("||").
	Debug = tabwriter.Debug
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

// TableConfig contains tabwriter configuration settings
type TableConfig struct {
	Minwidth int  // Minimum column width
	Tabwidth int  // Tab width
	Padding  int  // Number of padding characters
	Padchar  byte // Padding character
	Flags    uint // Flags are noted above
}

// defaultConfig returns a TableConfig struct with reasonable defaults
func defaultConfig() *TableConfig {
	// minwidth, tabwidth, padding, padchar, flags
	return &TableConfig{0, 8, 1, ' ', 0}
}

func tabular(w io.Writer, header bool, config *TableConfig) (*tabwriter.Writer, RowFunc) {
	if config == nil {
		config = defaultConfig()
	}

	// Format in tab-separated columns with a tab stop of 8.
	tw := tabwriter.NewWriter(w, config.Minwidth, config.Tabwidth, config.Padding, config.Padchar, config.Flags)

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

// Table prints a tabular format to the writer
// Formatting is controlled by TableConfig values
func (s *Streamer) Table(w io.Writer, header bool, config *TableConfig) error {
	tw, table := tabular(w, header, config)
	defer tw.Flush()
	return s.Stream(table)
}
