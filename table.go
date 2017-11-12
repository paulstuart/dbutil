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

// TableConfig contains tabwriter configuration settings
type TableConfig struct {
	Minwidth int  // Minimum column width
	Tabwidth int  // Tab width
	Padding  int  // Number of padding characters
	Padchar  byte // Padding character
	Flags    uint // Flags are noted above
}

func tabular(w io.Writer, header bool, config *TableConfig) (*tabwriter.Writer, StreamFunc) {
	if config == nil {
		//        minwidth, tabwidth, padding, padchar, flags
		config = &TableConfig{0, 8, 1, ' ', 0}
	}

	// Format in tab-separated columns with a tab stop of 8.
	tw := tabwriter.NewWriter(w, config.Minwidth, config.Tabwidth, config.Padding, config.Padchar, config.Flags)

	rower := func(values ...interface{}) {
		for i, v := range values {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			switch v := v.(type) {
			case []uint8:
				fmt.Fprint(tw, string(v))
			default:
				fmt.Fprint(tw, v)
			}
		}
		fmt.Fprint(tw, "\n")
	}

	return tw, func(columns []string, row int, values []interface{}) error {
		if header && row == 1 {
			fmt.Fprintln(tw, strings.Join(columns, "\t"))
			for i, col := range columns {
				columns[i] = strings.Repeat("=", len(col))
			}
			fmt.Fprintln(tw, strings.Join(columns, "\t"))
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
