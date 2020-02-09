package gxutil

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"bufio"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// Datastream is a stream of rows
type Datastream struct {
	Columns []Column
	Rows    chan []interface{}
	Buffer  [][]interface{}
	count   uint64
	closed  bool
}

// Dataset is a query returned dataset
type Dataset struct {
	Result   *sqlx.Rows
	Columns  []Column
	Rows     [][]interface{}
	SQL      string
	Duration float64
}

// WriteCsv writes to a csv file
func (data *Dataset) WriteCsv(path string) error {
	file, err := os.Create(path)

	w := csv.NewWriter(file)
	defer w.Flush()

	err = w.Write(data.GetFields())
	Check(err, "error write row to csv file")

	for _, row := range data.Rows {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = cast.ToString(val)
		}
		err := w.Write(rec)
		Check(err, "error write row to csv file")
	}
	return nil
}

// ParseString return an interface
// string: "varchar"
// integer: "integer"
// decimal: "decimal"
// date: "date"
// datetime: "timestamp"
// timestamp: "timestamp"
// text: "text"
func ParseString(s string) interface{} {
	// int
	i, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return i
	}

	// date
	// layout := "2006-01-02T15:04:05.000Z"
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, s)
	if err == nil {
		return t
	}

	// float
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return f
	}

	// boolean
	b, err := strconv.ParseBool(s)
	if err == nil {
		return b
	}

	return s
}

// GetFields return the fields of the Data
func (ds *Datastream) GetFields() []string {
	fields := make([]string, len(ds.Columns))

	for j, column := range ds.Columns {
		fields[j] = column.Name
	}

	return fields
}

// setFields sets the fields/columns of the Datastream
func (ds *Datastream) setFields(fields []string) {
	ds.Columns = make([]Column, len(fields))

	for i, field := range fields {
		ds.Columns[i] = Column{
			Name:     field,
			Position: int64(i + 1),
			Type:     "",
		}
	}
}

// Collect reads a stream and return a dataset
func (ds *Datastream) Collect() Dataset {

	var data Dataset

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]interface{}{}

	for row := range ds.Rows {
		data.Rows = append(data.Rows, row)
	}

	return data
}

// InferTypes infers types if needed and add to Buffer
// Experimental....
func (ds *Datastream) InferTypes() {
	infer := false
	for _, col := range ds.Columns {
		if col.Type == "" {
			infer = true
		}
	}

	if !infer {
		return
	}

	c := 0
	for row := range ds.Rows {
		c++
		ds.Buffer = append(ds.Buffer, row)
		if c == 1000 {
			data := Dataset{
				Columns: ds.Columns,
				Rows:    ds.Buffer,
			}
			data.InferColumnTypes()
			ds.Columns = data.Columns
			buffer := make([][]interface{}, len(ds.Buffer))
			for i, row1 := range ds.Buffer {
				row2 := make([]interface{}, len(row1))
				for j, val := range row1 {
					row2[j] = castVal(val, ds.Columns[j].Type)
				}
				buffer[i] = row2
			}
			ds.Buffer = buffer // buffer is now typed
		}
	}
}

// GetFields return the fields of the Data
func (data *Dataset) GetFields() []string {
	fields := make([]string, len(data.Columns))

	for j, column := range data.Columns {
		fields[j] = column.Name
	}

	return fields
}

// setFields sets the fields/columns of the Datastream
func (data *Dataset) setFields(fields []string) {
	data.Columns = make([]Column, len(fields))

	for i, field := range fields {
		data.Columns[i] = Column{
			Name:     field,
			Position: int64(i + 1),
			// Type:     "string",
		}
	}
}

// setColumns sets the fields/columns of the Datastream
func (data *Dataset) setColumns(colTypes []*sql.ColumnType, NativeTypeMap map[string]string) {
	data.Columns = make([]Column, len(colTypes))

	for i, colType := range colTypes {
		Type := strings.ToLower(colType.DatabaseTypeName())
		Type = strings.Split(Type, "(")[0]

		if _, ok := NativeTypeMap[Type]; ok {
			Type = NativeTypeMap[Type]
		} else if Type != "" {
			println(F("setColumns - type '%s' not found for col '%s'", Type, colType.Name()))
		}

		data.Columns[i] = Column{
			Name:     colType.Name(),
			Position: int64(i + 1),
			Type:     Type,
			colType:  colType,
		}
	}

}

// Records return rows of maps
func (data *Dataset) Records() []map[string]interface{} {
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields() {
			rec[field] = row[j]
		}
		records[i] = rec
	}
	return records
}

// InferColumnTypes determines the columns types
func (data *Dataset) InferColumnTypes() {
	const N = 1000 // Sample Size

	type ColumnStats struct {
		minLen    int
		maxLen    int
		min       int64
		max       int64
		nullCnt   int64
		intCnt    int64
		decCnt    int64
		boolCnt   int64
		stringCnt int64
		dateCnt   int64
		totalCnt  int64
	}

	var columns []Column
	var stats []ColumnStats

	if len(data.Rows) == 0 {
		return
	}

	for i, field := range data.GetFields() {
		columns = append(columns, Column{
			Name:     field,
			Position: int64(i + 1),
			Type:     "string",
		})
		stats = append(stats, ColumnStats{})
	}

	for i, row := range data.Rows {
		if i > N {
			break
		}

		for j, val := range row {
			val = ParseString(cast.ToString(val))
			stats[j].totalCnt++

			switch v := val.(type) {
			case time.Time:
				stats[j].dateCnt++
			case nil:
				stats[j].nullCnt++
			case int, int8, int16, int32, int64:
				stats[j].intCnt++
				val0 := cast.ToInt64(val)
				if val0 > stats[j].max {
					stats[j].max = val0
				}
				if val0 < stats[j].min {
					stats[j].min = val0
				}
			case float32, float64:
				stats[j].decCnt++
				val0 := cast.ToInt64(val)
				if val0 > stats[j].max {
					stats[j].max = val0
				}
				if val0 < stats[j].min {
					stats[j].min = val0
				}
			case bool:
				stats[j].boolCnt++
			case string, []uint8:
				stats[j].stringCnt++
				l := len(cast.ToString(val))
				if l > stats[j].maxLen {
					stats[j].maxLen = l
				}
				if l < stats[j].minLen {
					stats[j].minLen = l
				}

			default:
				_ = fmt.Sprint(v)
			}
		}
	}

	for j := range data.GetFields() {
		// PrintV(stats[j])
		if stats[j].stringCnt > 0 || stats[j].nullCnt == stats[j].totalCnt {
			if stats[j].maxLen > 255 {
				columns[j].Type = "text"
			} else {
				columns[j].Type = "string"
			}
		} else if stats[j].boolCnt+stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "bool"
		} else if stats[j].intCnt+stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "integer"
		} else if stats[j].dateCnt+stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "datetime"
		} else if stats[j].decCnt+stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "decimal"
		}
	}

	data.Columns = columns
}

// NewCsvReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvReader(limit int) *io.PipeReader {
	pipeR, pipeW := io.Pipe()
	ds.count = 0

	go func() {
		c := uint64(0) // local counter
		w := csv.NewWriter(pipeW)

		err := w.Write(ds.GetFields())
		if err != nil {
			Check(err, "Error writing ds.Fields")
			pipeW.Close()
		}

		for row0 := range ds.Rows {
			c++
			ds.count++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = cast.ToString(val)
			}
			err := w.Write(row)
			if err != nil {
				Check(err, "Error w.Write(row)")
				break
			}
			w.Flush()

			if limit > 0 && c >= uint64(limit) {
				break // close reader if row limit is reached
			}
		}

		if limit == 0 || c < uint64(limit) {
			ds.closed = true
		}

		pipeW.Close()
	}()

	return pipeR
}

// Compress uses gzip to compress
func Compress(reader io.Reader) io.Reader {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	go func() {
		_, err := io.Copy(gw, reader)
		Check(err, "Error gzip writing: io.Copy(gw, reader)")
		gw.Close()
		pw.Close()
	}()

	return pr
}

// Decompress uses gzip to decompress if it is gzip. Otherwise return same reader
func Decompress(reader io.Reader) (gReader io.Reader, err error) {

	bReader := bufio.NewReader(reader)
	testBytes, err := bReader.Peek(2)
	if err != nil {
		return bReader, err
	}

	// https://stackoverflow.com/a/28332019
	if testBytes[0] == 31 && testBytes[1] == 139 {
		// is gzip 
		gReader, err = gzip.NewReader(bReader)
		if err != nil {
			return bReader, err
		}
	} else {
		gReader = bReader
	}

	return gReader, err
}
