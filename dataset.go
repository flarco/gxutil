package gxutil

import (
	"encoding/csv"
	"io"
	"os"
	"time"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// Dataset is a query returned dataset
type Dataset struct {
	Result   *sqlx.Rows
	Fields   []string
	Records  []map[string]interface{}
	Rows     [][]interface{}
	SQL      string
	Duration float64
}

// CSV is a csv object
type CSV struct {
	Path    string
	file    *os.File
	Fields  []string
	Records []map[string]interface{}
	Rows    [][]interface{}
}

// ReadCSV reads CSV and returns dataset
func ReadCSV (path string) (Dataset, error) {
	var data Dataset

	file, err := os.Open(path)
	if err != nil {
		return data, err
	}

	err = data.LoadFile(file)
	if err != nil {
		return data, err
	}

	return data, nil
}

// LoadFile loads data from a file
func (data *Dataset) LoadFile(file *os.File) error {
	csv1 := CSV{
		file: file,
	}

	stream, err := csv1.ReadStream()
	if err != nil {
		return err
	}

	data.Result = nil
	data.Fields = csv1.Fields
	data.Records = []map[string]interface{}{}
	data.Rows = [][]interface{}{}

	for row := range stream {
		rec := map[string]interface{}{}
		for i, val := range row {
			rec[data.Fields[i]] = val
		}
		data.Rows = append(data.Rows, row)
		data.Records = append(data.Records, rec)
	}

	return nil
}

// WriteCsv writes to a csv file
func (data *Dataset) WriteCsv(path string) error {
	file, err := os.Create(path)

	w := csv.NewWriter(file)
	defer w.Flush()

	err = w.Write(data.Fields)
	Check(err, "error write row to csv file")

	for _, row := range data.Rows {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = val.(string)
		}
		err := w.Write(rec)
		Check(err, "error write row to csv file")
	}
	return nil
}


// string: "varchar"
// integer: "integer"
// decimal: "decimal"
// date: "date"
// datetime: "timestamp"
// timestamp: "timestamp"
// text: "text"

// ParseString return an interface
func ParseString (s string) interface{} {
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

// InferColumnTypes determines the columns types
func (data *Dataset) InferColumnTypes() []Column {
	const N = 1000 // Sample Size
	
	type ColumnStats struct {
		minLen int
		maxLen int
		min int64
		max int64
		nullCnt int64
		intCnt int64
		decCnt int64
		boolCnt int64
		stringCnt int64
		dateCnt int64
		totalCnt int64
	}

	var columns []Column
	var stats []ColumnStats


	for i, field := range data.Fields {
		columns = append(columns, Column{
			Name: field,
			Position: int64(i+1),
			Type: "string",
		})
		stats = append(stats, ColumnStats{})
	}

	for i, row := range data.Rows {
		if i > N {
			break
		}
		
		for j, val := range row {
			val = ParseString(val.(string))
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

	for j := range data.Fields {
		// PrintV(stats[j])
		if stats[j].stringCnt > 0 {
			if stats[j].maxLen > 255 {
				columns[j].Type = "text"
			} else {
				columns[j].Type = "string"
			}
		} else if stats[j].boolCnt + stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "bool"
		} else if stats[j].intCnt + stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "integer"
		} else if stats[j].dateCnt + stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "datetime"
		} else if stats[j].decCnt + stats[j].nullCnt == stats[j].totalCnt {
			columns[j].Type = "decimal"
		}
	}

	return columns
}

// Sample returns a sample of n rows
func (c *CSV) Sample(n int) (Dataset, error) {
	var data Dataset

	stream, err := c.ReadStream()
	if err != nil {
		return data, err
	}

	data.Result = nil
	data.Fields = c.Fields
	data.Records = []map[string]interface{}{}
	data.Rows = [][]interface{}{}
	count := 0
	for row := range stream {
		count++
		if count > n {
			break
		}
		rec := map[string]interface{}{}
		for i, val := range row {
			rec[data.Fields[i]] = val
		}
		data.Rows = append(data.Rows, row)
		data.Records = append(data.Records, rec)
	}

	// set nil reopen file
	c.file = nil

	return data, nil
}

// ReadStream returns the read CSV stream
func (c *CSV) ReadStream() (<-chan []interface{}, error) {
	if c.file == nil {
		file, err := os.Open(c.Path)
		if err != nil {
			return nil, err
		}
		c.file = file
	}


	c.Fields = []string{}
	c.Records = []map[string]interface{}{}
	c.Rows = [][]interface{}{}

	r := csv.NewReader(c.file)
	row0, err := r.Read()
	if err != nil {
		return nil, err
	} else if err == io.EOF {
		return nil, nil
	}

	c.Fields = row0
	chnl := make(chan []interface{})

	go func() {
		defer c.file.Close()

		count := 1
		for {
			row0, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				Check(err, "Error reading file")
				break
			} 

			count++
			row := make([]interface{}, len(row0))
			for i, val := range row0 {
				row[i] = val
			}
			chnl <- row

		}
		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()

	return chnl, nil
}

// WriteStream frotom CSV file
func (c *CSV) WriteStream(streamRow <-chan []interface{}) error {

	if c.file == nil {
		file, err := os.Create(c.Path)
		if err != nil {
			return err
		}
		c.file = file
	}

	defer c.file.Close()

	w := csv.NewWriter(c.file)
	defer w.Flush()

	err := w.Write(c.Fields)
	if err != nil {
		return Error(err, "error write row to csv file")
	}
	
	for row := range streamRow {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = val.(string)
		}
		err := w.Write(rec)
		Check(err, "error write row to csv file")
	}
	return nil
}
