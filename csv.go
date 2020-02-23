package gxutil

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cast"
)

// CSV is a csv object
type CSV struct {
	Path    string
	Columns []Column
	File    *os.File
	Data    *Dataset
	Reader  io.Reader
}

// ReadCsv reads CSV and returns dataset
func ReadCsv(path string) (Dataset, error) {
	file, err := os.Open(path)
	if err != nil {
		return Dataset{}, err
	}

	csv1 := CSV{
		File: file,
	}

	ds, err := csv1.ReadStream()
	if err != nil {
		return Dataset{}, err
	}

	data := ds.Collect()

	return data, nil
}

// ReadCsvStream reads CSV and returns datasream
func ReadCsvStream(path string) (Datastream, error) {

	csv1 := CSV{
		Path: path,
	}

	return csv1.ReadStream()
}

// InferSchema returns a sample of n rows
func (c *CSV) InferSchema() error {
	data, err := c.Sample(1000)
	if err != nil {
		return err
	}

	data.InferColumnTypes()
	c.Columns = data.Columns

	return nil
}

// Sample returns a sample of n rows
func (c *CSV) Sample(n int) (Dataset, error) {
	var data Dataset

	ds, err := c.ReadStream()
	if err != nil {
		return data, err
	}

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]interface{}{}
	count := 0
	for row := range ds.Rows {
		count++
		if count > n {
			break
		}
		data.Rows = append(data.Rows, row)
	}

	c.File = nil

	return data, nil
}

func toString(val interface{}) string {
	switch v := val.(type) {
	case time.Time:
		return cast.ToTime(val).Format("2006-01-02 15:04:05")
	default:
		_ = fmt.Sprint(v)
		return cast.ToString(val)
	}
}

func castVal(val interface{}, typ string) interface{} {
	var nVal interface{}
	switch typ {
	case "string", "text":
		nVal = cast.ToString(val)
	case "integer":
		nVal = cast.ToInt64(val)
	case "decimal":
		nVal = cast.ToFloat64(val)
	case "bool":
		nVal = cast.ToBool(val)
	case "datetime", "date", "timestamp":
		nVal = cast.ToTime(val)
	default:
		nVal = cast.ToString(val)
	}

	if cast.ToString(val) == "" {
		nVal = nil
	}
	return nVal
}

// ReadStream returns the read CSV stream with Line 1 as header
func (c *CSV) ReadStream() (ds Datastream, err error) {
	var r *csv.Reader

	if c.File == nil && c.Reader == nil {
		file, err := os.Open(c.Path)
		if err != nil {
			return ds, Error(err, "os.Open(c.Path)")
		}
		c.File = file
		c.Reader = bufio.NewReader(c.File)
	} else if c.File != nil {
		c.Reader = bufio.NewReader(c.File)
	}

	// decompress if gzip
	reader, err := Decompress(c.Reader)
	if err != nil {
		return ds, Error(err, "Decompress(c.Reader)")
	}

	r = csv.NewReader(reader)

	row0, err := r.Read()
	if err != nil {
		return ds, Error(err, "r.Read()")
	} else if err == io.EOF {
		return ds, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	ds = Datastream{
		Rows:    make(chan []interface{}),
		Columns: c.Columns,
		context: Context{ctx, cancel},
	}

	count := 1
	if ds.Columns == nil {
		ds.setFields(row0)

		// collect sample up to 1000 rows and infer types
		for {
			row0, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return ds, Error(err, "Error reading file")
			}

			row := make([]interface{}, len(row0))
			for i, val := range row0 {
				row[i] = castVal(val, ds.Columns[i].Type)
			}
			ds.Buffer = append(ds.Buffer, row)
			count++

			if count == 1000 {
				break
			}
		}

		sampleData := Dataset{Columns: ds.Columns, Rows: ds.Buffer}
		sampleData.InferColumnTypes()
		ds.Columns = sampleData.Columns
	}

	go func() {
		defer c.File.Close()

		for _, row := range ds.Buffer {
			for i, val := range row {
				row[i] = castVal(val, ds.Columns[i].Type)
			}
			ds.Rows <- row
		}

		for {
			row0, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				Check(err, "Error reading file")
				break
			}

			row := make([]interface{}, len(row0))
			for i, val := range row0 {
				row[i] = castVal(val, ds.Columns[i].Type)
			}

			select {
			case <-ds.context.ctx.Done():
				break
			default:
				ds.Rows <- row
			}
			count++
		}
		// Ensure that at the end of the loop we close the channel!
		close(ds.Rows)
		c.File = nil
	}()

	return ds, nil
}

// WriteStream to CSV file
func (c *CSV) WriteStream(ds Datastream) (cnt uint64, err error) {

	if c.File == nil {
		file, err := os.Create(c.Path)
		if err != nil {
			return cnt, err
		}
		c.File = file
	}

	defer c.File.Close()

	w := csv.NewWriter(c.File)

	err = w.Write(ds.GetFields())
	if err != nil {
		return cnt, Error(err, "error write row to csv file")
	}

	for row0 := range ds.Rows {
		cnt++
		row := make([]string, len(row0))
		for i, val := range row0 {
			row[i] = toString(val)
		}
		err := w.Write(row)
		if err != nil {
			return cnt, Error(err, "error write row to csv file")
		}
		w.Flush()
	}
	return cnt, nil
}

// NewReader creates a Reader
func (c *CSV) NewReader() (*io.PipeReader, error) {
	pipeR, pipeW := io.Pipe()
	ds, err := c.ReadStream()
	if err != nil {
		return nil, err
	}

	go func() {
		w := csv.NewWriter(pipeW)

		err := w.Write(ds.GetFields())
		if err != nil {
			Check(err, "Error writing ds.Fields")
			pipeW.Close()
		}

		for row0 := range ds.Rows {
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = toString(val)
			}
			err := w.Write(row)
			if err != nil {
				Check(err, "Error w.Write(row)")
				break
			}
			w.Flush()
		}
		pipeW.Close()
	}()

	return pipeR, nil
}
