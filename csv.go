package gxutil

import (
	"github.com/spf13/cast"
	"encoding/csv"
	"io"
	"os"
	"time"
	"fmt"
)

// CSV is a csv object
type CSV struct {
	Path string
	Columns []Column
	File *os.File
	Data *Dataset
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
	case "string":
		nVal = cast.ToString(val)
	case "integer":
		nVal = cast.ToInt64(val)
	case "decimal":
		nVal = cast.ToFloat64(val)
	case "bool":
		nVal = cast.ToBool(val)
	// case "datetime":
	// 	nVal = cast.ToTime(val)
	default:
		nVal = cast.ToString(val)
	}

	if cast.ToString(val) == "" {
		nVal = nil
	}
	return nVal
}

// ReadStream returns the read CSV stream
func (c *CSV) ReadStream() (Datastream, error) {
	var ds Datastream

	if c.File == nil {
		file, err := os.Open(c.Path)
		if err != nil {
			return ds, err
		}
		c.File = file
	}

	r := csv.NewReader(c.File)
	row0, err := r.Read()
	if err != nil {
		return ds, err
	} else if err == io.EOF {
		return ds, nil
	}

	ds = Datastream{
		Rows: make(chan []interface{}),
		Columns: c.Columns,
	}

	if ds.Columns == nil {
		ds.setFields(row0)
	}

	go func() {
		defer c.File.Close()
		sampleData := Dataset{
			Columns: ds.Columns,
		}

		inferAndPush := func() {
			// infers the column types and pushes to channel
			sampleData.InferColumnTypes()
			ds.Columns = sampleData.Columns
			for _, rowS := range sampleData.Rows {
				for i, val := range rowS {
					rowS[i] = castVal(val, ds.Columns[i].Type)
				}
				ds.Rows <- rowS
			}
		}

		count := 1
		for {
			row0, err := r.Read()
			if err == io.EOF {
				if len(sampleData.Rows) < 1000 {
					inferAndPush()
				}
				break
			} else if err != nil {
				Check(err, "Error reading file")
				break
			}

			row := make([]interface{}, len(row0))
			for i, val := range row0 {
				row[i] = castVal(val, ds.Columns[i].Type)
			}
			if count < 1000 {
				sampleData.Rows = append(sampleData.Rows, row)
			} else if count == 1000 {
				// need to infer type and push buffer
				sampleData.Rows = append(sampleData.Rows, row)
				inferAndPush()
			} else {
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
