package gxutil

import (
	"encoding/csv"
	"io"
	"os"

	"github.com/jmoiron/sqlx"
)

// Dataset is a query returned dataset
type Dataset struct {
	Result  *sqlx.Rows
	Fields  []string
	Records []map[string]interface{}
	Rows    [][]interface{}
	SQL     string
	Duration float64
}

// FromCsv converts from csv file
func (data *Dataset) FromCsv(file *os.File) error {
	// file, err := os.Open(path)
	// Check(err, "error opening file")
	defer file.Close()

	data.Result = nil
	data.Fields = []string{}
	data.Records = []map[string]interface{}{}
	data.Rows = [][]interface{}{}

	r := csv.NewReader(file)
	c := 0
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		Check(err, "error reading CSV line")

		c++
		if c == 1 {
			data.Fields = row
			continue
		} else {
			rec := make([]interface{}, len(row))
			recMap := map[string]interface{}{}
			for i, val := range row {
				rec[i] = val
				recMap[data.Fields[i]] = val
			}
			data.Rows = append(data.Rows, rec)
			data.Records = append(data.Records, recMap)
		}
	}
	return nil
}

// ToCsv converts to a csv file
func (data *Dataset) ToCsv(file *os.File) error {
	// file, err := os.Create(path)
	// Check(err, "error creating file")
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()
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
