package gxutil

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"

	// "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Dataset is a query returned dataset
type Dataset struct {
	Result     *sqlx.Rows
	Fields     []string
	Records    []map[string]interface{}
	Rows       [][]interface{}
	SQL        string
	Duration   float64
	Stream     <-chan []interface{}
	StreamFunc func()
	pipeR      *io.PipeReader
}

// CSV is a csv object
type CSV struct {
	Path    string
	File    *os.File
	Fields  []string
	Records []map[string]interface{}
	Rows    [][]interface{}
}

// S3 is a AWS s3 object
type S3 struct {
	Bucket string
	Region string
}

// ReadCSV reads CSV and returns dataset
func ReadCSV(path string) (Dataset, error) {
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
		File: file,
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

// InferColumnTypes determines the columns types
func (data *Dataset) InferColumnTypes() []Column {
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

	for i, field := range data.Fields {
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
	c.File = nil

	return data, nil
}

// ReadStream returns the read CSV stream
func (c *CSV) ReadStream() (<-chan []interface{}, error) {
	if c.File == nil {
		file, err := os.Open(c.Path)
		if err != nil {
			return nil, err
		}
		c.File = file
	}

	c.Fields = []string{}
	c.Records = []map[string]interface{}{}
	c.Rows = [][]interface{}{}

	r := csv.NewReader(c.File)
	row0, err := r.Read()
	if err != nil {
		return nil, err
	} else if err == io.EOF {
		return nil, nil
	}

	c.Fields = row0
	chnl := make(chan []interface{})

	go func() {
		defer c.File.Close()

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

// WriteStream to CSV file
func (c *CSV) WriteStream(streamRow <-chan []interface{}) error {

	if c.File == nil {
		file, err := os.Create(c.Path)
		if err != nil {
			return err
		}
		c.File = file
	}

	defer c.File.Close()

	w := csv.NewWriter(c.File)
	defer w.Flush()

	err := w.Write(c.Fields)
	if err != nil {
		return Error(err, "error write row to csv file")
	}

	for row0 := range streamRow {
		row := make([]string, len(row0))
		for i, val := range row0 {
			row[i] = val.(string)
		}
		err := w.Write(row)
		if err != nil {
			return Error(err, "error write row to csv file")
		}
	}
	return nil
}

/*
WriteStream  write to an S3 bucket (upload)
Example: Database or CSV stream into S3 file
*/
func (s *S3) WriteStream(key string, reader io.Reader) error {
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))
	uploader := s3manager.NewUploader(sess)
	uploader.Concurrency = 10

	// Upload the file to S3.
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	return nil
}

type fakeWriterAt struct {
	w io.Writer
}

func (fw fakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

// ReadStream read from an S3 bucket (download)
// Example: S3 file stream into Database or CSV
func (s *S3) ReadStream(key string) (*io.PipeReader, error) {
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		// Write the contents of S3 Object to the file
		_, err := downloader.Download(
			fakeWriterAt{pipeW},
			&s3.GetObjectInput{
				Bucket: aws.String(s.Bucket),
				Key:    aws.String(key),
			})
		Check(err, "Error downloading S3 File -> " + key)
		pipeW.Close()
	}()

	return pipeR, nil
}


// NewReader creates a Reader
func (data *Dataset) NewReader() *io.PipeReader {
	pipeR, pipeW := io.Pipe()

	go func() {
		w := csv.NewWriter(pipeW)

		err := w.Write(data.Fields)
		if err != nil {
			pipeW.Close()
		}

		for row0 := range data.Stream {
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = val.(string)
			}
			err := w.Write(row)
			if err != nil {
				break
			}
			w.Flush()
		}
		pipeW.Close()
	}()

	return pipeR
}
