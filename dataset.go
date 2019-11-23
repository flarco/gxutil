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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// Datastream is a stream of rows
type Datastream struct {
	Columns []Column
	Rows    chan []interface{}
}

// Dataset is a query returned dataset
type Dataset struct {
	Result   *sqlx.Rows
	Columns  []Column
	Rows     [][]interface{}
	SQL      string
	Duration float64
}

// CSV is a csv object
type CSV struct {
	Path string
	File *os.File
	Data *Dataset
}

// Parquet is a parquet object
type Parquet struct {
	Path  string
	File  *os.File
	PFile source.ParquetFile
	Data  *Dataset
}

// S3 is a AWS s3 object
type S3 struct {
	Bucket string
	Region string
}

// Collect reads a stream and return a dataset
func Collect(ds *Datastream) Dataset {

	var data Dataset

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]interface{}{}

	for row := range ds.Rows {
		data.Rows = append(data.Rows, row)
	}

	return data
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

	data := Collect(&ds)

	return data, nil
}

// ReadCsvStream reads CSV and returns datasream
func ReadCsvStream(path string) (Datastream, error) {

	csv1 := CSV{
		Path: path,
	}

	return csv1.ReadStream()
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
			rec[i] = val.(string)
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
			// Type:     "string",
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

	for j := range data.GetFields() {
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

	data.Columns = columns
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
	}
	ds.setFields(row0)

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
			ds.Rows <- row

		}
		// Ensure that at the end of the loop we close the channel!
		close(ds.Rows)
	}()

	return ds, nil
}

// WriteStream to CSV file
func (c *CSV) WriteStream(ds Datastream) error {

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

	err := w.Write(ds.GetFields())
	if err != nil {
		return Error(err, "error write row to csv file")
	}

	for row0 := range ds.Rows {
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

// WriteStream  write to an S3 bucket (upload)
// Example: Database or CSV stream into S3 file
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
		Check(err, "Error downloading S3 File -> "+key)
		pipeW.Close()
	}()

	return pipeR, nil
}

// Delete deletes an s3 object at provided key
func (s *S3) Delete(key string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create S3 service client
	svc := s3.New(sess)

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return Error(err, "Unable to delete S3 object: "+key)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	return err
}

// NewReader creates a Reader
func (ds *Datastream) NewReader() *io.PipeReader {
	pipeR, pipeW := io.Pipe()

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
				row[i] = val.(string)
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

// Decompress uses gzip to decompress
func Decompress(reader io.Reader) (io.Reader, error) {
	gr, err := gzip.NewReader(reader)
	return gr, err
}

// WriteStream to Parquet file from datastream
func (p *Parquet) WriteStream(ds Datastream) error {

	if p.File == nil {
		file, err := os.Create(p.Path)
		if err != nil {
			return err
		}
		p.File = file
	}

	p.PFile = writerfile.NewWriterFile(p.File)

	defer p.File.Close()

	// Need to determine types
	md := []string{
		"name=Name, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Age, type=INT32",
		"name=Id, type=INT64",
		"name=Weight, type=FLOAT",
		"name=Sex, type=BOOLEAN",
	}

	pw, err := writer.NewCSVWriter(md, p.PFile, 4)
	if err != nil {
		return err
	}
	defer pw.Flush(true)

	for row := range ds.Rows {
		err := pw.Write(row)
		if err != nil {
			return Error(err, "error write row to parquet file")
		}
	}

	err = pw.WriteStop()

	return err
}
