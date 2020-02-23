package gxutil

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// RedshiftConn is a Redshift connection
type RedshiftConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *RedshiftConn) Init() error {
	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "redshift",
	}

	return conn.BaseConn.Init()
}

func isRedshift(URL string) (isRs bool) {
	isRs = false
	db, err := sqlx.Open("postgres", URL)
	if err != nil {
		return isRs
	}
	res, err := db.Queryx("select version() v")
	if err != nil {
		return isRs
	}
	res.Next()
	row, err := res.SliceScan()
	if err != nil {
		return isRs
	}
	if strings.Contains(strings.ToLower(cast.ToString(row[0])), "redshift") {
		isRs = true
	}
	db.Close()
	return isRs
}

// Unload unloads a query to S3
func (conn *RedshiftConn) Unload(sql string) (s3Path string, err error) {

	s3 := S3{
		Bucket: conn.GetProp("s3Bucket"),
		Region: "us-east-1",
	}

	s3Path = F("sling/stream/%s.csv", cast.ToString(Now()))
	AwsID := os.Getenv("AWS_ACCESS_KEY_ID")
	AwsAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	txn := conn.Db().MustBegin()

	sql = strings.ReplaceAll(strings.ReplaceAll(sql, "\n", " "), "'", "''")

	s3.Delete(s3Path)
	unloadSQL := R(
		conn.template.Core["unload"],
		"sql", sql,
		"s3_bucket", s3.Bucket,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)
	_, err = txn.Exec(unloadSQL)
	if err != nil {
		cleanSQL := strings.ReplaceAll(unloadSQL, AwsID, "*****")
		cleanSQL = strings.ReplaceAll(cleanSQL, AwsAccessKey, "*****")
		return s3Path, Error(err, "SQL Error:\n"+cleanSQL)
	}

	Log(F("Unloaded to s3://%s/%s", s3.Bucket, s3Path))

	return s3Path, err
}

// BulkExportStream reads in bulk
func (conn *RedshiftConn) BulkExportStream(sql string) (ds Datastream, err error) {
	var mux sync.Mutex
	maxWorkers := 5
	workers := 0
	done := 0

	s3 := S3{
		Bucket: conn.GetProp("s3Bucket"),
	}

	s3Path, err := conn.Unload(sql)
	if err != nil {
		return ds, Error(err, "Could not unload.")
	}

	s3PartPaths, err := s3.List(s3Path + "/")
	if err != nil {
		return ds, Error(err, "Could not s3.List for "+s3Path+"/")
	}

	mainCtx, cancel := context.WithCancel(context.Background())
	ds = Datastream{
		Rows:    make(chan []interface{}, 100000), // 100000 row limit in memory
		context: Context{mainCtx, cancel},
	}

	decompressAndStream := func(s3PartPath string, dsMain *Datastream) {
		// limit concurent workers
		for {
			if workers < maxWorkers {
				workers++
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Log(F("Reading from s3://%s/%s", s3.Bucket, s3PartPath))

		gzReader, err := s3.ReadStream(s3PartPath)
		LogError(Error(err, F("Could not s3.ReadStream for s3://%s/%s", s3.Bucket, s3PartPath)))

		csvPart := CSV{Reader: gzReader}
		dsPart, err := csvPart.ReadStream()
		if err != nil {
			LogError(Error(err, F("Could not csvPart.ReadStream() s3://%s/%s", s3.Bucket, s3PartPath)))
		}

		mux.Lock()
		if dsMain.Columns == nil && len(dsPart.Buffer) > 0 {
			dsMain.Columns = dsPart.Columns
		}
		mux.Unlock()

		// foward to channel, rows will came in disorder
		for row := range dsPart.Rows {
			select {
			case <-dsMain.context.ctx.Done():
				break
			case <-dsPart.context.ctx.Done():
				break
			default:
				dsMain.Rows <- row
			}
		}
		workers--
		done++

		if done == len(s3PartPaths) {
			close(dsMain.Rows)
		}
	}

	// need to iterate through the s3 objects
	for _, s3PartPath := range s3PartPaths {
		// need to read them and decompress them
		// then append to datastream
		go decompressAndStream(s3PartPath, &ds)
	}

	// loop until columns are parsed
	for {
		if ds.Columns != nil && ds.Columns[0].Type != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ds, err
}

// BulkImportStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) BulkImportStream(tableFName string, ds Datastream) (count uint64, err error) {
	var wg sync.WaitGroup

	s3 := S3{
		Bucket: conn.GetProp("s3Bucket"),
	}

	s3Path := F("sling/%s.csv", tableFName)
	AwsID := os.Getenv("AWS_ACCESS_KEY_ID")
	AwsAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	fileRowLimit := cast.ToInt(conn.GetProp("fileRowLimit"))
	if fileRowLimit == 0 {
		fileRowLimit = 500000
	}

	if s3.Bucket == "" {
		return count, errors.New("Need to set 's3Bucket' to copy to redshift")
	}

	if AwsID == "" || AwsAccessKey == "" {
		return count, errors.New("Need to set env vars 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to redshift")
	}

	compressAndUpload := func(bytesData []byte, s3PartPath string, wg *sync.WaitGroup) {
		defer wg.Done()
		gzReader := Compress(bytes.NewReader(bytesData))
		err := s3.WriteStream(s3PartPath, gzReader)
		if err != nil {
			LogError(Error(err, F("could not upload to s3://%s/%s", s3.Bucket, s3PartPath)))
			return
		}
		Log(F("uploaded to s3://%s/%s", s3.Bucket, s3PartPath))
	}

	err = s3.Delete(s3Path)
	if err != nil {
		return count, Error(err, "Could not s3.Delete: "+s3Path)
	}

	fileCount := 0
	for {
		fileCount++
		s3PartPath := F("%s/%04d.gz", s3Path, fileCount)

		reader := ds.NewCsvReader(fileRowLimit)
		bytesData, err := ioutil.ReadAll(reader)
		if err != nil {
			return count, Error(err, "Could not ioutil.ReadAll")
		}

		// need to kick off threads to compress and upload
		// separately to not slow query ingress.
		wg.Add(1)
		go compressAndUpload(bytesData, s3PartPath, &wg)

		if ds.closed {
			wg.Wait()
			break
		}
	}

	txn := conn.Db().MustBegin()

	sql := R(
		conn.template.Core["copy_to"],
		"tgt_table", tableFName,
		"s3_bucket", s3.Bucket,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)
	_, err = txn.Exec(sql)
	if err != nil {
		cleanSQL := strings.ReplaceAll(sql, AwsID, "*****")
		cleanSQL = strings.ReplaceAll(cleanSQL, AwsAccessKey, "*****")
		return count, Error(err, "SQL Error:\n"+cleanSQL)
	}

	err = txn.Commit()
	if err != nil {
		return count, Error(err, "Could not commit")
	}

	return ds.count, nil
}
