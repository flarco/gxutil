package gxutil

import (
	"bytes"
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

// Connect connects to a database using sqlx
func (conn *RedshiftConn) Connect() error {

	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "redshift",
	}
	return conn.BaseConn.Connect()
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

func (conn *RedshiftConn) unload(sql string) (s3Path string, err error) {

	s3 := S3{
		Bucket: conn.GetProp("s3Bucket"),
		Region: "us-east-1",
	}

	s3Path = F("sling/stream/%s.csv", cast.ToString(Now()))
	AwsID := os.Getenv("AWS_ACCESS_KEY_ID")
	AwsAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	txn := conn.Db().MustBegin()

	sql = strings.ReplaceAll(strings.ReplaceAll(sql, "\n", " "), "'", "''")
	unloadSQL := R(
		conn.template.Core["unload"],
		"sql", sql,
		"s3_bucket", s3.Bucket,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)
	_, err = txn.Exec(unloadSQL)
	Log(F("Unloaded to s3://%s/%s", s3.Bucket, s3Path))

	return s3Path, err
}

// BulkStream reads in bulk
func (conn *RedshiftConn) BulkStream(sql string) (ds Datastream, err error) {

	maxWorkers := 5
	workers := 0
	done := 0

	s3 := S3{
		Bucket: conn.GetProp("s3Bucket"),
	}

	s3Path, err := conn.unload(sql)
	LogErrorExit(err)

	s3PartPaths, err := s3.List(s3Path + "/")
	LogErrorExit(err)

	ds = Datastream{
		Rows: make(chan []interface{}),
	}

	decompressAndStream := func(s3PartPath string) {
		// limit concurent workers
		for {
			if workers < maxWorkers {
				workers++
				break
			}
			time.Sleep(100)
		}

		gzReader, err := s3.ReadStream(s3PartPath)
		LogErrorExit(err)

		reader, err := Decompress(gzReader)
		LogErrorExit(err)

		csvPart := CSV{Reader: reader}
		dsPart, err := csvPart.ReadStream()

		if ds.Columns == nil {
			ds.Columns = dsPart.Columns
		}

		// foward to channel, rows will came in disorder
		for row := range dsPart.Rows {
			ds.Rows <- row
		}
		workers--
		done++

		if done == len(s3PartPaths) {
			close(ds.Rows)
		}
	}

	// need to iterate through the s3 objects
	for _, s3PartPath := range s3PartPaths {
		// need to read them and decompress them
		// then append to datastream
		go decompressAndStream(s3PartPath)
	}

	return ds, err
}

// InsertStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) InsertStream(tableFName string, ds Datastream) (count uint64, err error) {
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
		LogErrorExit(errors.New("Need to set 's3Bucket' to copy to redshift"))
	}

	if AwsID == "" || AwsAccessKey == "" {
		LogErrorExit(errors.New("Need to set env vars 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to redshift"))
	}

	compressAndUpload := func(bytesData []byte, s3PartPath string, wg *sync.WaitGroup) {
		defer wg.Done()
		gzReader := Compress(bytes.NewReader(bytesData))
		err := s3.WriteStream(s3PartPath, gzReader)
		LogErrorExit(err)
		Log(F("uploaded to s3://%s/%s", s3.Bucket, s3PartPath))
	}

	err = s3.Delete(s3Path)

	fileCount := 0
	for {
		fileCount++
		s3PartPath := F("%s/%04d.gz", s3Path, fileCount)
		LogErrorExit(err)

		reader := ds.NewCsvReader(fileRowLimit)
		bytesData, err := ioutil.ReadAll(reader)
		LogErrorExit(err)

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
	LogErrorExit(err)

	err = txn.Commit()
	LogErrorExit(err)

	return ds.count, nil
}
