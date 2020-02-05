package gxutil

import (
	"errors"
	"os"
	"strings"

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

// InsertStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) InsertStream(tableFName string, ds Datastream) (count uint64, err error) {

	s3 := S3{
		Bucket: conn.GetProp("s3Bucket"),
		Region: "us-east-1",
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

	err = s3.Delete(s3Path)

	fileCount := 0
	for {
		fileCount++
		s3PartPath := F("%s/%04d.gz", s3Path, fileCount)
		LogErrorExit(err)

		reader := ds.NewCsvReader(fileRowLimit) // limit the rows so we can split the files
		gzReader := Compress(reader)
		err = s3.WriteStream(s3PartPath, gzReader)
		LogErrorExit(err)
		Log(F("uploaded to s3://%s/%s", s3.Bucket, s3PartPath))

		if ds.closed {
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
