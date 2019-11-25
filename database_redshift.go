package gxutil

import (
	"strings"
	"os"
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// RedshiftConn is a Redshift connection
type RedshiftConn struct {
	BaseConn
	URL      string
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
	res.Next()
	row, err := res.SliceScan()
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
		Bucket: conn.properties["s3Bucket"],
		Region: "us-east-1",
	}

	s3Path := F("sling/%s.csv", tableFName)
	AwsID := os.Getenv("AWS_ACCESS_KEY_ID")
	AwsAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if s3.Bucket == "" {
		LogErrorExit(errors.New("Need to set 's3Bucket' to copy to redshift"))
	} 

	if AwsID == "" || AwsAccessKey == "" {
		LogErrorExit(errors.New("Need to set env vars 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to redshift"))
	}

	s3Path = s3Path+".gz"
	err = s3.Delete(s3Path)
	LogErrorExit(err)

	reader := ds.NewCsvReader()
	gzReader := Compress(reader)
	err = s3.WriteStream(s3Path, gzReader)
	LogErrorExit(err)

	Log(F("uploaded to s3://%s/%s", s3.Bucket, s3Path))

	sql := R(`COPY {tgt_table}
	FROM 's3://{s3_bucket}/{s3_path}'
	credentials 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
	escape delimiter ',' EMPTYASNULL BLANKSASNULL GZIP IGNOREHEADER 1 REMOVEQUOTES
	-- TIMEFORMAT AS 'MM.DD.YYYY HH:MI:SS' DATEFORMAT AS 'MM.DD.YYYY'
	`,
	"tgt_table", tableFName,
	"s3_bucket", s3.Bucket,
	"s3_path", s3Path,
	"aws_access_key_id", AwsID,
	"aws_secret_access_key", AwsAccessKey,
	)
	LogErrorExit(err)

	txn := conn.Db().MustBegin()
	_, err = txn.Exec(sql)
	LogErrorExit(err)

	err = txn.Commit()
	LogErrorExit(err)

	return ds.count, nil
}
