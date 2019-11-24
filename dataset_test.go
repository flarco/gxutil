package gxutil

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	s3Bucket = os.Getenv("S3_BUCKET")
	s3Region = os.Getenv("S3_REGION")
)

func TestS3(t *testing.T) {

	csvPath := "templates/test1.1.csv"
	s3Path := "test/test1.1.csv"
	s3PathPq := "test/test1.1.parquet"

	csv1 := CSV{Path: csvPath}


	s3 := S3{
		Bucket: s3Bucket,
		Region: s3Region,
	}

	err := s3.Delete(s3Path)
	assert.NoError(t, err)

	err = s3.Delete(s3PathPq)
	assert.NoError(t, err)

	err = s3.Delete(s3Path + ".gz")
	assert.NoError(t, err)

	csvFile, err := os.Open(csvPath)
	assert.NoError(t, err)

	err = s3.WriteStream(s3Path, csvFile)
	assert.NoError(t, err)

	reader, err := csv1.NewReader()
	assert.NoError(t, err)
	if err != nil {
		return
	}
	gzReader := Compress(reader)
	err = s3.WriteStream(s3Path+".gz", gzReader)
	assert.NoError(t, err)

	s3Reader, err := s3.ReadStream(s3Path)
	assert.NoError(t, err)

	csvFile.Seek(0, 0)
	csvReaderOut, err := ioutil.ReadAll(csvFile)
	s3ReaderOut, err := ioutil.ReadAll(s3Reader)
	csvFile.Close()
	assert.NoError(t, err)
	assert.Equal(t, string(csvReaderOut), string(s3ReaderOut))

	s3Reader, err = s3.ReadStream(s3Path + ".gz")
	assert.NoError(t, err)

	gS3Reader, err := Decompress(s3Reader)
	assert.NoError(t, err)

	s3ReaderOut, err = ioutil.ReadAll(gS3Reader)
	assert.NoError(t, err)
	assert.Equal(t, string(csvReaderOut), string(s3ReaderOut))

}
func TestCSV(t *testing.T) {
	err := os.Remove("test2.csv")

	csv1 := CSV{Path: "templates/test1.csv"}

	// Test streaming read & write
	ds, err := csv1.ReadStream()
	assert.NoError(t, err)
	if err != nil {
		return
	}

	csv2 := CSV{Path: "test2.csv"}
	err = csv2.WriteStream(ds)
	assert.NoError(t, err)

	// Test read & write
	data, err := ReadCsv("test2.csv")
	assert.NoError(t, err)

	assert.Len(t, data.Columns, 7)
	assert.Len(t, data.Rows, 1000)
	assert.Equal(t, "AOCG,\n883", data.Records()[0]["first_name"])
	assert.Equal(t, "EKOZ,989", data.Records()[1]["last_name"])

	err = os.Remove("test0.csv")

	err = data.WriteCsv("test0.csv")
	assert.NoError(t, err)

	err = os.Remove("test0.csv")
	err = os.Remove("test2.csv")

	// csv3 := CSV{
	// 	File:   data.Reader,
	// 	Fields: csv1.Fields,
	// }
	// stream, err = csv3.ReadStream()
	// assert.NoError(t, err)

	// csv2 = CSV{
	// 	Path:   "test2.csv",
	// 	Fields: csv1.Fields,
	// }
	// err = csv2.WriteStream(stream)
	// assert.NoError(t, err)
	// err = os.Remove("test2.csv")

}
