package gxutil

import (
	"os"
	"testing"
	"github.com/stretchr/testify/assert"
	// "github.com/gobuffalo/packr"
)

func TestCSV(t *testing.T) {
	err := os.Remove("test2.csv")

	csv1 := CSV{
		Path: "templates/test1.csv",
	}
	csv2 := CSV{
		Path: "test2.csv",
	}

	// Test streaming read & write
	stream, err := csv1.ReadStream()
	assert.NoError(t, err)
	if err != nil {
		return
	}

	csv2.Fields = csv1.Fields
	err = csv2.WriteStream(stream)
	assert.NoError(t, err)

	// Test read & write
	data, err := ReadCSV("test2.csv")
	assert.NoError(t, err)

	assert.Len(t, data.Fields, 7)
	assert.Len(t, data.Rows, 1000)
	assert.Equal(t, "AOCG,\n883", data.Records[0]["first_name"])
	assert.Equal(t, "EKOZ,989", data.Records[1]["last_name"])

	err = os.Remove("test0.csv")

	err = data.WriteCsv("test0.csv")
	assert.NoError(t, err)
	
	err = os.Remove("test0.csv")
	err = os.Remove("test2.csv")

}