package main

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	g "github.com/flarco/gxutil"
	"github.com/stretchr/testify/assert"
)

type testDB struct {
	name      string
	URL       string
	table     string
	conn      g.Connection
	connected bool
}

var (
	testFile1Bytes []byte
	PostgresURL    = testDB{name: "Postgres", URL: os.Getenv("POSTGRES_URL"), table: "public.test1"}   // https://github.com/lib/pq
	RedshiftURL    = testDB{name: "Redshift", URL: os.Getenv("REDSHIFT_URL"), table: "public.test1"}   // https://github.com/lib/pq
	OracleURL      = testDB{name: "Oracle", URL: os.Getenv("ORACLE_URL"), table: "public.test1"}       // https://github.com/godror/godror
	SQLServerURL   = testDB{name: "SQLServer", URL: os.Getenv("SQLSERVER_URL"), table: "public.test1"} // https://github.com/denisenkom/go-mssqldb
	MySQLURL       = testDB{name: "MySQL", URL: os.Getenv("MYSQL_URL"), table: "public.test1"}         // https://github.com/go-sql-driver/mysql/
	SQLiteURL      = testDB{name: "SQLite", URL: os.Getenv("SQLITE_URL"), table: "public.test1"}       // https://github.com/mattn/go-sqlite3
	SnowflakeURL   = testDB{name: "Snowflake", URL: os.Getenv("SNOWFLAKE_URL"), table: "public.test1"} // https://github.com/snowflakedb/gosnowflake
)

var DBs = []testDB{
	PostgresURL,
	// RedshiftURL,
	// OracleURL,
	// SQLServerURL,
	// MySQLURL,
	// SQLiteURL,
	// SnowflakeURL,
}

var DBPtrs [10]*testDB

// func TestDbConn(t *testing.T) {
// 	for i, DB := range DBs {
// 		DB.conn = g.GetConn(DB.URL)
// 		DB.conn.Connect()
// 		DBPtrs[i] = &DB
// 	}
// }

func TestInToDb(t *testing.T) {
	testFile1, err := os.Open("tests/test1.1.csv.gz")
	if err != nil {
		assert.NoError(t, err)
		return
	}

	tReader, err := g.Decompress(bufio.NewReader(testFile1))
	assert.NoError(t, err)
	testFile1Bytes, err = ioutil.ReadAll(tReader)

	for _, tgtDB := range DBs {
		testFile1.Seek(0, 0)

		cfg := Config{
			file:     testFile1,
			tgtDB:    tgtDB.URL,
			tgtTable: tgtDB.table,
		}
		runInToDB(cfg)
	}
}

func TestDbToDb(t *testing.T) {
	var err error
	assert.NoError(t, err)

	for _, srcDB := range DBs {
		for _, tgtDB := range DBs {
			cfg := Config{
				srcDB:    srcDB.URL,
				srcTable: srcDB.table,
				tgtDB:    tgtDB.URL,
				tgtTable: srcDB.table + "_copy",
			}
			runDbToDb(cfg)
		}
	}
}

func TestDbToOut(t *testing.T) {

	for _, srcDB := range DBs {
		filePath2 := g.F("tests/%s.out.csv", srcDB.name)
		testFile2, err := os.Create(filePath2)
		if err != nil {
			assert.NoError(t, err)
			return
		}

		srcTable := srcDB.table
		srcTableCopy := srcDB.table + "_copy"
		cfg := Config{
			srcDB:    srcDB.URL,
			srcTable: srcTableCopy,
			file:     testFile2,
		}
		runDbToOut(cfg)

		testFile2, err = os.Open(filePath2)
		assert.NoError(t, err)
		testFile2Bytes, err := ioutil.ReadAll(testFile2)

		equal := assert.Equal(t, string(testFile1Bytes), string(testFile2Bytes))

		if equal {
			err = os.Remove(filePath2)
			assert.NoError(t, err)
			srcDB.conn = g.GetConn(srcDB.URL)
			srcDB.conn.Connect()
			srcDB.conn.DropTable(srcTable)
			srcDB.conn.DropTable(srcTableCopy)
			srcDB.conn.Close()
		}
	}
}
