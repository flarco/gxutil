package main

import (
	"os"
	"testing"
	"github.com/stretchr/testify/assert"
)

var (
	PostgresURL = os.Getenv("POSTGRES_URL") // https://github.com/lib/pq
	OracleURL = os.Getenv("ORACLE_URL") // https://github.com/godror/godror
	SQLServerURL = os.Getenv("SQLSERVER_URL") // https://github.com/denisenkom/go-mssqldb
	MySQLURL = os.Getenv("MYSQL_URL") // https://github.com/go-sql-driver/mysql/
	SQLiteURL = os.Getenv("SQLITE_URL") // https://github.com/mattn/go-sqlite3
	SnowflakeURL = os.Getenv("SNOWFLAKE_URL") // https://github.com/snowflakedb/gosnowflake
)


func TestDbToDb(t *testing.T) {
	var err error
	assert.NoError(t, err)

	c := Config{
		srcDB: PostgresURL,
		tgtDB: PostgresURL,
	}
	runDbToDb(c)
}