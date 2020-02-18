package gxutil

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	// "github.com/gobuffalo/packr"
)

var (
	PostgresURL = os.Getenv("POSTGRES_URL")
	SQLiteURL   = "./test.db"
)

type person struct {
	FirstName string `gorm:"primary_key" json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
}

type place struct {
	Country string `json:"country" gorm:"index:idx_country_city"`
	City    string `json:"city" gorm:"index:idx_country_city"`
	Telcode int64  `json:"telcode"`
}

type transactions struct {
	Date                time.Time `json:"date" `
	Description         string    `json:"description"`
	OriginalDescription string    `json:"original_description"`
	Amount              float64   `json:"amount"`
	TransactionType     string    `json:"transaction_type"`
	Category            string    `json:"category"`
	AccountName         string    `json:"account_name"`
	Labels              string    `json:"labels"`
	Notes               string    `json:"notes"`
}

type testDB struct {
	conn       Connection
	name       string
	URL        string
	viewDDL    string
	schema     string
	placeDDL   string
	placeVwDDL string
}

var DBs = []*testDB{
	&testDB{
		name:       "postgres",
		URL:        os.Getenv("POSTGRES_URL"),
		viewDDL:    `create or replace view place_vw as select * from place where telcode = 65`,
		schema:     "public",
		placeDDL:   "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
		placeVwDDL: " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);",
	},
	&testDB{
		name: "sqlilte3",
		URL:  "file:./test.db",
		viewDDL: `create view place_vw as select * from place where telcode = 65`,
		schema:     "main",
		placeDDL:   "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
		placeVwDDL: "CREATE VIEW place_vw as select * from place where telcode = 65",
	},
	// &testDB{
	// 	name: "MySQL",
	// 	URL:  os.Getenv("MYSQL_URL"),
	// 	viewDDL: `create view place_vw as select * from place where telcode = 65`,
	// 	schema:     "main",
	// 	placeDDL:   "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
	// 	placeVwDDL: "CREATE VIEW place_vw as select * from place where telcode = 65",
	// },
	// &testDB{
	// 	name: "sqlserver",
	// 	URL:  os.Getenv("MSSQL_URL"),
	// 	viewDDL: `create view place_vw as select * from place where telcode = 65`,
	// 	schema:     "main",
	// 	placeDDL:   "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
	// 	placeVwDDL: "CREATE VIEW place_vw as select * from place where telcode = 65",
	// },
	// &testDB{
	// 	name: "oracle",
	// 	URL:  os.Getenv("ORACLE_URL"),
	// 	viewDDL: `create view place_vw as select * from place where telcode = 65`,
	// 	schema:     "system",
	// 	placeDDL:   "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
	// 	placeVwDDL: "CREATE VIEW place_vw as select * from place where telcode = 65",
	// },
}

func init() {
	for _, db := range DBs {
		if db.URL == "" {
			log.Fatal("No Env Var URL for " + db.name)
		} else if db.name == "SQLite" {
			os.Remove(strings.ReplaceAll(db.URL, "file:", ""))
		}
		db.conn = GetConn(db.URL)
		err := db.conn.Connect()
		LogErrorExit(err)
	}
}

func TestDBs(t *testing.T) {
	for _, db := range DBs {
		DBTest(t, db)
		if db.name == "sqlilte3" {
			os.Remove(strings.ReplaceAll(db.URL, "file:", ""))
		}
	}
}

func DBTest(t *testing.T, db *testDB) {
	println("Testing " + db.name)

	conn := db.conn
	gConn, err := conn.GetGormConn()
	assert.NoError(t, err)

	err = conn.DropTable(db.schema + ".person", db.schema + ".place", db.schema + ".transactions")
	assert.NoError(t, err)

	// conn.Db().MustExec(tablesDDL)
	gConn.SingularTable(true)
	gConn.AutoMigrate(&person{}, &place{}, &transactions{})
	conn.Db().MustExec(db.viewDDL)

	tx := conn.Db().MustBegin()
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
	tx.MustExec("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
	tx.MustExec("INSERT INTO transactions (date, description, amount) VALUES ($1, $2, $3)", "2019-10-10", "test\" \nproduct", "65.657")
	tx.MustExec("INSERT INTO transactions (date, description, amount) VALUES ($1, $2, $3)", "2020-10-10", "new \nproduct", "5.657")
	tx.Commit()

	// Test Streaming
	streamRec, err := conn.StreamRecords(`select * from person`)
	assert.NoError(t, err)

	recs := []map[string]interface{}{}
	for rec := range streamRec {
		recs = append(recs, rec)
	}
	assert.Len(t, recs, 2)

	stream, err := conn.StreamRows(`select * from person`)
	assert.NoError(t, err)

	rows := [][]interface{}{}
	for row := range stream.Rows {
		rows = append(rows, row)
	}
	assert.Len(t, rows, 2)

	data, err := conn.Query(`select * from person`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)

	data, err = conn.Query(`select * from place`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)

	data, err = conn.Query(`select * from transactions`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, 65.657, data.Records()[0]["amount"])

	// GetSchemas
	data, err = conn.GetSchemas()
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetTables
	data, err = conn.GetTables(db.schema)
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetViews
	data, err = conn.GetViews("information_schema")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)
	assert.Greater(t, data.Duration, 0.0)

	// GetColumns
	data, err = conn.GetColumns(db.schema + ".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Contains(t, []string{"text", "varchar(255)"}, data.Records()[0]["data_type"])

	// GetPrimarkKeys
	data, err = conn.GetPrimarkKeys(db.schema + ".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 1)
	assert.Equal(t, "first_name", data.Records()[0]["column_name"])

	// GetIndexes
	data, err = conn.GetIndexes(db.schema + ".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, "city", data.Records()[1]["column_name"])

	// GetColumnsFull
	data, err = conn.GetColumnsFull(db.schema + ".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, "bigint", data.Records()[2]["data_type"])

	// GetDDL of table
	ddl, err := conn.GetDDL(db.schema + ".place")
	assert.NoError(t, err)
	assert.Equal(t, db.placeDDL, ddl)

	// GetDDL of view
	ddl, err = conn.GetDDL(db.schema + ".place_vw")
	assert.NoError(t, err)
	assert.Equal(t, db.placeVwDDL, ddl)

	// load Csv from test file
	csv1 := CSV{Path: "test/test1.csv"}

	stream, err = csv1.ReadStream()
	assert.NoError(t, err)

	csvTable := db.schema + ".test1"
	ddl, err = conn.GenerateDDL(csvTable, Dataset{Columns: stream.Columns, Rows: stream.Buffer})
	assert.NoError(t, err)

	_, err = conn.Db().Exec(ddl)
	assert.NoError(t, err)

	// import to database
	_, err = conn.InsertStream(csvTable, stream)
	assert.NoError(t, err)

	// select back to assert equality
	count, err := conn.GetCount(csvTable)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1000), count)

	// Test Schemata
	sData, err := conn.GetSchemata(db.schema)
	assert.NoError(t, err)
	assert.Equal(t, db.schema, sData.Name)
	assert.Contains(t, sData.Tables, "person")
	assert.Contains(t, sData.Tables, "place_vw")
	assert.Contains(t, conn.Schemata().Tables, db.schema+".person")
	assert.Len(t, sData.Tables["person"].Columns, 3)
	assert.Contains(t, []string{"text", "varchar(255)"}, sData.Tables["person"].ColumnsMap["email"].Type)
	assert.Equal(t, true, sData.Tables["place_vw"].IsView)
	assert.Equal(t, int64(3), conn.Schemata().Tables[db.schema+".person"].ColumnsMap["email"].Position)

	// RunAnalysis field_stat
	values := map[string]interface{}{
		"t1":         db.schema + ".place",
		"t2":         db.schema + ".place",
		"t1_field":   "t1.country",
		"t1_fields1": "country",
		"t1_filter":  "1=1",
		"t2_field":   "t2.country",
		"t2_fields1": "country",
		"t2_filter":  "1=1",
		"conds":      `lower(t1.country) = lower(t2.country)`,
	}
	data, err = conn.RunAnalysis("table_join_match", values)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []interface{}{0.0, int64(0)}, data.Records()[0]["t1_null_cnt"])
	assert.Equal(t, 100.0, data.Records()[1]["match_rate"])

	// RunAnalysisTable field_stat
	data, err = conn.RunAnalysisTable("table_count", db.schema+".person", db.schema+".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, int64(2), data.Records()[0]["cnt"])
	assert.Equal(t, int64(3), data.Records()[1]["cnt"])

	// RunAnalysisField field_stat
	data, err = conn.RunAnalysisField("field_stat", db.schema+".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, int64(2), data.Records()[0]["tot_cnt"])
	assert.Equal(t, int64(0), data.Records()[1]["f_dup_cnt"])

	// Drop all tables
	err = conn.DropTable("person", "place", "transactions", "test1")
	assert.NoError(t, err)

	conn.Close()
}

func PGtoPGTest(t *testing.T, srcTable string) {
	tgtTable := srcTable + "2"

	// var srcConn, tgtConn PostgresConn
	srcConn := GetConn(PostgresURL)
	tgtConn := GetConn(PostgresURL)

	err := srcConn.Connect()
	assert.NoError(t, err)

	err = tgtConn.Connect()
	assert.NoError(t, err)

	ddl, err := srcConn.GetDDL(srcTable)
	assert.NoError(t, err)
	newDdl := strings.Replace(ddl, srcTable, tgtTable, 1)

	err = tgtConn.DropTable(tgtTable)
	assert.NoError(t, err)

	_, err = tgtConn.Db().Exec(newDdl)
	assert.NoError(t, err)

	stream, err := srcConn.StreamRows(`select * from ` + srcTable)
	assert.NoError(t, err)

	if err == nil {
		_, err = tgtConn.InsertStream(tgtTable, stream)
		assert.NoError(t, err)

		data, err := tgtConn.RunAnalysisTable("table_count", srcTable, tgtTable)
		assert.NoError(t, err)
		assert.Equal(t, data.Records()[0]["cnt"], data.Records()[1]["cnt"])
	}

	// use Copy TO
	_, err = tgtConn.Query("truncate table " + tgtTable)
	assert.NoError(t, err)

	stream, err = srcConn.BulkStream(`select * from ` + srcTable)
	assert.NoError(t, err)

	if err == nil {
		_, err = tgtConn.InsertStream(tgtTable, stream)
		assert.NoError(t, err)

		data, err := tgtConn.RunAnalysisTable("table_count", srcTable, tgtTable)
		assert.NoError(t, err)
		assert.Equal(t, data.Records()[0]["cnt"], data.Records()[1]["cnt"])
	}

	err = tgtConn.DropTable(tgtTable)
	assert.NoError(t, err)

	srcConn.Close()
	tgtConn.Close()

}
