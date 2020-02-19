package gxutil

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cast"
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

type transact struct {
	Datetime            time.Time `json:"date" `
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
	conn          Connection
	name          string
	URL           string
	schema        string
	transactDDL   string
	personDDL     string
	placeDDL      string
	placeIndex    string
	placeVwDDL    string
	placeVwSelect string
}

var DBs = map[string]*testDB{
	"postgres": &testDB{
		name:       "postgres",
		URL:        os.Getenv("POSTGRES_URL"),
		schema:     "public",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:   "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex:    `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view place_vw as select * from place where telcode = 65`,
		placeVwSelect: " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);",
	},

	"sqlite3": &testDB{
		name: "sqlite3",
		URL:  "file:./test.db",
		schema:     "main",

		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:   "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
		placeIndex:    `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    "CREATE VIEW place_vw as select * from place where telcode = 65",
		placeVwSelect: "CREATE VIEW place_vw as select * from place where telcode = 65",
	},

	"mysql": &testDB{
		name: "mysql",
		URL:  os.Getenv("MYSQL_URL"),
		schema:     "mysql",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:   "CREATE TABLE `place` (\n  `country` varchar(255) DEFAULT NULL,\n  `city` varchar(255) DEFAULT NULL,\n  `telcode` decimal(10,0) DEFAULT NULL,\n  KEY `idx_country_city` (`country`,`city`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		placeIndex:    `select 1`, //`CREATE INDEX idx_country_city ON place(country, city)`,
		placeVwDDL:    `create or replace view place_vw as select * from place where telcode = 65`,
		placeVwSelect: "CREATE ALGORITHM=UNDEFINED DEFINER=`admin`@`%` SQL SECURITY DEFINER VIEW `place_vw` AS select `place`.`country` AS `country`,`place`.`city` AS `city`,`place`.`telcode` AS `telcode` from `place` where (`place`.`telcode` = 65)",
	},

	// "sqlserver": &testDB{
	// 	name: "sqlserver",
	// 	URL:  os.Getenv("MSSQL_URL"),
	// 	schema:     "public",
	// 	transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
	// 	personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
	// 	placeDDL:   "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
	// 	placeIndex:    `CREATE INDEX idx_country_city
	// 	ON place(country, city)`,
	// 	placeVwDDL:    `create or replace view place_vw as select * from place where telcode = 65`,
	// 	placeVwSelect: " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);",
	// },

	"oracle": &testDB{
		name:        "oracle",
		URL:         os.Getenv("ORACLE_URL"),
		schema:      "system",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "\n  CREATE TABLE \"SYSTEM\".\"PLACE\" \n   (\t\"COUNTRY\" VARCHAR2(255), \n\t\"CITY\" VARCHAR2(255), \n\t\"TELCODE\" NUMBER(*,0)\n   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING\n  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)\n  TABLESPACE \"SYSTEM\" ",
		placeIndex: `CREATE INDEX idx_country_city 
		ON place(country, city)`,
		placeVwDDL: "CREATE VIEW place_vw as select * from place where telcode = 65",
		placeVwSelect: "select \"COUNTRY\",\"CITY\",\"TELCODE\" from place where telcode = 65",
	},
}

func TestPostgres(t *testing.T) {
	DBTest(t, DBs["postgres"])
}

func TestSQLite(t *testing.T) {
	os.Remove(strings.ReplaceAll(DBs["sqlite3"].URL, "file:", ""))
	DBTest(t, DBs["sqlite3"])
}

func TestMySQL(t *testing.T) {
	DBTest(t, DBs["mysql"])
}

func TestOracle(t *testing.T) {
	DBTest(t, DBs["oracle"])
}

func DBTest(t *testing.T, db *testDB) {
	println("Testing " + db.name)
	if db.URL == "" {
		log.Fatal("No Env Var URL for " + db.name)
	} 
	conn := GetConn(db.URL)
	err := conn.Connect()
	assert.NoError(t, err)

	err = conn.DropTable(db.schema+".person", db.schema+".place", db.schema+".transact")
	assert.NoError(t, err)

	err = conn.DropView(db.schema + ".place_vw")
	assert.NoError(t, err)

	// gConn, err := conn.GetGormConn()
	// assert.NoError(t, err)
	// gConn.SingularTable(true)
	// gConn.AutoMigrate(&person{}, &place{}, &transact{})

	conn.Db().MustExec(db.transactDDL)
	conn.Db().MustExec(db.personDDL)
	conn.Db().MustExec(db.placeDDL)
	conn.Db().MustExec(db.placeIndex)
	conn.Db().MustExec(db.placeVwDDL)

	personInsertStatement := conn.GenerateInsertStatement("person", []string{"first_name", "last_name", "email"})
	placeInsertStatement := conn.GenerateInsertStatement("place", []string{"country", "city", "telcode"})
	transactInsertStatement := conn.GenerateInsertStatement("transact", []string{"date_time", "description", "amount"})

	tx := conn.Db().MustBegin()
	tx.MustExec(personInsertStatement, "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.MustExec(personInsertStatement, "John", "Doe", "johndoeDNE@gmail.net")
	tx.MustExec(placeInsertStatement, "United States", "New York", "1")
	tx.MustExec(placeInsertStatement, "Hong Kong", nil, "852")
	tx.MustExec(placeInsertStatement, "Singapore", nil, "65")
	tx.MustExec(transactInsertStatement, cast.ToTime("2019-10-10"), "test\" \nproduct", 65.657)
	tx.MustExec(transactInsertStatement, cast.ToTime("2020-10-10"), "new \nproduct", 5.657)
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

	data, err = conn.Query(`select * from transact`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, 65.657, cast.ToFloat64(data.Records()[0]["amount"]))

	// GetSchemas
	data, err = conn.GetSchemas()
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetTables
	data, err = conn.GetTables(db.schema)
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetViews
	data, err = conn.GetViews(db.schema)
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)
	assert.Greater(t, data.Duration, 0.0)

	// GetColumns
	data, err = conn.GetColumns(db.schema + ".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Contains(t, []string{"text", "varchar(255)", "VARCHAR2", "character varying", "varchar"}, data.Records()[0]["data_type"])

	// GetPrimaryKeys
	data, err = conn.GetPrimaryKeys(db.schema + ".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 1)
	assert.Equal(t, "first_name", strings.ToLower(cast.ToString(data.Records()[0]["column_name"])))

	// GetIndexes
	data, err = conn.GetIndexes(db.schema + ".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, "city", strings.ToLower(cast.ToString(data.Records()[1]["column_name"])))

	// GetColumnsFull
	data, err = conn.GetColumnsFull(db.schema + ".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Contains(t, []string{"bigint", "NUMBER", "decimal"}, data.Records()[2]["data_type"])

	// GetDDL of table
	ddl, err := conn.GetDDL(db.schema + ".place")
	assert.NoError(t, err)
	assert.Equal(t, db.placeDDL, ddl)

	// GetDDL of view
	ddl, err = conn.GetDDL(db.schema + ".place_vw")
	assert.NoError(t, err)
	assert.Equal(t, db.placeVwSelect, ddl)

	// load Csv from test file
	csv1 := CSV{Path: "test/test1.csv"}

	stream, err = csv1.ReadStream()
	assert.NoError(t, err)


	csvTable := db.schema + ".test1"
	ddl, err = conn.GenerateDDL(csvTable, Dataset{Columns: stream.Columns, Rows: stream.Buffer})
	assert.NoError(t, err)
	ok := assert.NotEmpty(t, ddl)

	if ok {
		_, err = conn.Db().Exec(ddl)
		assert.NoError(t, err)

		// import to database
		_, err = conn.InsertStream(csvTable, stream)
		assert.NoError(t, err)

		// select back to assert equality
		count, err := conn.GetCount(csvTable)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1000), count)
	}


	// Test Schemata
	sData, err := conn.GetSchemata(db.schema)
	assert.NoError(t, err)
	assert.Equal(t, db.schema, sData.Name)
	assert.Contains(t, sData.Tables, "person")
	assert.Contains(t, sData.Tables, "place_vw")
	assert.Contains(t, conn.Schemata().Tables, db.schema+".person")
	assert.Len(t, sData.Tables["person"].Columns, 3)
	assert.Contains(t, []string{"text", "varchar(255)", "VARCHAR2", "character varying", "varchar"}, sData.Tables["person"].ColumnsMap["email"].Type)
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
	assert.Contains(t, []interface{}{0.0, int64(0), "0"}, data.Records()[0]["t1_null_cnt"])
	assert.Equal(t, 100.0, cast.ToFloat64(data.Records()[1]["match_rate"]))

	// RunAnalysisTable field_stat
	data, err = conn.RunAnalysisTable("table_count", db.schema+".person", db.schema+".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.EqualValues(t, int64(2), data.Records()[0]["cnt"])
	assert.EqualValues(t, int64(3), data.Records()[1]["cnt"])

	// RunAnalysisField field_stat
	data, err = conn.RunAnalysisField("field_stat", db.schema+".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.EqualValues(t, int64(2), data.Records()[0]["tot_cnt"])
	assert.EqualValues(t, int64(0), data.Records()[1]["f_dup_cnt"])

	// Drop all tables
	err = conn.DropTable("person", "place", "transact", "test1")
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
