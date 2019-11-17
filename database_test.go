package gxutil

import (
	"os"
	"testing"
	"time"
	"strings"

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

func TestPG(t *testing.T) {

	viewDdl := `
	create or replace view place_vw as
	select * from place
	where telcode = 65
	`
	conn := Connection{
		URL: PostgresURL,
	}
	err := conn.Connect()
	assert.NoError(t, err)

	gConn, err := conn.GetGormConn()
	assert.NoError(t, err)

	_, err = conn.DropTable("person", "place", "transactions")
	assert.NoError(t, err)

	// conn.Db.MustExec(tablesDDL)
	gConn.SingularTable(true)
	gConn.AutoMigrate(&person{}, &place{}, &transactions{})
	conn.Db.MustExec(viewDdl)

	tx := conn.Db.MustBegin()
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
	tx.MustExec("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
	tx.MustExec("INSERT INTO transactions (date, description, amount) VALUES ($1, $2, $3)", "2019-10-10", "test product", "65.657")
	tx.Commit()

	// Test Streaming
	streamRec, err := conn.StreamRecords(`select * from person`)
	assert.NoError(t, err)

	recs := []map[string]interface{}{}
	for rec := range streamRec {
		recs = append(recs, rec)
	}
	assert.Len(t, recs, 2)

	streamRow, err := conn.StreamRows(`select * from person`)
	assert.NoError(t, err)

	rows := [][]interface{}{}
	for row := range streamRow {
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
	assert.Len(t, data.Rows, 1)
	assert.Equal(t, 65.657, data.Records[0]["amount"])

	// GetSchemas
	data, err = conn.GetSchemas()
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetTables
	data, err = conn.GetTables("public")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetViews
	data, err = conn.GetViews("information_schema")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)
	assert.Greater(t, data.Duration, 0.0)

	// GetColumns
	data, err = conn.GetColumns("public.person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, "text", data.Records[0]["data_type"])

	// GetPrimarkKeys
	data, err = conn.GetPrimarkKeys("public.person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 1)
	assert.Equal(t, "first_name", data.Records[0]["column_name"])

	// GetIndexes
	data, err = conn.GetIndexes("public.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, "city", data.Records[1]["column_name"])

	// GetColumnsFull
	data, err = conn.GetColumnsFull("public.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, "bigint", data.Records[2]["data_type"])

	// GetDDL of table
	data, err = conn.GetDDL("public.place")
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE public.place\n(\n    country text NULL,\n    city text NULL,\n    telcode bigint NULL\n)", data.Rows[0][0])

	// GetDDL of view
	data, err = conn.GetDDL("public.place_vw")
	assert.NoError(t, err)
	assert.Equal(t, " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);", data.Rows[0][0])

	// load Csv from seed file
	// box := packr.NewBox("./seeds")
	// file, err := box.Open("place.csv")
	// assert.NoError(t, err)
	// data.FromCsv(file)

	// import to database

	// select back to assert equality

	// Test Schemata
	sData, err := conn.GetSchemata("public")
	assert.NoError(t, err)
	assert.Equal(t, "public", sData.Name)
	assert.Contains(t, sData.Tables, "person")
	assert.Contains(t, sData.Tables, "place_vw")
	assert.Contains(t, conn.Schemata.Tables, "public.person")
	assert.Len(t, sData.Tables["person"].Columns, 3)
	assert.Equal(t, "text", sData.Tables["person"].ColumnsMap["email"].Type)
	assert.Equal(t, true, sData.Tables["place_vw"].IsView)
	assert.Equal(t, int64(3), conn.Schemata.Tables["public.person"].ColumnsMap["email"].Position)

	// RunAnalysis field_stat
	values := map[string]interface{}{
		"t1":         "public.place",
		"t2":         "public.place",
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
	assert.Equal(t, 0.0, data.Records[0]["t1_null_cnt"])
	assert.Equal(t, 100.0, data.Records[1]["match_rate"])

	// RunAnalysisTable field_stat
	data, err = conn.RunAnalysisTable("table_count", "public.person", "public.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, int64(2), data.Records[0]["cnt"])
	assert.Equal(t, int64(3), data.Records[1]["cnt"])

	// RunAnalysisField field_stat
	data, err = conn.RunAnalysisField("field_stat", "public.person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, int64(2), data.Records[0]["tot_cnt"])
	assert.Equal(t, int64(0), data.Records[1]["f_dup_cnt"])


	// Drop all tables
	_, err = conn.DropTable("person", "place", "transactions")
	assert.NoError(t, err)

	conn.Close()
}

func TestSQLite(t *testing.T) {
	err := os.Remove(SQLiteURL)

	viewDdl := `
	create view place_vw as
	select * from place
	where telcode = 65
	`
	conn := Connection{
		URL:  SQLiteURL,
		Type: "sqlite3",
	}
	err = conn.Connect()
	assert.NoError(t, err)

	gConn, err := conn.GetGormConn()
	assert.NoError(t, err)

	_, err = conn.DropTable("person", "place", "transactions")
	assert.NoError(t, err)

	// conn.Db.MustExec(tablesDDL)
	gConn.SingularTable(true)
	gConn.AutoMigrate(&person{}, &place{}, &transactions{})
	gConn.Close()

	conn.Db.MustExec(viewDdl)
	tx := conn.Db.MustBegin()
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
	tx.MustExec("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", "1")
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", "852")
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", "65")
	tx.MustExec("INSERT INTO transactions (date, description, amount) VALUES ($1, $2, $3)", "2019-10-10", "test product", "65.657")
	tx.Commit()

	data, err := conn.Query(`select * from person`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)

	data, err = conn.Query(`select * from place`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)

	data, err = conn.Query(`select * from transactions`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 1)
	assert.Equal(t, 65.657, data.Records[0]["amount"])

	// GetSchemas
	data, err = conn.GetSchemas()
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetTables
	data, err = conn.GetTables("main")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetViews
	data, err = conn.GetViews("information_schema")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)
	assert.Greater(t, data.Duration, 0.0)

	// GetColumns
	data, err = conn.GetColumns("person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, "varchar(255)", data.Records[0]["data_type"])

	// GetPrimarkKeys
	data, err = conn.GetPrimarkKeys("main.person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 1)
	assert.Equal(t, "first_name", data.Records[0]["column_name"])

	// GetIndexes
	data, err = conn.GetIndexes("main.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, "city", data.Records[1]["column_name"])

	// GetColumnsFull
	data, err = conn.GetColumnsFull("main.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, "bigint", data.Records[2]["data_type"])

	// GetDDL of table
	data, err = conn.GetDDL("main.place")
	assert.NoError(t, err)
	assert.Equal(t, "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )", data.Rows[0][0])

	// GetDDL of view
	data, err = conn.GetDDL("main.place_vw")
	assert.NoError(t, err)
	assert.Equal(t, "CREATE VIEW place_vw as\n\tselect * from place\n\twhere telcode = 65", data.Rows[0][0])

	// load Csv from seed file
	// box := packr.NewBox("./seeds")
	// file, err := box.Open("place.csv")
	// assert.NoError(t, err)
	// data.FromCsv(file)

	// import to database

	// select back to assert equality

	// Test Schemata
	sData, err := conn.GetSchemata("main")
	assert.NoError(t, err)
	assert.Equal(t, "main", sData.Name)
	assert.Contains(t, sData.Tables, "person")
	assert.Contains(t, sData.Tables, "place_vw")
	assert.Contains(t, conn.Schemata.Tables, "main.person")
	assert.Len(t, sData.Tables["person"].Columns, 3)
	assert.Equal(t, "varchar(255)", sData.Tables["person"].ColumnsMap["email"].Type)
	assert.Equal(t, true, sData.Tables["place_vw"].IsView)
	assert.Equal(t, int64(3), conn.Schemata.Tables["main.person"].ColumnsMap["email"].Position)

	// RunAnalysis field_stat
	values := map[string]interface{}{
		"t1":         "main.place",
		"t2":         "main.place",
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
	assert.Equal(t, int64(0), data.Records[0]["t1_null_cnt"])
	assert.Equal(t, 100.0, data.Records[1]["match_rate"])

	// RunAnalysisTable field_stat
	data, err = conn.RunAnalysisTable("table_count", "main.person", "main.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Equal(t, int64(2), data.Records[0]["cnt"])
	assert.Equal(t, int64(3), data.Records[1]["cnt"])

	// RunAnalysisField field_stat
	data, err = conn.RunAnalysisField("field_stat", "main.person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, int64(2), data.Records[0]["tot_cnt"])
	assert.Equal(t, int64(0), data.Records[1]["f_dup_cnt"])

	// Drop all tables
	_, err = conn.DropTable("person", "place", "transactions")
	assert.NoError(t, err)

	err = os.Remove(SQLiteURL)
	assert.NoError(t, err)

	conn.Close()
}



func TestPGtoPG(t *testing.T) {
	srcConn := Connection{
		URL: PostgresURL,
	}
	tgtConn := Connection{
		URL: PostgresURL,
	}

	err := srcConn.Connect()
	assert.NoError(t, err)

	err = tgtConn.Connect()
	assert.NoError(t, err)

	srcTable := "bank.mint_transactions"
	tgtTable := "public.mint_transactions"

	data, err := srcConn.GetDDL(srcTable)
	assert.NoError(t, err)
	ddl := data.Rows[0][0].(string)
	newDdl := strings.Replace(ddl, srcTable, tgtTable, 1)

	_, err = tgtConn.DropTable(tgtTable)
	assert.NoError(t, err)

	_, err = tgtConn.Db.Exec(newDdl)
	assert.NoError(t, err)

	streamRow, err := srcConn.StreamRows(`select * from ` + srcTable)
	assert.NoError(t, err)

	err = tgtConn.InsertStream(tgtTable, srcConn.Data.Fields, streamRow)
	assert.NoError(t, err)

	srcConn.Close()
	tgtConn.Close()

}