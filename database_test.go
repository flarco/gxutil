package gxutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	// "github.com/gobuffalo/packr"
)

var (
	PostgresURL = os.Getenv("POSTGRES_URL")

	tablesDDL = `CREATE TABLE if not exists public.person (
			first_name text PRIMARY KEY,
			last_name text,
			email text
	);

	CREATE TABLE if not exists public.place (
			country text,
			city text NULL,
			telcode integer
	);

	CREATE INDEX idx_country_city ON public.place(country, city);
	
	CREATE TABLE if not exists public.transactions (
		"date" date NULL,
		description varchar NULL,
		original_description varchar NULL,
		amount numeric NULL,
		transaction_type varchar NULL,
		category varchar NULL,
		account_name varchar NULL,
		labels varchar NULL,
		notes varchar NULL
	);

	create or replace view public.place_vw as
	select * from public.place
	where telcode = 65;
	`
)

func TestPG(t *testing.T) {
	conn := Connection{
		URL: PostgresURL,
	}
	err := conn.Connect()
	assert.NoError(t, err)

	_, err = conn.DropTable("person", "place", "transactions")
	assert.NoError(t, err)

	conn.Db.MustExec(tablesDDL)
	conn.Db.MustExec(`truncate table place`)

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
	assert.Equal(t, "integer", data.Records[2]["data_type"])

	// GetDDL of table
	data, err = conn.GetDDL("public.place")
	assert.NoError(t, err)
	assert.Equal(t, `CREATE TABLE public.place
(
    country text NULL,
    city text NULL,
    telcode integer NULL
)`, data.Rows[0][0])

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
	assert.Contains(t, conn.Schemata.Tables, "public.person")
	assert.Len(t, sData.Tables["person"].Columns, 3)
	assert.Equal(t, "text", sData.Tables["person"].ColumnsMap["email"].Type)
	assert.Equal(t, int64(3), conn.Schemata.Tables["public.person"].ColumnsMap["email"].Position)

	// RunAnalysis field_stat
	values := map[string]interface{}{
		"t1": "public.place",
		"t2":"public.place",
		"t1_field":"t1.country",
		"t1_fields1":"country",
		"t1_filter":"1=1",
		"t2_field":"t2.country",
		"t2_fields1":"country",
		"t2_filter":"1=1",
		"conds":`lower(t1.country) = lower(t2.country)`,
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
}
