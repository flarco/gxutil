package gxutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	// "github.com/gobuffalo/packr"
)

var (
	PostgresURL = os.Getenv("POSTGRES_URL")

	tablesDDL = `CREATE TABLE if not exists person (
			first_name text,
			last_name text,
			email text
	);

	CREATE TABLE if not exists place (
			country text,
			city text NULL,
			telcode integer
	);
	
	CREATE TABLE if not exists transactions (
		"date" date NULL,
		description varchar NULL,
		original_description varchar NULL,
		amount numeric NULL,
		transaction_type varchar NULL,
		category varchar NULL,
		account_name varchar NULL,
		labels varchar NULL,
		notes varchar NULL
	)`
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
	assert.Equal(t, data.Records[0]["amount"], 65.657)

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

	// PrintV(data.Rows)

	// GetColumns
	data, err = conn.GetColumns("public.person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, data.Records[0]["data_type"], "text")

	// GetColumnsFull
	data, err = conn.GetColumnsFull("public.place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Equal(t, data.Records[2]["data_type"], "integer")

	// load Csv from seed file
	// box := packr.NewBox("./seeds")
	// file, err := box.Open("place.csv")
	// assert.NoError(t, err)
	// data.FromCsv(file)

	// import to database

	// select back to assert equality

	_, err = conn.DropTable("person", "place")
	assert.NoError(t, err)

}
