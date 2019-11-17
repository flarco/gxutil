package gxutil

import (
	pq "github.com/lib/pq"
)

// PostgresConn is a Postgres connection
type PostgresConn struct {
	Connection
	URL string
 }


// Connect connects to a database using sqlx
func (conn *PostgresConn) Connect() error {

	conn.Connection = Connection{
		URL: conn.URL,
		Type: "postgres",
	}
	return conn.Connection.Connect()
}


// InsertStream inserts a stream into a table
func (conn *PostgresConn) InsertStream(tableFName string, columns []string, streamRow <-chan []interface{}) error {

	schema, table := splitTableFullName(tableFName)

	txn := conn.Db.MustBegin()

	stmt, err := txn.Prepare(pq.CopyInSchema(schema, table, columns...))
	if err != nil {
		return err
	}

	for row := range streamRow {
		// Do insert
		_, err := stmt.Exec(row...)
		if err != nil {
			txn.Rollback()
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		txn.Rollback()
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
