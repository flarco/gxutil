package gxutil

import (
	"fmt"
	pq "github.com/lib/pq"
)

// PostgresConn is a Postgres connection
type PostgresConn struct {
	BaseConn
	URL string
 }


// Connect connects to a database using sqlx
func (conn *PostgresConn) Connect() error {

	conn.BaseConn = BaseConn{
		URL: conn.URL,
		Type: "postgres",
	}
	return conn.BaseConn.Connect()
}


// InsertStream inserts a stream into a table
func (conn *PostgresConn) InsertStream(tableFName string, ds Datastream) (count uint64, err error) {

	columns := ds.GetFields()
	schema, table := splitTableFullName(tableFName)

	txn := conn.Db().MustBegin()

	stmt, err := txn.Prepare(pq.CopyInSchema(schema, table, columns...))
	if err != nil {
		return count, Error(err, "pq.CopyInSchema")
	}

	for row := range ds.Rows {
		count++
		// Do insert
		_, err := stmt.Exec(row...)
		if err != nil {
			txn.Rollback()
			return count, Error(err, "\n"+fmt.Sprint(row))
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		txn.Rollback()
		return count, Error(err, "stmt.Exec()")
	}

	err = stmt.Close()
	if err != nil {
		return count, Error(err, "stmt.Close()")
	}

	err = txn.Commit()
	if err != nil {
		return count, Error(err, "txn.Commit()")
	}

	return count, nil
}
