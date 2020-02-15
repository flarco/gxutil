package gxutil

// MySQLConn is a Postgres connection
type MySQLConn struct {
	BaseConn
	URL string
}

// Connect connects to a database using sqlx
func (conn *MySQLConn) Connect() error {

	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "mysql",
	}
	
	return conn.BaseConn.Connect()
}

// InsertStream inserts a stream into a table
// Bulk Insert: https://github.com/go-sql-driver/mysql/#load-data-local-infile-support
func (conn *MySQLConn) InsertStream(tableFName string, ds Datastream) (count uint64, err error) {

	return count, nil
}
