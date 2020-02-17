package gxutil

// MySQLConn is a Postgres connection
type MySQLConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *MySQLConn) Init() error {
	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "mysql",
	}

	return conn.BaseConn.LoadYAML()
}

// InsertStream inserts a stream into a table
// Bulk Insert: https://github.com/go-sql-driver/mysql/#load-data-local-infile-support
func (conn *MySQLConn) InsertStream(tableFName string, ds Datastream) (count uint64, err error) {

	return count, nil
}
