package gxutil

import (
	"strings"
	"github.com/xo/dburl"
)

// MySQLConn is a Postgres connection
type MySQLConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *MySQLConn) Init() error {

	u, err := dburl.Parse(conn.URL)
	if err != nil {
		return err
	}

	// Add tcp explicitly...
	// https://github.com/go-sql-driver/mysql/issues/427#issuecomment-474034276
	conn.URL = strings.ReplaceAll(
		conn.URL,
		"@"+u.Host,
		F("@tcp(%s)", u.Host),
	)

	// remove scheme
	conn.URL = strings.ReplaceAll(
		conn.URL,
		"mysql://",
		"",
	)

	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "mysql",
	}

	return conn.BaseConn.LoadYAML()
}
