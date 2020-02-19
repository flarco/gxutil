package gxutil

import (
	"strings"
	"github.com/xo/dburl"
	"github.com/spf13/cast"
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

// GetDDL returns DDL for given table.
func (conn *MySQLConn) GetDDL(tableFName string) (string, error) {
	schema, table := splitTableFullName(tableFName)
	sql := R(
		conn.template.Metadata["ddl"],
		"schema", schema,
		"table", table,
	)
	data, err := conn.Query(sql)
	if err != nil {
		return "", err
	}

	if len(data.Rows) == 0 {
		return "", nil
	}

	return cast.ToString(data.Rows[0][1]), nil
}
