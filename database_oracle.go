package gxutil

import (
	"github.com/spf13/cast"
)

// OracleConn is a Postgres connection
type OracleConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *OracleConn) Init() error {
	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "oracle",
	}

	return conn.BaseConn.LoadYAML()
}

// GetDDL returns the DDL for a table or view
func (conn *OracleConn) GetDDL(tableFName string) (string, error) {

	schema, table := splitTableFullName(tableFName)
	sqlTable := R(
		conn.BaseConn.template.Metadata["ddl_table"],
		"schema", schema,
		"table", table,
	)
	sqlView := R(
		conn.BaseConn.template.Metadata["ddl_view"],
		"schema", schema,
		"table", table,
	)

	data, err := conn.Query(sqlView)
	if err != nil {
		return "", err
	}

	if len(data.Rows) == 0 {
		data, err = conn.Query(sqlTable)
		if err != nil {
			return "", err
		}
	}

	if len(data.Rows) == 0 {
		return "", nil
	}

	return cast.ToString(data.Rows[0][0]), nil
}
