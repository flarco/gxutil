package gxutil

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