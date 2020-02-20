package gxutil


// MsSQLServerConn is a Microsoft SQL Server connection
type MsSQLServerConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *MsSQLServerConn) Init() error {
	conn.BaseConn = BaseConn{
		URL:  conn.URL,
		Type: "sqlserver",
	}

	return conn.BaseConn.LoadYAML()
}