package gxutil

import (
	"github.com/jinzhu/gorm"
	"github.com/jmoiron/sqlx"
)

type conn interface { 
	Close() error
	Connect() error
	GetGormConn() (*gorm.DB, error)
	LoadYAML() error 
	StreamRows(sql string) (<-chan []interface{}, error)
	Query(sql string) (Dataset, error)
}

type BaseConn struct {
	conn
	URL      string
	Type     string // the type of database for sqlx: postgres, mysql, sqlite
	Db       *sqlx.DB
	db       gorm.DB
	Data     Dataset
	Template Template
	Schemata Schemata
 }

type PostgresConnn struct {
	*BaseConn
 }


// Close closes the connection
func (conn *BaseConn) Close() error {
	return conn.Db.Close()
}
