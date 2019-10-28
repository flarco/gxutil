package main

import (
	"log"
	"path"
	"runtime"
	"strings"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"github.com/gobuffalo/packr"
)


// Connection is a database connection
type Connection struct {
	url string
	genre string // the type of database for sqlx: postgres, mysql, sqlite
	db *sqlx.DB
	Data Dataset
	template Template
}

// Template is a database YAML template
type Template struct {
	Core map[string]string
	Metadata map[string]string
	Analysis map[string]string
	Function map[string]string
	GeneralTypeMap map[string]string
}

// Connect connects to a database using sqlx
func (conn *Connection) Connect() error {
	if conn.genre == "" {
		if strings.HasPrefix(conn.url, "postgresql://") { conn.genre = "postgres" }
	}
	if conn.genre != "" {
		conn.LoadYAML()
	}
	db, err := sqlx.Open(conn.genre, conn.url)
	conn.db = db
	if err != nil {
			log.Fatalln(err)
			return err
	}
	err = conn.db.Ping()
	println(R(`connected to {g}`, "g", conn.genre))
	return err
}


// Close closes the connection
func (conn *Connection) Close() error {
	return conn.db.Close()
}

// LoadYAML loads the approriate yaml template
func (conn *Connection) LoadYAML() error {
	_, filename, _, _ := runtime.Caller(1)
	box := packr.NewBox(path.Join(path.Dir(filename), "templates"))
	
	baseTemplateBytes, err := box.FindString("base.yaml")
	Check(err, "box.FindString")
  err = yaml.Unmarshal([]byte(baseTemplateBytes), &conn.template)
	Check(err, "yaml.Unmarshal")
	
	templateBytes, err := box.FindString(conn.genre+".yaml")
	Check(err, "box.FindString")
	template := Template{}
  err = yaml.Unmarshal([]byte(templateBytes), &template)
	Check(err, "yaml.Unmarshal")

	if len(template.Core) > 0 {
		conn.template.Core = template.Core
	}

	if len(template.Analysis) > 0 {
		conn.template.Analysis = template.Analysis
	}

	if len(template.Function) > 0 {
		conn.template.Function = template.Function
	}

	if len(template.Metadata) > 0 {
		conn.template.Metadata = template.Metadata
	}

	if len(template.GeneralTypeMap) > 0 {
		conn.template.GeneralTypeMap = template.GeneralTypeMap
	}

	return nil
}

// Query runs a sql query, returns `result`, `error`
func (conn *Connection) Query(sql string) (Dataset, error) {
	result, err := conn.db.Queryx(sql)
	Check(err, "conn.db.Queryx(sql)")

	fields, err := result.Columns()
	Check(err, "result.Columns()")
	
	conn.Data.Result = result
	conn.Data.Fields = fields
	conn.Data.Records = []map[string]interface{}{}
	conn.Data.Rows = [][]interface{}{}

	for result.Next() {
		// get records
		rec := map[string]interface{}{}
		err := result.MapScan(rec)
		Check(err, "MapScan(rec)")
		
		// add record
		conn.Data.Records = append(conn.Data.Records, rec)
		
		// add row
		row := []interface{}{}
		for _, field := range fields {
			row = append(row, rec[field])
		}
		conn.Data.Rows = append(conn.Data.Rows, row)

	}
	return conn.Data, err
}

func splitTableFullName (tableName string) (string, string) {
	var (
		schema string
		table string
	)

	a := strings.Split(tableName, ".") 
	if len(a) == 2 {
		schema = a[0]
		table = a[1]
	} else if len(a) == 1 {
		table = a[0]
	}
	return strings.ToLower(schema), strings.ToLower(table)
}

// GetSchemas returns schemas
func (conn *Connection) GetSchemas() (Dataset, error) {
	return conn.Query(conn.template.Metadata["get_schemas"])
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *Connection) GetObjects(schema string, objectType string) (Dataset, error) {
	sql := R(conn.template.Metadata["get_objects"], "schema", schema, "object_type", objectType)
	return conn.Query(sql)
}


// GetTables returns tables for given schema
func (conn *Connection) GetTables(schema string) (Dataset, error) {
	sql := R(conn.template.Metadata["get_tables"], "schema", schema)
	return conn.Query(sql)
}


// GetViews returns views for given schema
func (conn *Connection) GetViews(schema string) (Dataset, error) {
	sql := R(conn.template.Metadata["get_views"], "schema", schema)
	return conn.Query(sql)
}

// GetColumns returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *Connection) GetColumns(tableName string) (Dataset, error) {
	schema, table := splitTableFullName(tableName)

	sql := R(
		conn.template.Metadata["get_columns"],
		"schema", schema,
		"table", table,
	)
	return conn.Query(sql)
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *Connection) GetColumnsFull(tableName string) (Dataset, error) {
	schema, table := splitTableFullName(tableName)

	sql := R(
		conn.template.Metadata["get_columns_full"],
		"schema", schema,
		"table", table,
	)
	return conn.Query(sql)
}



// GetPrimarkKeys returns primark keys for given table.
func (conn *Connection) GetPrimarkKeys(tableName string) (Dataset, error) {
	sql := R(conn.template.Metadata["get_primary_keys"], "table", tableName)
	return conn.Query(sql)
}



// GetIndexes returns indexes for given table.
func (conn *Connection) GetIndexes(tableName string) (Dataset, error) {
	sql := R(conn.template.Metadata["get_indexes"], "table", tableName)
	return conn.Query(sql)
}


// GetDDL returns DDL for given table.
func (conn *Connection) GetDDL(tableName string) (Dataset, error) {
	sql := R(conn.template.Metadata["get_ddl"], "table", tableName)
	return conn.Query(sql)
}

// DropTable drops given table.
func (conn *Connection) DropTable(tableNames ...string) (Dataset, error) {

	var (
		result Dataset
		err error
	)

	for _, tableName := range tableNames {
		sql := R(conn.template.Core["drop_table"], "table", tableName)
		result, err = conn.Query(sql)
		Check(err, "Error for " + sql)
	}
	return result, err
}


// Import imports `data` into `tableName`
func (conn *Connection) Import (data Dataset, tableName string) error {

	return nil
}
