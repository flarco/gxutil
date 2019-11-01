package gxutil

import (
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gobuffalo/packr"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

// Connection is a database connection
type Connection struct {
	URL      string
	Type     string // the type of database for sqlx: postgres, mysql, sqlite
	Db       *sqlx.DB
	Data     Dataset
	Template Template
}

// Template is a database YAML template
type Template struct {
	Core           map[string]string
	Metadata       map[string]string
	Analysis       map[string]string
	Function       map[string]string
	GeneralTypeMap map[string]string
}

// Connect connects to a database using sqlx
func (conn *Connection) Connect() error {
	if conn.Type == "" {
		if strings.HasPrefix(conn.URL, "postgresql://") {
			conn.Type = "postgres"
		}
	}
	if conn.Type != "" {
		conn.LoadYAML()
	}
	db, err := sqlx.Open(conn.Type, conn.URL)
	if err != nil {
		return Error(err, "Could not connect to DB")
	}

	conn.Db = db

	err = conn.Db.Ping()
	if err != nil {
		return Error(err, "Could not ping DB")
	}

	println(R(`connected to {g}`, "g", conn.Type))
	return nil
}

// Close closes the connection
func (conn *Connection) Close() error {
	return conn.Db.Close()
}

// LoadYAML loads the approriate yaml template
func (conn *Connection) LoadYAML() error {
	_, filename, _, _ := runtime.Caller(1)
	box := packr.NewBox(path.Join(path.Dir(filename), "templates"))

	baseTemplateBytes, err := box.FindString("base.yaml")
	if err != nil {
		return Error(err, "box.FindString('base.yaml')")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &conn.Template); err != nil {
		return Error(err, "yaml.Unmarshal")
	}

	templateBytes, err := box.FindString(conn.Type + ".yaml")
	if err != nil {
		return Error(err, "box.FindString('.yaml') for "+conn.Type)
	}

	template := Template{}
	err = yaml.Unmarshal([]byte(templateBytes), &template)
	if err != nil {
		return Error(err, "yaml.Unmarshal")
	}

	if len(template.Core) > 0 {
		conn.Template.Core = template.Core
	}

	if len(template.Analysis) > 0 {
		conn.Template.Analysis = template.Analysis
	}

	if len(template.Function) > 0 {
		conn.Template.Function = template.Function
	}

	if len(template.Metadata) > 0 {
		conn.Template.Metadata = template.Metadata
	}

	if len(template.GeneralTypeMap) > 0 {
		conn.Template.GeneralTypeMap = template.GeneralTypeMap
	}

	return nil
}

// Query runs a sql query, returns `result`, `error`
func (conn *Connection) Query(sql string) (Dataset, error) {
	result, err := conn.Db.Queryx(sql)
	if err != nil {
		return conn.Data, Error(err, "conn.Db.Queryx(sql)")
	}

	fields, err := result.Columns()
	if err != nil {
		return conn.Data, Error(err, "result.Columns()")
	}

	conn.Data.Result = result
	conn.Data.Fields = fields
	conn.Data.Records = []map[string]interface{}{}
	conn.Data.Rows = [][]interface{}{}

	for result.Next() {
		// get records
		rec := map[string]interface{}{}
		err := result.MapScan(rec)
		if err != nil {
			return conn.Data, Error(err, "MapScan(rec)")
		}

		// Ensure usable types
		for i, val := range rec {

			switch v := val.(type) {
			case time.Time:
				rec[i] = val.(time.Time)
			case nil:
				rec[i] = nil
			case int64:
				rec[i] = val.(int64)
			case []uint8:
				arr := val.([]uint8)
				buf := make([]byte, len(arr))
				for j, n := range arr {
					buf[j] = byte(n)
				}
				f, err := strconv.ParseFloat(string(buf), 64)
				if err != nil {
					rec[i] = string(buf)
				} else {
					rec[i] = f
				}
			default:
				rec[i] = val.(string)
				_ = fmt.Sprint(v)
			}
		}

		// add record
		conn.Data.Records = append(conn.Data.Records, rec)

		// add row
		row := []interface{}{}
		for _, field := range fields {
			row = append(row, rec[field])
		}
		conn.Data.Rows = append(conn.Data.Rows, row)

	}
	return conn.Data, nil
}

func splitTableFullName(tableName string) (string, string) {
	var (
		schema string
		table  string
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
	return conn.Query(conn.Template.Metadata["get_schemas"])
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *Connection) GetObjects(schema string, objectType string) (Dataset, error) {
	sql := R(conn.Template.Metadata["get_objects"], "schema", schema, "object_type", objectType)
	return conn.Query(sql)
}

// GetTables returns tables for given schema
func (conn *Connection) GetTables(schema string) (Dataset, error) {
	sql := R(conn.Template.Metadata["get_tables"], "schema", schema)
	return conn.Query(sql)
}

// GetViews returns views for given schema
func (conn *Connection) GetViews(schema string) (Dataset, error) {
	sql := R(conn.Template.Metadata["get_views"], "schema", schema)
	return conn.Query(sql)
}

// GetColumns returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *Connection) GetColumns(tableName string) (Dataset, error) {
	schema, table := splitTableFullName(tableName)

	sql := R(
		conn.Template.Metadata["get_columns"],
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
		conn.Template.Metadata["get_columns_full"],
		"schema", schema,
		"table", table,
	)
	return conn.Query(sql)
}

// GetPrimarkKeys returns primark keys for given table.
func (conn *Connection) GetPrimarkKeys(tableName string) (Dataset, error) {
	sql := R(conn.Template.Metadata["get_primary_keys"], "table", tableName)
	return conn.Query(sql)
}

// GetIndexes returns indexes for given table.
func (conn *Connection) GetIndexes(tableName string) (Dataset, error) {
	sql := R(conn.Template.Metadata["get_indexes"], "table", tableName)
	return conn.Query(sql)
}

// GetDDL returns DDL for given table.
func (conn *Connection) GetDDL(tableName string) (Dataset, error) {
	sql := R(conn.Template.Metadata["get_ddl"], "table", tableName)
	return conn.Query(sql)
}

// DropTable drops given table.
func (conn *Connection) DropTable(tableNames ...string) (Dataset, error) {

	var (
		result Dataset
		err    error
	)

	for _, tableName := range tableNames {
		sql := R(conn.Template.Core["drop_table"], "table", tableName)
		result, err = conn.Query(sql)
		if err != nil {
			return result, Error(err, "Error for "+sql)
		}
	}
	return result, nil
}

// Import imports `data` into `tableName`
func (conn *Connection) Import(data Dataset, tableName string) error {

	return nil
}
