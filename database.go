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
	"gopkg.in/yaml.v2"
)

// Connection is a database connection
type Connection struct {
	URL      string
	Type     string // the type of database for sqlx: postgres, mysql, sqlite
	Db       *sqlx.DB
	Data     Dataset
	Template Template
	Schemata Schemata
}

type Column struct {
	Position int64  `json:"position"`
	Name     string `json:"name"`
	Type     string `json:"type"`
}

type Table struct {
	Name       string `json:"name"`
	IsView     bool   `json:"is_view"` // whether is a view
	Columns    []Column
	ColumnsMap map[string]*Column
}

type Schema struct {
	Name   string `json:"name"`
	Tables map[string]Table
}

// Schemata contains the full schema for a connection
type Schemata struct {
	Schemas map[string]Schema
	Tables  map[string]*Table // all tables with full name lower case (schema.table)
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
	conn.Schemata = Schemata{
		Schemas: map[string]Schema{},
		Tables:  map[string]*Table{},
	}

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
			case int:
				rec[i] = int64(val.(int))
			case int8:
				rec[i] = int64(val.(int8))
			case int16:
				rec[i] = int64(val.(int16))
			case int32:
				rec[i] = int64(val.(int32))
			case int64:
				rec[i] = val.(int64)
			case bool:
				rec[i] = val.(bool)
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
	// fields: [schema_name]
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
	// fields: [table_name]
	sql := R(conn.Template.Metadata["get_tables"], "schema", schema)
	return conn.Query(sql)
}

// GetViews returns views for given schema
func (conn *Connection) GetViews(schema string) (Dataset, error) {
	// fields: [table_name]
	sql := R(conn.Template.Metadata["get_views"], "schema", schema)
	return conn.Query(sql)
}

// GetColumns returns columns for given table. `tableFName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *Connection) GetColumns(tableFName string) (Dataset, error) {
	sql := getTemplateTableFName(conn, "get_columns", tableFName)
	return conn.Query(sql)
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *Connection) GetColumnsFull(tableFName string) (Dataset, error) {
	sql := getTemplateTableFName(conn, "get_columns_full", tableFName)
	return conn.Query(sql)
}

// GetPrimarkKeys returns primark keys for given table.
func (conn *Connection) GetPrimarkKeys(tableFName string) (Dataset, error) {
	sql := getTemplateTableFName(conn, "get_indexes", tableFName)
	return conn.Query(sql)
}

// GetIndexes returns indexes for given table.
func (conn *Connection) GetIndexes(tableFName string) (Dataset, error) {
	sql := getTemplateTableFName(conn, "get_indexes", tableFName)
	return conn.Query(sql)
}

// GetDDL returns DDL for given table.
func (conn *Connection) GetDDL(tableFName string) (Dataset, error) {
	sql := getTemplateTableFName(conn, "get_ddl", tableFName)
	return conn.Query(sql)
}

func getTemplateTableFName(conn *Connection, template string, tableFName string) string {
	schema, table := splitTableFullName(tableFName)
	sql := R(
		conn.Template.Metadata[template],
		"schema", schema,
		"table", table,
	)
	return sql
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

// GetSchemata obtain full schemata info
func (conn *Connection) GetSchemata(schemaName string) (Schema, error) {

	schema := Schema{
		Name:   "",
		Tables: map[string]Table{},
	}

	sql := R(conn.Template.Metadata["get_schemata"], "schema", schemaName)
	schemaData, err := conn.Query(sql)
	if err != nil {
		return schema, Error(err, "Could not GetSchemata for "+schemaName)
	}

	schema.Name = schemaName

	for _, rec := range schemaData.Records {
		tableName := rec["table_name"].(string)

		table := Table{
			Name:       tableName,
			IsView:     rec["is_view"].(bool),
			Columns:    []Column{},
			ColumnsMap: map[string]*Column{},
		}

		if _, ok := schema.Tables[tableName]; ok {
			table = schema.Tables[tableName]
		}

		column := Column{
			Position: rec["column_id"].(int64),
			Name:     rec["column_name"].(string),
			Type:     rec["data_type"].(string),
		}

		table.Columns = append(table.Columns, column)
		table.ColumnsMap[column.Name] = &column

		conn.Schemata.Tables[schemaName+"."+tableName] = &table
		schema.Tables[tableName] = table

	}

	conn.Schemata.Schemas[schemaName] = schema

	return schema, nil
}
