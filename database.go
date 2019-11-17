package gxutil

import (
	"errors"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gobuffalo/packr"
	"github.com/jinzhu/gorm"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
)

// Connection is a database connection
type Connection struct {
	URL      string
	Type     string // the type of database for sqlx: postgres, mysql, sqlite
	Db       *sqlx.DB
	db       gorm.DB
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

// GetGormConn returns the gorm db connection
func (conn *Connection) GetGormConn() (*gorm.DB, error) {
	return gorm.Open(conn.Type, conn.URL)
}

// LoadYAML loads the approriate yaml template
func (conn *Connection) LoadYAML() error {
	conn.Template = Template{
		Core:           map[string]string{},
		Metadata:       map[string]string{},
		Analysis:       map[string]string{},
		Function:       map[string]string{},
		GeneralTypeMap: map[string]string{},
	}

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

	for key, val := range template.Core {
		conn.Template.Core[key] = val
	}

	for key, val := range template.Analysis {
		conn.Template.Analysis[key] = val
	}

	for key, val := range template.Function {
		conn.Template.Function[key] = val
	}

	for key, val := range template.Metadata {
		conn.Template.Metadata[key] = val
	}

	for key, val := range template.GeneralTypeMap {
		conn.Template.GeneralTypeMap[key] = val
	}

	return nil
}

func processRec(rec map[string]interface{}) map[string]interface{} {

	// Ensure usable types
	for i, val := range rec {

		switch v := val.(type) {
		case time.Time:
			rec[i] = val.(time.Time)
		case nil:
			rec[i] = val
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
		case float32:
			rec[i] = val.(float32)
		case float64:
			rec[i] = val.(float64)
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

	return rec
}

// StreamRecords the records of a sql query, returns `result`, `error`
func (conn *Connection) StreamRecords(sql string) (<-chan map[string]interface{}, error) {

	start := time.Now()

	if sql == "" {
		return nil, errors.New("Empty Query")
	}

	result, err := conn.Db.Queryx(sql)
	if err != nil {
		return nil, Error(err, "SQL Error for:\n"+sql)
	}

	fields, err := result.Columns()
	if err != nil {
		return nil, Error(err, "result.Columns()")
	}

	conn.Data.Result = result
	conn.Data.Fields = fields
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Records = []map[string]interface{}{}
	conn.Data.Rows = [][]interface{}{}

	chnl := make(chan map[string]interface{})
	go func() {
		for result.Next() {
			// get records
			rec := map[string]interface{}{}
			err := result.MapScan(rec)
			if err != nil {
				// return nil, Error(err, "MapScan(rec)")
				close(chnl)
			}

			rec = processRec(rec)
			chnl <- rec

		}
		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()

	return chnl, nil
}

// StreamRows the rows of a sql query, returns `result`, `error`
func (conn *Connection) StreamRows(sql string) (<-chan []interface{}, error) {
	start := time.Now()

	if sql == "" {
		return nil, errors.New("Empty Query")
	}

	result, err := conn.Db.Queryx(sql)
	if err != nil {
		return nil, Error(err, "SQL Error for:\n"+sql)
	}

	fields, err := result.Columns()
	if err != nil {
		return nil, Error(err, "result.Columns()")
	}

	conn.Data.Result = result
	conn.Data.Fields = fields
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Records = []map[string]interface{}{}
	conn.Data.Rows = [][]interface{}{}

	chnl := make(chan []interface{})
	go func() {
		for result.Next() {
			// get records
			rec := map[string]interface{}{}
			err := result.MapScan(rec)
			if err != nil {
				// return nil, Error(err, "MapScan(rec)")
				close(chnl)
			}

			rec = processRec(rec)

			// add row
			row := []interface{}{}
			for _, field := range fields {
				row = append(row, rec[field])
			}
			chnl <- row

		}
		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()

	return chnl, nil

}

// Query runs a sql query, returns `result`, `error`
func (conn *Connection) Query(sql string) (Dataset, error) {
	start := time.Now()

	if sql == "" {
		return Dataset{}, errors.New("Empty Query")
	}

	result, err := conn.Db.Queryx(sql)
	if err != nil {
		return conn.Data, Error(err, "SQL Error for:\n"+sql)
	}

	fields, err := result.Columns()
	if err != nil {
		return conn.Data, Error(err, "result.Columns()")
	}

	conn.Data.Result = result
	conn.Data.Fields = fields
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Records = []map[string]interface{}{}
	conn.Data.Rows = [][]interface{}{}

	for result.Next() {
		// get records
		rec := map[string]interface{}{}
		err := result.MapScan(rec)
		if err != nil {
			return conn.Data, Error(err, "MapScan(rec)")
		}

		rec = processRec(rec)

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
		schema = ""
		table = a[0]
	}
	return strings.ToLower(schema), strings.ToLower(table)
}

// GetSchemas returns schemas
func (conn *Connection) GetSchemas() (Dataset, error) {
	// fields: [schema_name]
	return conn.Query(conn.Template.Metadata["schemas"])
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *Connection) GetObjects(schema string, objectType string) (Dataset, error) {
	sql := R(conn.Template.Metadata["objects"], "schema", schema, "object_type", objectType)
	return conn.Query(sql)
}

// GetTables returns tables for given schema
func (conn *Connection) GetTables(schema string) (Dataset, error) {
	// fields: [table_name]
	sql := R(conn.Template.Metadata["tables"], "schema", schema)
	return conn.Query(sql)
}

// GetViews returns views for given schema
func (conn *Connection) GetViews(schema string) (Dataset, error) {
	// fields: [table_name]
	sql := R(conn.Template.Metadata["views"], "schema", schema)
	return conn.Query(sql)
}

// GetColumns returns columns for given table. `tableFName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *Connection) GetColumns(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "columns", tableFName)
	return conn.Query(sql)
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *Connection) GetColumnsFull(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "columns_full", tableFName)
	return conn.Query(sql)
}

// GetPrimarkKeys returns primark keys for given table.
func (conn *Connection) GetPrimarkKeys(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "primary_keys", tableFName)
	return conn.Query(sql)
}

// GetIndexes returns indexes for given table.
func (conn *Connection) GetIndexes(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "indexes", tableFName)
	return conn.Query(sql)
}

// GetDDL returns DDL for given table.
func (conn *Connection) GetDDL(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "ddl", tableFName)
	return conn.Query(sql)
}

func getMetadataTableFName(conn *Connection, template string, tableFName string) string {
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

	sql := R(conn.Template.Metadata["schemata"], "schema", schemaName)
	schemaData, err := conn.Query(sql)
	if err != nil {
		return schema, Error(err, "Could not GetSchemata for "+schemaName)
	}

	schema.Name = schemaName

	for _, rec := range schemaData.Records {
		tableName := rec["table_name"].(string)

		switch v := rec["is_view"].(type) {
		case int64:
			if rec["is_view"].(int64) == 0 {
				rec["is_view"] = false
			} else {
				rec["is_view"] = true
			}
		default:
			_ = fmt.Sprint(v)
			_ = rec["is_view"]
		}

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
			Position: rec["position"].(int64),
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

// RunAnalysis runs an analysis
func (conn *Connection) RunAnalysis(analysisName string, values map[string]interface{}) (Dataset, error) {
	sql := Rm(
		conn.Template.Analysis[analysisName],
		values,
	)
	return conn.Query(sql)
}

// RunAnalysisTable runs a table level analysis
func (conn *Connection) RunAnalysisTable(analysisName string, tableFNames ...string) (Dataset, error) {

	if len(tableFNames) == 0 {
		return Dataset{}, errors.New("Need to provied tables for RunAnalysisTable")
	}

	sqls := []string{}

	for _, tableFName := range tableFNames {
		schema, table := splitTableFullName(tableFName)
		sql := R(
			conn.Template.Analysis[analysisName],
			"schema", schema,
			"table", table,
		)
		sqls = append(sqls, sql)
	}

	sql := strings.Join(sqls, "\nUNION ALL\n")
	return conn.Query(sql)
}

// RunAnalysisField runs a field level analysis
func (conn *Connection) RunAnalysisField(analysisName string, tableFName string, fields ...string) (Dataset, error) {
	schema, table := splitTableFullName(tableFName)

	sqls := []string{}

	if len(fields) == 0 {
		// get fields
		result, err := conn.GetColumns(tableFName)
		if err != nil {
			return Dataset{}, err
		}

		for _, rec := range result.Records {
			fields = append(fields, rec["column_name"].(string))
		}
	}

	for _, field := range fields {
		sql := R(
			conn.Template.Analysis[analysisName],
			"schema", schema,
			"table", table,
			"field", field,
		)
		sqls = append(sqls, sql)
	}

	sql := strings.Join(sqls, "\nUNION ALL\n")
	return conn.Query(sql)
}

// InsertStreamBatch inserts a stream into a table in batch
func (conn *Connection) InsertStreamBatch(tableFName string, columns []string, streamRow <-chan []interface{}) error {
	batchSize := 5000

	// replaceSQL replaces the instance occurrence of any string pattern with an increasing $n based sequence
	replaceSQL := func(old, searchPattern string) string {
		tmpCount := strings.Count(old, searchPattern)
		for m := 1; m <= tmpCount; m++ {
			old = strings.Replace(old, searchPattern, "$"+strconv.Itoa(m), 1)
		}
		return old
	}

	values := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i := 0; i < len(columns); i++ {
		values[i] = F("$%d", i+1)
		placeholders[i] = "?"
	}

	insertTemplate := R(
		"INSERT INTO {table} ({columns}) VALUES ",
		"table", tableFName,
		"columns", strings.Join(columns, ", "),
		"values", strings.Join(values, ", "),
	)

	tx := conn.Db.MustBegin()
	// rows := [][]interface{}{}
	placeholderRows := []string{}
	rowCounter := 0
	vals := []interface{}{}
	for row := range streamRow {
		rowCounter++
		// rows = append(rows, row)
		placeholderRows = append(
			placeholderRows,
			"("+strings.Join(placeholders, ", ")+")",
		)

		vals = append(vals, row...)
		if rowCounter%batchSize == 0 {
			// Insert batch
			placeholderSQL := strings.Join(placeholderRows, ", ")
			insertSQL := replaceSQL(insertTemplate+placeholderSQL, "?")
			// println(insertSQL)
			stmt, _ := tx.Prepare(insertSQL)
			_, err := stmt.Exec(vals...)
			if err != nil {
				tx.Rollback()
				return err
			}
			placeholderRows = []string{}
			vals = []interface{}{}
		}
	}

	// Insert remaining
	placeholderSQL := strings.Join(placeholderRows, ", ")
	insertSQL := replaceSQL(insertTemplate+placeholderSQL, "?")
	stmt, _ := tx.Prepare(insertSQL)
	_, err := stmt.Exec(vals...)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()

	return nil
}

// InsertStream inserts a stream into a table
func (conn *Connection) InsertStream(tableFName string, columns []string, streamRow <-chan []interface{}) error {

	values := make([]string, len(columns))
	for i := 0; i < len(columns); i++ {
		values[i] = F("$%d", i+1)
	}

	insertTemplate := R(
		"INSERT INTO {table} ({columns}) VALUES ({values})",
		"table", tableFName,
		"columns", strings.Join(columns, ", "),
		"values", strings.Join(values, ", "),
	)

	tx := conn.Db.MustBegin()
	for row := range streamRow {
		// Do insert
		_, err := tx.Exec(insertTemplate, row...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()

	return nil
}
