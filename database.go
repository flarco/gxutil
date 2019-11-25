package gxutil

import (
	"github.com/spf13/cast"
	"errors"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
	"database/sql"


	"github.com/gobuffalo/packr"
	"github.com/jinzhu/gorm"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
)

// Connection is the Base interface for Connections
type Connection interface {
	Connect() error
	Close() error
	GetGormConn() (*gorm.DB, error)
	LoadYAML() error
	StreamRows(sql string) (Datastream, error)
	Query(sql string) (Dataset, error)
	GenerateDDL(tableFName string, data Dataset) (string, error)
	GetDDL(string) (string, error)
	DropTable(...string) (error)
	InsertStream(tableFName string, ds Datastream) (count int64, err error)
	Db() *sqlx.DB
	Schemata() *Schemata
	Template() *Template

	StreamRecords(sql string) (<-chan map[string]interface{}, error)
	GetSchemata(string) (Schema, error)
	GetSchemas() (Dataset, error)
	GetTables(string) (Dataset, error)
	GetViews(string) (Dataset, error)
	GetColumns(string) (Dataset, error)
	GetPrimarkKeys(string) (Dataset, error)
	GetIndexes(string) (Dataset, error)
	GetColumnsFull(string) (Dataset, error)
	GetCount(string) (uint64, error)
	RunAnalysis(string, map[string]interface{}) (Dataset, error)
	RunAnalysisTable(string, ...string) (Dataset, error)
	RunAnalysisField(string, string, ...string) (Dataset, error)
}

// BaseConn is a database connection
type BaseConn struct {
	Connection
	URL      string
	Type     string // the type of database for sqlx: postgres, mysql, sqlite
	db       *sqlx.DB
	Data     Dataset
	template Template
	schemata Schemata
}

// Column represents a schemata column
type Column struct {
	Position int64  `json:"position"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	colType  *sql.ColumnType
}

// Table represents a schemata table
type Table struct {
	Name       string `json:"name"`
	FullName   string `json:"full_name"`
	IsView     bool   `json:"is_view"` // whether is a view
	Columns    []Column
	ColumnsMap map[string]*Column
}

// Schema represents a schemata schema
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
	GeneralTypeMap map[string]string `yaml:"general_type_map"`
	NativeTypeMap map[string]string `yaml:"native_type_map"`
}

// GetConn return the most proper connection for a given database
func GetConn(URL string) Connection {
	var conn Connection

	if strings.HasPrefix(URL, "postgresql:") {
		conn = &PostgresConn{URL: URL}
	} else if strings.HasPrefix(URL, "file:") {
		conn = &BaseConn{URL: URL, Type: "sqlite3"}
	} else {
		conn = &BaseConn{URL: URL}
	}

	return conn
}


// Db returns the sqlx db object
func (conn *BaseConn) Db() *sqlx.DB {
	return conn.db
}
// Schemata returns the Schemata object
func (conn *BaseConn) Schemata() *Schemata {
	return &conn.schemata
}

// Template returns the Template object
func (conn *BaseConn) Template() *Template {
	return &conn.template
}

// Connect connects to the database
func (conn *BaseConn) Connect() error {
	conn.schemata = Schemata{
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

	conn.db = db

	err = conn.db.Ping()
	if err != nil {
		return Error(err, "Could not ping DB")
	}

	LogCGreen(R(`connected to {g}`, "g", conn.Type))
	return nil
}

// Close closes the connection
func (conn *BaseConn) Close() error {
	return conn.db.Close()
}

// GetGormConn returns the gorm db connection
func (conn *BaseConn) GetGormConn() (*gorm.DB, error) {
	return gorm.Open(conn.Type, conn.URL)
}

// LoadYAML loads the approriate yaml template
func (conn *BaseConn) LoadYAML() error {
	conn.template = Template{
		Core:           map[string]string{},
		Metadata:       map[string]string{},
		Analysis:       map[string]string{},
		Function:       map[string]string{},
		GeneralTypeMap: map[string]string{},
		NativeTypeMap: map[string]string{},
	}

	_, filename, _, _ := runtime.Caller(1)
	box := packr.NewBox(path.Join(path.Dir(filename), "templates"))

	baseTemplateBytes, err := box.FindString("base.yaml")
	if err != nil {
		return Error(err, "box.FindString('base.yaml')")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &conn.template); err != nil {
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

	// jsonMap := make(map[string]interface{})
	// println(templateBytes)
	// err = json.Unmarshal([]byte(templateBytes), &jsonMap)
	// val, err := j.Search("general_type_map", jsonMap)
	// PrintV(val)

	for key, val := range template.Core {
		conn.template.Core[key] = val
	}

	for key, val := range template.Analysis {
		conn.template.Analysis[key] = val
	}

	for key, val := range template.Function {
		conn.template.Function[key] = val
	}

	for key, val := range template.Metadata {
		conn.template.Metadata[key] = val
	}

	for key, val := range template.GeneralTypeMap {
		conn.template.GeneralTypeMap[key] = val
	}

	for key, val := range template.NativeTypeMap {
		conn.template.NativeTypeMap[key] = val
	}

	return nil
}

func processVal(val interface{}) interface{} {

	var nVal interface{}
	switch v := val.(type) {
	case time.Time:
		nVal = cast.ToTime(val)
	case nil:
		nVal = val
	case int:
		nVal = cast.ToInt64(val)
	case int8:
		nVal = cast.ToInt64(val)
	case int16:
		nVal = cast.ToInt64(val)
	case int32:
		nVal = cast.ToInt64(val)
	case int64:
		nVal = cast.ToInt64(val)
	case float32:
		nVal = cast.ToFloat32(val)
	case float64:
		nVal = cast.ToFloat64(val)
	case bool:
		nVal = cast.ToBool(val)
	case []uint8:
		// arr := val.([]uint8)
		// buf := make([]byte, len(arr))
		// for j, n := range arr {
		// 	buf[j] = byte(n)
		// }
		f, err := strconv.ParseFloat(cast.ToString(val), 64)
		if err != nil {
			nVal = cast.ToString(val)
		} else {
			nVal = f
		}
	default:
		nVal = cast.ToString(val)
		_ = fmt.Sprint(v)
	}
	return nVal

}

func processRec(rec map[string]interface{}) map[string]interface{} {
	// Ensure usable types
	for i, val := range rec {
		rec[i] = processVal(val)
	}
	return rec
}

// StreamRecords the records of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRecords(sql string) (<-chan map[string]interface{}, error) {

	start := time.Now()

	if sql == "" {
		return nil, errors.New("Empty Query")
	}

	result, err := conn.db.Queryx(sql)
	if err != nil {
		return nil, Error(err, "SQL Error for:\n"+sql)
	}

	fields, err := result.Columns()
	if err != nil {
		return nil, Error(err, "result.Columns()")
	}

	conn.Data.Result = result
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.setFields(fields)

	chnl := make(chan map[string]interface{})
	go func() {
		for result.Next() {
			// get records
			rec := map[string]interface{}{}
			err := result.MapScan(rec)
			if err != nil {
				Check(err, "MapScan(rec)")
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
func (conn *BaseConn) StreamRows(sql string) (Datastream, error) {
	var ds Datastream
	start := time.Now()

	if strings.TrimSpace(sql) == "" {
		return ds, errors.New("Empty Query")
	}

	result, err := conn.db.Queryx(sql)
	if err != nil {
		return ds, Error(err, "SQL Error for:\n"+sql)
	}

	fields, err := result.Columns()
	if err != nil {
		return ds, Error(err, "result.Columns()")
	}

	colTypes, err := result.ColumnTypes()
	if err != nil {
		return ds, Error(err, "result.ColumnTypes()")
	}

	conn.Data.Result = result
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.setColumns(colTypes, conn.template.NativeTypeMap)


	ds = Datastream{
		Columns: conn.Data.Columns, 
		Rows: make(chan []interface{}),
	}
	
	go func() {
		for result.Next() {
			// get records
			rec := map[string]interface{}{}
			err := result.MapScan(rec)
			if err != nil {
				Check(err, "MapScan(rec)")
				break
			}

			rec = processRec(rec)

			// add row
			row := []interface{}{}
			for _, field := range fields {
				row = append(row, rec[field])
			}
			ds.Rows <- row

		}
		// Ensure that at the end of the loop we close the channel!
		close(ds.Rows)
	}()

	return ds, nil

}


// Query runs a sql query, returns `result`, `error`
func (conn *BaseConn) Query(sql string) (Dataset, error) {
	
	ds, err := conn.StreamRows(sql)
	if err != nil {
		return Dataset{}, err
	}

	data := Collect(&ds)
	data.Duration = conn.Data.Duration // Collect does not time duration

	return data, nil
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

// GetCount returns count of records
func (conn *BaseConn) GetCount(tableFName string) (uint64, error) {
	sql := F(`select count(*) cnt from %s`, tableFName)
	data, err := conn.Query(sql)
	if err != nil {
		return 0, err
	}
	return cast.ToUint64(data.Rows[0][0]), nil
}

// GetSchemas returns schemas
func (conn *BaseConn) GetSchemas() (Dataset, error) {
	// fields: [schema_name]
	return conn.Query(conn.template.Metadata["schemas"])
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *BaseConn) GetObjects(schema string, objectType string) (Dataset, error) {
	sql := R(conn.template.Metadata["objects"], "schema", schema, "object_type", objectType)
	return conn.Query(sql)
}

// GetTables returns tables for given schema
func (conn *BaseConn) GetTables(schema string) (Dataset, error) {
	// fields: [table_name]
	sql := R(conn.template.Metadata["tables"], "schema", schema)
	return conn.Query(sql)
}

// GetViews returns views for given schema
func (conn *BaseConn) GetViews(schema string) (Dataset, error) {
	// fields: [table_name]
	sql := R(conn.template.Metadata["views"], "schema", schema)
	return conn.Query(sql)
}

// GetColumns returns columns for given table. `tableFName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *BaseConn) GetColumns(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "columns", tableFName)
	return conn.Query(sql)
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *BaseConn) GetColumnsFull(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "columns_full", tableFName)
	return conn.Query(sql)
}

// GetPrimarkKeys returns primark keys for given table.
func (conn *BaseConn) GetPrimarkKeys(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "primary_keys", tableFName)
	return conn.Query(sql)
}

// GetIndexes returns indexes for given table.
func (conn *BaseConn) GetIndexes(tableFName string) (Dataset, error) {
	sql := getMetadataTableFName(conn, "indexes", tableFName)
	return conn.Query(sql)
}

// GetDDL returns DDL for given table.
func (conn *BaseConn) GetDDL(tableFName string) (string, error) {
	sql := getMetadataTableFName(conn, "ddl", tableFName)
	data, err := conn.Query(sql)
	if err != nil {
		return "", err
	}
	return data.Rows[0][0].(string), nil
}

func getMetadataTableFName(conn *BaseConn, template string, tableFName string) string {
	schema, table := splitTableFullName(tableFName)
	sql := R(
		conn.template.Metadata[template],
		"schema", schema,
		"table", table,
	)
	return sql
}

// DropTable drops given table.
func (conn *BaseConn) DropTable(tableNames ...string) (err error) {

	for _, tableName := range tableNames {
		sql := R(conn.template.Core["drop_table"], "table", tableName)
		_, err = conn.Query(sql)
		if err != nil {
			return Error(err, "Error for "+sql)
		}
	}
	return nil
}

// Import imports `data` into `tableName`
func (conn *BaseConn) Import(data Dataset, tableName string) error {

	return nil
}

// GetSchemata obtain full schemata info
func (conn *BaseConn) GetSchemata(schemaName string) (Schema, error) {

	schema := Schema{
		Name:   "",
		Tables: map[string]Table{},
	}

	sql := R(conn.template.Metadata["schemata"], "schema", schemaName)
	schemaData, err := conn.Query(sql)
	if err != nil {
		return schema, Error(err, "Could not GetSchemata for "+schemaName)
	}

	schema.Name = schemaName

	for _, rec := range schemaData.Records() {
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

		conn.schemata.Tables[schemaName+"."+tableName] = &table
		schema.Tables[tableName] = table

	}

	conn.schemata.Schemas[schemaName] = schema

	return schema, nil
}

// RunAnalysis runs an analysis
func (conn *BaseConn) RunAnalysis(analysisName string, values map[string]interface{}) (Dataset, error) {
	sql := Rm(
		conn.template.Analysis[analysisName],
		values,
	)
	return conn.Query(sql)
}

// RunAnalysisTable runs a table level analysis
func (conn *BaseConn) RunAnalysisTable(analysisName string, tableFNames ...string) (Dataset, error) {

	if len(tableFNames) == 0 {
		return Dataset{}, errors.New("Need to provied tables for RunAnalysisTable")
	}

	sqls := []string{}

	for _, tableFName := range tableFNames {
		schema, table := splitTableFullName(tableFName)
		sql := R(
			conn.template.Analysis[analysisName],
			"schema", schema,
			"table", table,
		)
		sqls = append(sqls, sql)
	}

	sql := strings.Join(sqls, "\nUNION ALL\n")
	return conn.Query(sql)
}

// RunAnalysisField runs a field level analysis
func (conn *BaseConn) RunAnalysisField(analysisName string, tableFName string, fields ...string) (Dataset, error) {
	schema, table := splitTableFullName(tableFName)

	sqls := []string{}

	if len(fields) == 0 {
		// get fields
		result, err := conn.GetColumns(tableFName)
		if err != nil {
			return Dataset{}, err
		}

		for _, rec := range result.Records() {
			fields = append(fields, rec["column_name"].(string))
		}
	}

	for _, field := range fields {
		sql := R(
			conn.template.Analysis[analysisName],
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
func (conn *BaseConn) InsertStreamBatch(tableFName string, columns []string, streamRow <-chan []interface{}) error {
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

	tx := conn.db.MustBegin()
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
func (conn *BaseConn) InsertStream(tableFName string, ds Datastream) (count int64, err error) {

	fields := ds.GetFields()
	values := make([]string, len(fields))
	for i := 0; i < len(fields); i++ {
		values[i] = F("$%d", i+1)
	}

	insertTemplate := R(
		"INSERT INTO {table} ({columns}) VALUES ({values})",
		"table", tableFName,
		"columns", strings.Join(fields, ", "),
		"values", strings.Join(values, ", "),
	)

	tx := conn.db.MustBegin()
	for row := range ds.Rows {
		count++
		// Do insert
		_, err := tx.Exec(insertTemplate, row...)
		if err != nil {
			tx.Rollback()
			return count, err
		}
	}
	tx.Commit()

	return count, nil
}

// GenerateDDL genrate a DDL based on a dataset
func (conn *BaseConn) GenerateDDL(tableFName string, data Dataset) (string, error) {

	data.InferColumnTypes()
	columnsDDL := []string{}

	for _, col := range data.Columns {
		// convert from general type to native type
		if _, ok := conn.template.GeneralTypeMap[col.Type]; ok {
			columnDDL := F(
				"%s %s", 
				col.Name,
				conn.template.GeneralTypeMap[col.Type],
			)
			columnsDDL = append(columnsDDL, columnDDL)
		} else {
			return "", errors.New(
				F(
					"No type mapping defined for '%s' for '%s'",
					col.Type,
					conn.Type,
				),
			)
		}
	}

	ddl := R(
		conn.template.Core["create_table"],
		"table", tableFName,
		"col_types", strings.Join(columnsDDL, ",\n"),
	)

	return ddl, nil
}
