package gxutil

import (
	"database/sql"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/godror/godror"
	"github.com/jinzhu/gorm"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/markbates/pkger"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

// Connection is the Base interface for Connections
type Connection interface {
	Init() error
	Connect() error
	Kill() error
	Close() error
	GetType() string
	GetGormConn() (*gorm.DB, error)
	LoadYAML() error
	StreamRows(sql string) (Datastream, error)
	BulkExportStream(sql string) (Datastream, error)
	BulkImportStream(tableFName string, ds Datastream) (count uint64, err error)
	Query(sql string) (Dataset, error)
	QueryContext(ctx context.Context, sql string) (Dataset, error)
	GenerateDDL(tableFName string, data Dataset) (string, error)
	GenerateInsertStatement(tableName string, fields []string) string
	DropTable(...string) error
	DropView(...string) error
	InsertStream(tableFName string, ds Datastream) (count uint64, err error)
	Db() *sqlx.DB
	Schemata() *Schemata
	Template() *Template
	SetProp(string, string)
	GetProp(string) string
	GetTemplateValue(path string) (value string)
	Context() Context

	bindVar(i int, field string) string
	StreamRecords(sql string) (<-chan map[string]interface{}, error)
	GetDDL(string) (string, error)
	GetSchemata(string) (Schema, error)
	GetSchemas() (Dataset, error)
	GetTables(string) (Dataset, error)
	GetViews(string) (Dataset, error)
	GetColumns(string) (Dataset, error)
	GetPrimaryKeys(string) (Dataset, error)
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
	URL        string
	Type       string // the type of database for sqlx: postgres, mysql, sqlite
	db         *sqlx.DB
	Data       Dataset
	context    Context
	template   Template
	schemata   Schemata
	properties map[string]string
}

// Column represents a schemata column
type Column struct {
	Position int64  `json:"position"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	stats    ColumnStats
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
	NativeTypeMap  map[string]string `yaml:"native_type_map"`
	Variable       map[string]string
}

// GetConn return the most proper connection for a given database
func GetConn(URL string) Connection {
	var conn Connection

	if strings.HasPrefix(URL, "postgres") {
		if isRedshift(URL) {
			conn = &RedshiftConn{URL: URL}
		} else {
			conn = &PostgresConn{URL: URL}
		}
	} else if strings.HasPrefix(URL, "mysql:") {
		conn = &MySQLConn{URL: URL}
	} else if strings.HasPrefix(URL, "sqlserver:") {
		conn = &BaseConn{URL: URL, Type: "sqlserver"}
	} else if strings.HasPrefix(URL, "oracle:") {
		conn = &OracleConn{URL: URL}
	} else if strings.HasPrefix(URL, "file:") {
		conn = &BaseConn{URL: URL, Type: "sqlite3"}
	} else {
		conn = &BaseConn{URL: URL}
	}

	// Init
	conn.Init()

	return conn
}

func getDriverName(name string) (driverName string) {
	driverName = name
	if driverName == "redshift" {
		driverName = "postgres"
	}
	if driverName == "oracle" {
		driverName = "godror"
	}
	if driverName == "sqlserver" {
		driverName = "mssql"
	}
	return
}

// Init initiates the connection object
func (conn *BaseConn) Init() (err error) {
	conn.LoadYAML()
	connCtx, cancel := context.WithCancel(context.Background())
	conn.context.ctx = connCtx
	conn.context.cancel = cancel
	conn.SetProp("connected", "false")
	return nil
}

// Db returns the sqlx db object
func (conn *BaseConn) Db() *sqlx.DB {
	return conn.db
}

// GetType returns the type db object
func (conn *BaseConn) GetType() string {
	return conn.Type
}

// Context returns the db context
func (conn *BaseConn) Context() Context {
	return conn.context
}

// Schemata returns the Schemata object
func (conn *BaseConn) Schemata() *Schemata {
	return &conn.schemata
}

// Template returns the Template object
func (conn *BaseConn) Template() *Template {
	return &conn.template
}

// GetProp returns the value of a property
func (conn *BaseConn) GetProp(key string) string {
	return conn.properties[key]
}

// SetProp sets the value of a property
func (conn *BaseConn) SetProp(key string, val string) {
	if conn.properties == nil {
		conn.properties = map[string]string{}
	}
	conn.properties[key] = val
}

// Kill kill the database connection
func (conn *BaseConn) Kill() error {
	conn.context.cancel()
	conn.SetProp("connected", "false")
	return nil
}

// Connect connects to the database
func (conn *BaseConn) Connect() error {
	conn.schemata = Schemata{
		Schemas: map[string]Schema{},
		Tables:  map[string]*Table{},
	}

	if conn.Type == "" {
		return errors.New("conn.Type needs to be specified")
	}

	conn.LoadYAML()

	db, err := sqlx.Open(getDriverName(conn.Type), conn.URL)
	if err != nil {
		return Error(err, "Could not connect to DB: " + getDriverName(conn.Type))
	}

	conn.db = db
	conn.properties = map[string]string{}

	err = conn.db.Ping()
	if err != nil {
		return Error(err, "Could not ping DB")
	}

	conn.SetProp("connected", "true")

	LogCGreen(R(`connected to {g}`, "g", conn.Type))
	return nil
}

// Close closes the connection
func (conn *BaseConn) Close() error {
	return conn.db.Close()
}

// GetGormConn returns the gorm db connection
func (conn *BaseConn) GetGormConn() (*gorm.DB, error) {
	return gorm.Open(getDriverName(conn.Type), conn.URL)
}

// GetTemplateValue returns the value of the path
func (conn *BaseConn) GetTemplateValue(path string) (value string) {

	prefixes := map[string]map[string]string{
		"core.":             conn.template.Core,
		"analysis.":         conn.template.Analysis,
		"function.":         conn.template.Function,
		"metadata.":         conn.template.Metadata,
		"general_type_map.": conn.template.GeneralTypeMap,
		"native_type_map.":  conn.template.NativeTypeMap,
		"variable.":         conn.template.Variable,
	}

	for prefix, dict := range prefixes {
		if strings.HasPrefix(path, prefix) {
			key := strings.Replace(path, prefix, "", 1)
			value = dict[key]
			break
		}
	}

	return value
}

// LoadYAML loads the approriate yaml template
func (conn *BaseConn) LoadYAML() error {
	conn.template = Template{
		Core:           map[string]string{},
		Metadata:       map[string]string{},
		Analysis:       map[string]string{},
		Function:       map[string]string{},
		GeneralTypeMap: map[string]string{},
		NativeTypeMap:  map[string]string{},
		Variable:       map[string]string{},
	}

	_, filename, _, _ := runtime.Caller(1)
	pkgerRead := func(name string) (TemplateBytes []byte, err error) {
		TemplateFile, err := pkger.Open(path.Join(path.Dir(filename), "templates", name))
		if err != nil {
			return nil, Error(err, "pkger.Open()"+path.Join(path.Dir(filename), "templates", name))
		}
		TemplateBytes, err = ioutil.ReadAll(TemplateFile)
		return TemplateBytes, err
	}

	baseTemplateBytes, err := pkgerRead("base.yaml")
	if err != nil {
		return Error(err, "box.FindString('base.yaml')")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &conn.template); err != nil {
		return Error(err, "yaml.Unmarshal")
	}

	templateBytes, err := pkgerRead(conn.Type + ".yaml")
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

	for key, val := range template.Variable {
		conn.template.Variable[key] = val
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
		nVal = cast.ToString(val)
	case godror.Number:
		nVal = ParseString(cast.ToString(val))
	case bool:
		nVal = cast.ToBool(val)
	case []uint8:
		nVal = ParseString(cast.ToString(val))
	default:
		nVal = ParseString(cast.ToString(val))
		_ = fmt.Sprint(v)
		// fmt.Printf("%T\n", val)
	}
	return nVal

}

func processRow(row []interface{}) []interface{} {
	// Ensure usable types
	for i, val := range row {
		row[i] = processVal(val)
	}
	return row
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
				LogError(err)
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

// BulkExportStream streams the rows in bulk
func (conn *BaseConn) BulkExportStream(sql string) (ds Datastream, err error) {
	Log("BulkExportStream not implemented for " + conn.Type)
	return conn.StreamRows(sql)
}

// BulkImportStream import the stream rows in bulk
func (conn *BaseConn) BulkImportStream(tableFName string, ds Datastream) (count uint64, err error) {
	Log("BulkImportStream not implemented for " + conn.Type)
	return conn.InsertStream(tableFName, ds)
}

// StreamRows the rows of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRows(sql string) (ds Datastream, err error) {
	return conn.StreamRowsContext(conn.context.ctx, sql)
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BaseConn) StreamRowsContext(ctx context.Context, sql string) (ds Datastream, err error) {
	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, errors.New("Empty Query")
	}

	queryCtx, queryCancel := context.WithCancel(ctx)
	result, err := conn.db.QueryxContext(queryCtx, sql)
	if err != nil {
		queryCancel()
		return ds, Error(err, "SQL Error for:\n"+sql)
	}

	colTypes, err := result.ColumnTypes()
	if err != nil {
		queryCancel()
		return ds, Error(err, "result.ColumnTypes()")
	}

	conn.Data.Result = result
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.setColumns(colTypes, conn.template.NativeTypeMap)

	ds = Datastream{
		Columns: conn.Data.Columns,
		Rows:    make(chan []interface{}),
		context: Context{queryCtx, queryCancel},
	}

	go func() {
		for result.Next() {
			// add row
			row, err := result.SliceScan()
			if err != nil {
				LogError(err)
				break
			}
			row = processRow(row)

			select {
			case <-ds.context.ctx.Done():
				close(ds.Rows)
				break
			default:
				ds.Rows <- row
			}

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
		return Dataset{SQL:sql}, err
	}

	data := ds.Collect()
	data.SQL = sql
	data.Duration = conn.Data.Duration // Collect does not time duration

	return data, nil
}

// QueryContext runs a sql query with ctx, returns `result`, `error`
func (conn *BaseConn) QueryContext(ctx context.Context, sql string) (Dataset, error) {

	ds, err := conn.StreamRowsContext(ctx, sql)
	if err != nil {
		return Dataset{SQL:sql}, err
	}

	data := ds.Collect()
	data.SQL = sql
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

// GetPrimaryKeys returns primark keys for given table.
func (conn *BaseConn) GetPrimaryKeys(tableFName string) (Dataset, error) {
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
	schema, table := splitTableFullName(tableFName)
	ddlCol := cast.ToInt(conn.template.Variable["ddl_col"])
	sqlTable := R(
		conn.template.Metadata["ddl_table"],
		"schema", schema,
		"table", table,
	)
	sqlView := R(
		conn.template.Metadata["ddl_view"],
		"schema", schema,
		"table", table,
	)

	data, err := conn.Query(sqlView)
	if err != nil {
		return "", err
	}

	if len(data.Rows) == 0 || data.Rows[0][ddlCol] == nil {
		data, err = conn.Query(sqlTable)
		if err != nil {
			return "", err
		}
	}

	if len(data.Rows) == 0 {
		return "", nil
	}

	return cast.ToString(data.Rows[0][ddlCol]), nil
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
			errIgnoreWord := conn.template.Variable["error_ignore_drop_table"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return Error(err, "Error for "+sql)
			} else {
				log.Debug(F("table %s does not exist", tableName))
			}
		} else {
			log.Debug(F("table %s dropped", tableName))
		}
	}
	return nil
}

// DropView drops given view.
func (conn *BaseConn) DropView(viewNames ...string) (err error) {

	for _, viewName := range viewNames {
		sql := R(conn.template.Core["drop_view"], "view", viewName)
		_, err = conn.Query(sql)
		if err != nil {
			errIgnoreWord := conn.template.Variable["error_ignore_drop_view"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return Error(err, "Error for "+sql)
			} else {
				log.Debug(F("view %s does not exist", viewName))
			}
		} else {
			log.Debug(F("view %s dropped", viewName))
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
		tableName := strings.ToLower(cast.ToString(rec["table_name"]))

		switch v := rec["is_view"].(type) {
		case int64, float64:
			if cast.ToInt64(rec["is_view"]) == 0 {
				rec["is_view"] = false
			} else {
				rec["is_view"] = true
			}
		case string:
			if strings.ToLower(cast.ToString(rec["is_view"])) == "true"  {
				rec["is_view"] = true
			} else {
				rec["is_view"] = false
			}

		default:
			_ = fmt.Sprint(v)
			_ = rec["is_view"]
		}

		table := Table{
			Name:       tableName,
			IsView:     cast.ToBool(rec["is_view"]),
			Columns:    []Column{},
			ColumnsMap: map[string]*Column{},
		}

		if _, ok := schema.Tables[tableName]; ok {
			table = schema.Tables[tableName]
		}

		column := Column{
			Position: cast.ToInt64(rec["position"]),
			Name:     strings.ToLower(cast.ToString(rec["column_name"])),
			Type:     cast.ToString(rec["data_type"]),
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
			conn.GetTemplateValue("analysis."+analysisName),
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
			fields = append(fields, cast.ToString(rec["column_name"]))
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

// InsertBatchStream inserts a stream into a table in batch
func (conn *BaseConn) InsertBatchStream(tableFName string, columns []string, streamRow <-chan []interface{}) error {
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
	for i, field := range columns {
		values[i] = conn.bindVar(i+1, field)
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

// bindVar return proper bind var according to https://jmoiron.github.io/sqlx/#bindvars
func (conn *BaseConn) bindVar(i int, field string) string {
	return R(
		conn.template.Variable["bind_string"],
		"i", cast.ToString(i),
		"field", field,
	)
}

func (conn *BaseConn) quote(i int, field string) string {
	q := conn.template.Variable["quote_string"]
	return q + field + q
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *BaseConn) GenerateInsertStatement(tableName string, fields []string) string {

	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	for i, field := range fields {
		values[i] = conn.bindVar(i+1, field)
		qFields[i] = "\"" + field + "\""
	}

	return R(
		"INSERT INTO {table} ({fields}) VALUES ({values})",
		"table", tableName,
		"fields", strings.Join(fields, ", "),
		"values", strings.Join(values, ", "),
	)
}

// InsertStream inserts a stream into a table
func (conn *BaseConn) InsertStream(tableFName string, ds Datastream) (count uint64, err error) {

	insertTemplate := conn.GenerateInsertStatement(tableFName, ds.GetFields())

	tx := conn.db.MustBegin()
	for row := range ds.Rows {
		count++
		// Do insert
		_, err := tx.Exec(insertTemplate, row...)
		if err != nil {
			tx.Rollback()
			return count, Error(
				err,
				F("Insert: %s\nFor Row: %#v", insertTemplate, row),
			)
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
		nativeType, ok := conn.template.GeneralTypeMap[col.Type]
		if !ok {
			return "", errors.New(
				F(
					"No type mapping defined for '%s' for '%s'",
					col.Type,
					conn.Type,
				),
			)
		}

		// Add precision as needed
		if strings.HasSuffix(nativeType, "()") {
			length := col.stats.maxLen*2
			if col.Type == "string" {
				if length < 255 {
					length = 255
				}
				nativeType = strings.ReplaceAll(
					nativeType,
					"()",
					F("(%d)", length),
				)
			} else if col.Type == "integer" {
				if length < 10 {
					length = 10
				}
				nativeType = strings.ReplaceAll(
					nativeType,
					"()",
					F("(%d)", length),
				)
			}
		} else if strings.HasSuffix(nativeType, "(,)") {
			length := col.stats.maxLen*2
			scale := col.stats.maxDecLen*2
			if col.Type == "decimal" {
				if length < 10 {
					length = 10
				}
				if scale < 4 {
					scale = 4
				}
				nativeType = strings.ReplaceAll(
					nativeType,
					"(,)",
					F("(%d,%d)", length, scale),
				)
			}
		}

		columnDDL := F(
			"%s %s",
			col.Name,
			nativeType,
		)
		columnsDDL = append(columnsDDL, columnDDL)
	}

	ddl := R(
		conn.template.Core["create_table"],
		"table", tableFName,
		"col_types", strings.Join(columnsDDL, ",\n"),
	)

	log.WithFields(log.Fields{
		"table":          tableFName,
		"ddl":            ddl,
		"buffer.Columns": data.Columns,
		"buffer.Rows":    len(data.Rows),
	}).Debug("generated DDL")

	return ddl, nil
}
