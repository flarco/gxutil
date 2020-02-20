package gxutil

import (
	"bytes"
	"io"
	"os/exec"
	"strings"

	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// MySQLConn is a Postgres connection
type MySQLConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *MySQLConn) Init() error {

	u, err := dburl.Parse(conn.URL)
	if err != nil {
		return err
	}

	// Add tcp explicitly...
	// https://github.com/go-sql-driver/mysql/issues/427#issuecomment-474034276
	URL := strings.ReplaceAll(
		conn.URL,
		"@"+u.Host,
		F("@tcp(%s)", u.Host),
	)

	// remove scheme
	URL = strings.ReplaceAll(
		URL,
		"mysql://",
		"",
	)

	conn.BaseConn = BaseConn{
		URL:  URL,
		Type: "mysql",
	}

	// Turn off Bulk export for now
	// the LoadDataOutFile needs special circumstances
	conn.BaseConn.SetProp("allow_bulk_export", "false")
	conn.BaseConn.SetProp("allow_bulk_import", "true")

	return conn.BaseConn.LoadYAML()
}

// BulkInsert
// Common Error: ERROR 3948 (42000) at line 1: Loading local data is disabled; this must be enabled on both the client and server sides
// Need to enable on serer side: https://stackoverflow.com/a/60027776
// mysql server needs to be launched with '--local-infile=1' flag
// mysql --local-infile=1 -h {host} -P {port} -u {user} -p{password} mysql -e "LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"

// BulkExportStream bulk Export
func (conn *MySQLConn) BulkExportStream(sql string) (ds Datastream, err error) {
	_, err = exec.LookPath("mysql")
	if err != nil {
		Log("mysql not found in path. Using cursor...")
		return conn.BaseConn.StreamRows(sql)
	}

	if !cast.ToBool(conn.BaseConn.GetProp("allow_bulk_export")) {
		return conn.BaseConn.StreamRows(sql)
	}

	stdOutReader, err := conn.LoadDataOutFile(sql)
	if err != nil {
		return ds, err
	}

	csv := CSV{Reader: stdOutReader}
	ds, err = csv.ReadStream()

	return ds, err
}

// BulkImportStream bulk import stream
func (conn *MySQLConn) BulkImportStream(tableFName string, ds Datastream) (count uint64, err error) {
	_, err = exec.LookPath("mysql")
	if err != nil {
		Log("mysql not found in path. Using cursor...")
		return conn.BaseConn.InsertStream(tableFName, ds)
	}

	if !cast.ToBool(conn.BaseConn.GetProp("allow_bulk_import")) {
		return conn.BaseConn.InsertStream(tableFName, ds)
	}

	return conn.LoadDataInFile(tableFName, ds)
}

// LoadDataOutFile Bulk Export
// Possible error: ERROR 1227 (42000) at line 1: Access denied; you need (at least one of) the FILE privilege(s) for this operation
// File priviledge needs to be granted to user
// also the --secure-file-priv option needs to be set properly for it to work.
func (conn *MySQLConn) LoadDataOutFile(sql string) (stdOutReader io.Reader, err error) {
	var stderr bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		Log("Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")
	query := R(
		`{sql} INTO OUTFILE '/dev/stdout'
		FIELDS ENCLOSED BY '"' TERMINATED BY ',' ESCAPED BY '"'
		LINES TERMINATED BY '\r\n'`,
		"sql", sql,
	)
	proc := exec.Command(
		"mysql",
		// "--local-infile=1",
		"-h", host,
		"-P", url.Port(),
		"-u", url.User.Username(),
		"-p"+password,
		database,
		"-e", strings.ReplaceAll(query, "\n", " "),
	)
	proc.Stderr = &stderr
	stdOutReader, err = proc.StdoutPipe()

	go func() {
		err := proc.Run()
		if err != nil {
			// bytes, _ := proc.CombinedOutput()
			cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
			println(Error(
				err,
				F(
					"MySQL Export Command -> %s\nMySQL Export Error  -> %s",
					cmdStr, stderr.String(),
				),
			).Error())
		}
	}()

	return stdOutReader, err
}

// LoadDataInFile Bulk Import
func (conn *MySQLConn) LoadDataInFile(tableFName string, ds Datastream) (count uint64, err error) {
	var stderr bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		Log("Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")

	loadQuery := R(`LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;`, "table", tableFName)
	proc := exec.Command(
		"mysql",
		"--local-infile=1",
		"-h", host,
		"-P", url.Port(),
		"-u", url.User.Username(),
		"-p"+password,
		database,
		"-e", loadQuery,
	)

	proc.Stderr = &stderr
	proc.Stdin = ds.NewCsvReader(0)

	err = proc.Run()
	if err != nil {
		cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
		err = Error(
			err,
			F(
				"MySQL Import Command -> %s\nMySQL Import Error  -> %s",
				cmdStr, stderr.String(),
			),
		)
		return ds.count, err
	}

	return ds.count, nil
}
