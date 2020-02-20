package gxutil

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

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

	conn.BaseConn.SetProp("allow_bulk_import", "false")

	return conn.BaseConn.LoadYAML()
}

// BulkImportStream bulk import stream
func (conn *OracleConn) BulkImportStream(tableFName string, ds Datastream) (count uint64, err error) {
	_, err = exec.LookPath("sqlldr")
	if err != nil {
		Log("sqlldr not found in path. Using cursor...")
		return conn.BaseConn.InsertStream(tableFName, ds)
	}

	if !cast.ToBool(conn.BaseConn.GetProp("allow_bulk_import")) {
		return conn.BaseConn.InsertStream(tableFName, ds)
	}

	return conn.SQLLoad(tableFName, ds)
}

// SQLLoad uses sqlldr to Bulk Import
// cat test1.csv | sqlldr system/oracle@oracle.host:1521/xe control=sqlldr.ctl log=/dev/stdout bad=/dev/stderr
func (conn *OracleConn) SQLLoad(tableFName string, ds Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		Log("Error dburl.Parse(conn.URL)")
		return
	}

	ctlPath := F(
		"/tmp/oracle.sqlldr.%d.%s.ctl",
		time.Now().Unix(),
		RandString(alphaRunes, 3),
	)

	// write to ctlPath
	ctlStr := R(
		conn.BaseConn.GetTemplateValue("core.sqlldr"),
		"table", tableFName,
		"columns", strings.Join(ds.GetFields(), ",\n"),
	)
	err = ioutil.WriteFile(
		ctlPath,
		[]byte(ctlStr),
		0755,
	)
	if err != nil {
		Log("Error writing to "+ctlPath)
		return
	}

	password, _ := url.User.Password()
	hostPort := url.Host
	sid := strings.ReplaceAll(url.Path, "/", "")
	credHost := F(
		"%s/%s@%s/%s", url.User.Username(),
		password, hostPort, sid,
	)

	proc := exec.Command(
		"sqlldr",
		credHost,
		"control="+ctlPath,
		"data=/dev/stdin",
		"log=/dev/stdout",
		"bad=/dev/stderr",
	)

	proc.Stderr = &stderr
	proc.Stdout = &stdout
	proc.Stdin = ds.NewCsvReader(0)

	// run and wait for finish
	err = proc.Run()

	// Delete ctrl file
	os.Remove(ctlPath)

	if err != nil {
		cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
		println(stdout.String())
		err = Error(
			err,
			F(
				"Oracle Import Command -> %s\nOracle Import Error  -> %s",
				cmdStr, stderr.String(),
			),
		)
	}

	return ds.count, err
}
