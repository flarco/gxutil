package main

import (
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	g "github.com/flarco/gxutil"
	"github.com/integrii/flaggy"
	"github.com/spf13/cast"
)

var version = "0.3"

var start time.Time
var DbDb *flaggy.Subcommand
var DbFf *flaggy.Subcommand
var FfDb *flaggy.Subcommand

var examples = `
sling --srcDB $POSTGRES_URL --srcTable housing.my_data2 --limit 10 > /tmp/my_data2.csv
cat /tmp/my_data2.csv | sling --tgtDB $POSTGRES_URL --tgtTable housing.my_data3 --truncate

sling --srcDB $POSTGRES_URL --srcTable housing.my_data2 | sling --tgtDB $POSTGRES_URL --tgtTable housing.my_data3

sling --srcDB $POSTGRES_URL --tgtDB $POSTGRES_URL --srcTable housing.my_data2 --tgtTable housing.my_data3
`

// Config is a config for the sling task
type Config struct {
	srcDB       string
	srcTable    string
	tgtDB       string
	tgtTable    string
	sqlFile     string
	s3Bucket    string
	limit       uint64
	drop        bool
	truncate    bool
	in          bool
	out         bool
	file        *os.File
	tgtTableDDL string
	tgtTableTmp string
}

func init() {
}

func Init() {
	cfg := Config{}
	showExamples := false

	// determine if stdin data is piped
	// https://stackoverflow.com/a/26567513
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		cfg.in = true
	}

	// Set your program's name and description.  These appear in help output.
	flaggy.SetName("Sling")
	flaggy.SetDescription("A ELT tool.")

	// You can disable various things by changing bools on the default parser
	// (or your own parser if you have created one).
	flaggy.DefaultParser.ShowHelpOnUnexpected = true

	// You can set a help prepend or append on the default parser.
	flaggy.DefaultParser.AdditionalHelpPrepend = "Slings data from a data source to a data target."

	// Add a flag to the main program (this will be available in all subcommands as well).
	flaggy.String(&cfg.srcDB, "", "srcDB", "The source database.")
	flaggy.String(&cfg.tgtDB, "", "tgtDB", "The target database.")
	flaggy.String(&cfg.srcTable, "", "srcTable", "The source table.")
	flaggy.String(&cfg.tgtTable, "", "tgtTable", "The target table.\n")
	// flaggy.Bool(&cfg.in, "", "in", "Use STDIN  Pipe as source (as CSV format).")
	// flaggy.Bool(&cfg.out, "", "out", "Use STDOUT Pipe as target (as CSV format).")
	flaggy.String(&cfg.sqlFile, "", "sqlFile", "The path of sql file to use as query")
	flaggy.UInt64(&cfg.limit, "", "limit", "The maximum rows to transfer (0 is infinite)")
	flaggy.Bool(&cfg.drop, "", "drop", "Drop the target table before load (default appends).")
	flaggy.Bool(&cfg.truncate, "", "truncate", "Truncate the target table before inserting / appending (default drops and recreates).\n")
	flaggy.String(&cfg.s3Bucket, "", "s3Bucket", "The S3 Bucket to use (for Redshift transfers).")
	flaggy.Bool(&showExamples, "", "examples", "Shows some examples.")

	// Create any subcommands and set their parameters.
	DbDb = flaggy.NewSubcommand("db-db")
	DbDb.Description = "Transfer data from Database to Database"
	// DbDb.String(&cfg.srcDB, "sd", "srcDB", "The source database.")
	// DbDb.String(&cfg.tgtDB, "td", "tgtDB", "The target database.")
	// DbDb.String(&cfg.srcTable, "st", "srcTable", "The source table.")
	// DbDb.String(&cfg.tgtTable, "tt", "tgtTable", "The target table.")
	// DbDb.UInt64(&cfg.limit, "", "limit", "The maximum rows to transfer (0 is infinite)")

	DbFf = flaggy.NewSubcommand("db-ff")
	DbFf.Description = "Transfer data from Database to Flat-File"

	FfDb = flaggy.NewSubcommand("ff-db")
	FfDb.Description = "Transfer data from Flat-File to Database"

	// flaggy.AttachSubcommand(DbDb, 1)
	// flaggy.AttachSubcommand(FfDb, 1)
	// flaggy.AttachSubcommand(DbFf, 1)

	// Set the version and parse all inputs into variables.
	flaggy.SetVersion(version)
	flaggy.Parse()

	InToDB := (cfg.in && cfg.tgtDB != "")
	DbToDb := cfg.srcDB != "" && cfg.tgtDB != ""
	DbToOut := cfg.srcDB != "" && cfg.tgtDB == ""

	ok := InToDB || DbToDb || DbToOut || showExamples

	if !ok {
		flaggy.ShowHelp("Must specify srcDB or tgtDB with STDIN data.")
		return
	}

	if InToDB {
		cfg.file = os.Stdin
		g.LogErrorExit(runFileToDB(cfg))
	} else if DbToDb {
		g.LogErrorExit(runDbToDb(cfg))
	} else if DbToOut {
		cfg.file = os.Stdout
		g.LogErrorExit(runDbToFile(cfg))
	} else if showExamples {
		println(examples)
	}
}

// writeTmpToTarget write from the temp table to the final table
// data is already in temp table
func writeTmpToTarget(c Config, tgtConn g.Connection) (err error) {
	// drop / replace / insert into target
	if c.drop {
		err = tgtConn.DropTable(c.tgtTable)
		if err != nil {
			return g.Error(err, "Could not drop table "+c.tgtTable)
		}

		// rename tmp to tgt
		sql := g.R(
			tgtConn.GetTemplateValue("core.rename_table"),
			"table", c.tgtTableTmp,
			"new_table", c.tgtTable,
		)
		_, err = tgtConn.Db().Exec(sql)
		if err != nil {
			return g.Error(err, "Could not execute SQL: "+sql)
		}
	} else {
		// insert

		tgtColsData, err := tgtConn.GetColumns(c.tgtTable)
		if err != nil {
			return g.Error(err, "Could not GetColumns for "+c.tgtTable)
		}

		tgtCols := []string{}
		for _, row := range tgtColsData.Rows {
			tgtCols = append(tgtCols, cast.ToString(row[0]))
		}

		sql := g.R(
			tgtConn.GetTemplateValue("core.insert_temp"),
			"table", c.tgtTable,
			"cols", strings.Join(tgtCols, ", "),
			"temp_table", c.tgtTableTmp,
		)

		_, err = tgtConn.Db().Exec(sql)
		if err != nil {
			return g.Error(err, "Could not execute SQL: "+sql)
		}

	}
	return nil
}

func runDbToFile(c Config) (err error) {
	start = time.Now()

	srcConn := g.GetConn(c.srcDB)
	err = srcConn.Connect()
	if err != nil {
		return g.Error(err, "Could not connect to: "+srcConn.GetType())
	}

	srcConn.SetProp("s3Bucket", c.s3Bucket)

	csv := g.CSV{File: c.file}

	sql := `select * from ` + c.srcTable

	if c.sqlFile != "" {
		bytes, err := ioutil.ReadFile(c.sqlFile)
		if err != nil {
			return g.Error(err, "Could not ReadFile: "+c.sqlFile)
		}
		sql = string(bytes)
	}

	if c.limit > 0 {
		sql = g.R(
			srcConn.Template().Core["limit"],
			"fields", "*",
			"table", c.srcTable,
			"limit", cast.ToString(c.limit),
		)
	}

	stream, err := srcConn.BulkExportStream(sql)
	if err != nil {
		return g.Error(err, "Could not BulkStream: "+sql)
	}

	cnt, err := csv.WriteStream(stream)
	if err != nil {
		return g.Error(err, "Could not WriteStream")
	}

	g.Log(g.F("wrote %d rows [%s r/s]", cnt, getRate(cnt)))

	srcConn.Close()
	return nil
}

// create temp table
// load into temp table
// insert / upsert / replace into target table
func runFileToDB(c Config) (err error) {
	start = time.Now()
	tgtConn := g.GetConn(c.tgtDB)
	err = tgtConn.Connect()
	if err != nil {
		return g.Error(err, "Could not connect to: "+tgtConn.GetType())
	}

	csv := g.CSV{File: c.file}
	stream, err := csv.ReadStream()
	if err != nil {
		return g.Error(err, "Could not ReadStream")
	}

	tgtConn.SetProp("s3Bucket", c.s3Bucket)

	c.tgtTableTmp = c.tgtTable + g.RandString("alpha", 3)

	if c.drop {
		d := g.Dataset{Columns: stream.Columns, Rows: stream.Buffer}
		c.tgtTableDDL, err = tgtConn.GenerateDDL(c.tgtTable, d)
		if err != nil {
			return g.Error(err, "Could not create table "+c.tgtTable)
		}

		err = tgtConn.DropTable(c.tgtTable)
		if err != nil {
			return g.Error(err, "Could not drop table "+c.tgtTable)
		}

		_, err = tgtConn.Db().Exec(c.tgtTableDDL)
		if err != nil {
			return g.Error(err, "Could not execute SQL: "+c.tgtTableDDL)
		}
		g.Log("(re)created table " + c.tgtTable)
	} else if c.truncate {
		sql := `truncate table ` + c.tgtTable
		_, err = tgtConn.Db().Exec(sql)
		if err != nil {
			return g.Error(err, "Could not execute SQL: "+sql)
		}
		g.Log("truncated table " + c.tgtTable)
	}

	g.Log("streaming inserts")
	// stream.SetProgressBar()
	cnt, err := tgtConn.BulkImportStream(c.tgtTable, stream)
	if err != nil {
		return g.Error(err, "Could not InsertStream: "+c.tgtTable)
	}
	g.Log(g.F("inserted %d rows [%s r/s]", cnt, getRate(cnt)))

	tgtConn.Close()
	return nil
}

func runDbToDb(c Config) (err error) {
	start = time.Now()

	// var srcConn, tgtConn PostgresConn
	srcConn := g.GetConn(c.srcDB)
	tgtConn := g.GetConn(c.tgtDB)

	err = srcConn.Connect()
	if err != nil {
		return g.Error(err, "Could not connect to: "+srcConn.GetType())
	}

	err = tgtConn.Connect()
	if err != nil {
		return g.Error(err, "Could not connect to: "+tgtConn.GetType())
	}

	srcConn.SetProp("s3Bucket", c.s3Bucket)
	tgtConn.SetProp("s3Bucket", c.s3Bucket)

	sql := `select * from ` + c.srcTable

	if c.sqlFile != "" {
		bytes, err := ioutil.ReadFile(c.sqlFile)
		if err != nil {
			return g.Error(err, "Could not ReadFile: "+c.sqlFile)
		}
		sql = string(bytes)
	}

	if c.limit > 0 {
		sql = g.R(
			srcConn.Template().Core["limit"],
			"fields", "*",
			"table", c.srcTable,
			"limit", cast.ToString(c.limit),
		)
	}

	stream, err := srcConn.BulkExportStream(sql)
	if err != nil {
		return g.Error(err, "Could not BulkStream: "+sql)
	}

	c.tgtTableTmp = c.tgtTable + g.RandString("alpha", 3)

	if c.drop {
		d := g.Dataset{Columns: stream.Columns, Rows: stream.Buffer}
		c.tgtTableDDL, err = tgtConn.GenerateDDL(c.tgtTable, d)
		if err != nil {
			return g.Error(err, "Could not create table "+c.tgtTable)
		}

		err = tgtConn.DropTable(c.tgtTable)
		if err != nil {
			return g.Error(err, "Could not drop table "+c.tgtTable)
		}

		_, err = tgtConn.Db().Exec(c.tgtTableDDL)
		if err != nil {
			return g.Error(err, "Could not execute SQL: "+c.tgtTableDDL)
		}
		g.Log("created table " + c.tgtTable)
	} else if c.truncate {
		sql := `truncate table ` + c.srcTable
		_, err = tgtConn.Db().Exec(sql)
		if err != nil {
			return g.Error(err, "Could not execute SQL: "+sql)
		}
		g.Log("truncated table " + c.tgtTable)
	}

	g.Log("streaming inserts")
	// stream.SetProgressBar()
	cnt, err := tgtConn.BulkImportStream(c.tgtTable, stream)
	if err != nil {
		return g.Error(err, "Could not InsertStream: "+c.tgtTable)
	}
	g.Log(g.F("inserted %d rows [%s r/s]", cnt, getRate(cnt)))

	srcConn.Close()
	tgtConn.Close()
	return nil
}

func getRate(cnt uint64) string {
	return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
}

func main() {
	Init()
}
