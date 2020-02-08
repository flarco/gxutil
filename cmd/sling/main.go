package main

import (
	"io/ioutil"
	"math"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	g "github.com/flarco/gxutil"
	"github.com/integrii/flaggy"
	"github.com/spf13/cast"
)

var version = "0.2"

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
	srcDB    string
	srcTable string
	tgtDB    string
	tgtTable string
	sqlFile  string
	s3Bucket string
	limit    uint64
	append   bool
	truncate bool
	in       bool
	out      bool
}

func Init() {
	cfg := Config{}
	showExamples := false
	start = time.Now()

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
	flaggy.Bool(&cfg.append, "", "append", "Append to the target table (default drops and recreates).")
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
	g.PrintV(version)
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
		runInToDB(cfg)
	} else if DbToDb {
		runDbToDb(cfg)
	} else if DbToOut {
		runDbToOut(cfg)
	} else if showExamples {
		println(examples)
	}
}

func runDbToOut(c Config) {
	srcConn := g.GetConn(c.srcDB)
	err := srcConn.Connect()
	g.LogErrorExit(err)

	csv := g.CSV{File: os.Stdout}

	sql := `select * from ` + c.srcTable

	if c.sqlFile != "" {
		bytes, err := ioutil.ReadFile(c.sqlFile)
		if err != nil {
			g.LogErrorExit(err)
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

	stream, err := srcConn.BulkStream(sql)
	g.LogErrorExit(err)

	cnt, err := csv.WriteStream(stream)
	g.LogErrorExit(err)
	g.Log(g.F("wrote %d rows [%s r/s]", cnt, getRate(cnt)))

}

func runInToDB(c Config) {
	tgtConn := g.GetConn(c.tgtDB)
	err := tgtConn.Connect()
	g.LogErrorExit(err)

	csv := g.CSV{File: os.Stdin}
	stream, err := csv.ReadStream()
	g.LogErrorExit(err)

	if !c.append && !c.truncate {
		d := g.Dataset{Columns: stream.Columns, Rows: stream.Buffer}
		newDdl, err := tgtConn.GenerateDDL(c.tgtTable, d)

		err = tgtConn.DropTable(c.tgtTable)
		g.LogErrorExit(err)

		_, err = tgtConn.Db().Exec(newDdl)
		g.LogErrorExit(err)
		g.Log("(re)created table " + c.tgtTable)
	} else if c.truncate {
		_, err = tgtConn.Db().Exec(`truncate table ` + c.tgtTable)
		g.LogErrorExit(err)
		g.Log("truncated table " + c.tgtTable)
	}

	g.Log("streaming inserts")
	// stream.SetProgressBar()
	cnt, err := tgtConn.InsertStream(c.tgtTable, stream)
	g.LogErrorExit(err)
	g.Log(g.F("inserted %d rows [%s r/s]", cnt, getRate(cnt)))

	tgtConn.Close()
}

func runDbToDb(c Config) {

	// var srcConn, tgtConn PostgresConn
	srcConn := g.GetConn(c.srcDB)
	tgtConn := g.GetConn(c.tgtDB)

	err := srcConn.Connect()
	g.LogErrorExit(err)

	err = tgtConn.Connect()
	g.LogErrorExit(err)

	srcConn.SetProp("s3Bucket", c.s3Bucket)
	tgtConn.SetProp("s3Bucket", c.s3Bucket)

	sql := `select * from ` + c.srcTable

	if c.sqlFile != "" {
		bytes, err := ioutil.ReadFile(c.sqlFile)
		if err != nil {
			g.LogErrorExit(err)
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

	stream, err := srcConn.BulkStream(sql)
	g.LogErrorExit(err)

	if !c.append && !c.truncate {

		d := g.Dataset{Columns: stream.Columns, Rows: stream.Buffer}
		newDdl, err := tgtConn.GenerateDDL(c.tgtTable, d)

		err = tgtConn.DropTable(c.tgtTable)
		g.LogErrorExit(err)

		_, err = tgtConn.Db().Exec(newDdl)
		g.LogErrorExit(err)
		g.Log("created table " + c.tgtTable)
	} else if c.truncate {
		_, err = tgtConn.Db().Exec(`truncate table ` + c.srcTable)
		g.LogErrorExit(err)
		g.Log("truncated table " + c.tgtTable)
	}

	g.Log("streaming inserts")
	// stream.SetProgressBar()
	cnt, err := tgtConn.InsertStream(c.tgtTable, stream)
	g.LogErrorExit(err)
	g.Log(g.F("inserted %d rows [%s r/s]", cnt, getRate(cnt)))

	srcConn.Close()
	tgtConn.Close()
}

func getRate(cnt uint64) string {
	return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
}

func main() {
	Init()
}
