# sling

A Piped CSV compatible ELT tool.

# Getting Started

`sling --help`

```
Sling - A ELT tool.
Slings data from a data source to a data target.

  Flags:
       --version    Displays the program version string.
    -h --help       Displays help with available flag, subcommand, and positional value parameters.
       --srcDB      The source database.
       --tgtDB      The target database.
       --srcTable   The source table.
       --tgtTable   The target table.

       --sqlFile    The path of sql file to use as query
       --limit      The maximum rows to transfer (0 is infinite) (default: 0)
       --drop       Drop the target table before load (default appends).
       --truncate   Truncate the target table before inserting / appending (default drops and recreates).

       --s3Bucket   The S3 Bucket to use (for Redshift transfers).
       --examples   Shows some examples.
```

## Examples
`sling --srcDB $POSTGRES_URL --srcTable housing.florida_mls_data2 --limit 10 > /tmp/florida_mls_data2.csv`

`cat /tmp/florida_mls_data2.csv | sling --tgtDB $POSTGRES_URL --tgtTable housing.florida_mls_data3 --truncate`


# Installation

**Mac Binary**

```
wget https://github.com/flarco/gxutil/releases/download/sling-latest-macOS/sling-mac.gz
gzip -d sling-mac.gz
./sling-mac --help
```

**Linux Binary**

```
wget https://github.com/flarco/gxutil/releases/download/sling-latest-Linux/sling-linux.gz
gzip -d sling-linux.gz
./sling-linux --help
```

**Install from Repo with Go**

```
# Need pkger to embed static files into the Go binary
go get github.com/markbates/pkger/cmd/pkger

git clone https://github.com/flarco/gxutil.git
cd gxutil/

# will create file cmd/sling/pkged.go
pkger -include github.com/flarco/gxutil:/templates -o cmd/sling

cd cmd/sling
go install
```
