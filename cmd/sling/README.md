# sling

A Piped CSV compatible ELT tool.

# Getting Started

**Go Install**

`go get github/com/flarco/cmd/sling`

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

```
git clone git@github.com:flarco/sling.git
cd sling
go get
go build -o sling
```

# Examples
`sling --srcDB $POSTGRES_URL --srcTable housing.florida_mls_data2 --limit 10 > /tmp/florida_mls_data2.csv`

`cat /tmp/florida_mls_data2.csv | sling --tgtDB $POSTGRES_URL --tgtTable housing.florida_mls_data3 --truncate`