# sling
A Piped CSV compatible ELT tool.

# Getting Started

```
git clone git@github.com:flarco/sling.git
cd sling
go get
go build -o sling
```

# Examples
`sling --srcDB $POSTGRES_URL --srcTable housing.florida_mls_data2 --limit 10 > /tmp/florida_mls_data2.csv`

`cat /tmp/florida_mls_data2.csv | sling --tgtDB $POSTGRES_URL --tgtTable housing.florida_mls_data3 --truncate`