set +H
export POSTGRES_URL="postgresql://admin:delta15!@192.168.10.130:5432/db1"
go test -v