go build -race -buildmode=plugin ../mrapps/wc.go
echo "build finished"
go run -race workertest.go