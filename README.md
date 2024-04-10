A toy project to run a consistent hash-ring with multiple nodes

start some nodes:
go run cmd/server/server.go -port 7070
go run cmd/server/server.go -port 7071
go run cmd/server/server.go -port 7072

start a client:
go run cmd/client/client.go 
