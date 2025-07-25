#!/bin/bash

# Start port number
port=7071

# Start processes
for i in {1..8}
do
    go run cmd/server/server.go -port $port &
    echo "Started server on port $port"
    ((port+=11))
    # Small sleep to prevent overwhelming the system
    sleep 0.1
done

echo "All servers started. Use 'pkill -f \"server.go\"' to stop all servers."
