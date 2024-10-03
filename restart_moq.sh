#!/bin/bash

# Change to the working directory
cd ~/akmoq/moxygen

# Find and kill the existing process if it's running
pid=$(pgrep -f 'moqrelayserverak -port 4433')
if [ -n "$pid" ]; then
    echo "Killing existing process with PID $pid"
    kill -9 $pid
fi

# Start the new process
nohup ./_build/bin/moqrelayserverak -port 4433 -cert ./certs/fullchain.pem -key ./certs/privkey.pem -endpoint "/moq" --logging INFO > nohup.out 2>&1 &
disown
echo "Started new process"
