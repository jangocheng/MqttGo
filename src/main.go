package main

import (
    "fmt"

    "server"
    . "zjlog"
)

func main() {
    e := NewLog("test.log")
    if e != nil {
        fmt.Println("failed to init log, error:", e)
        return
    }

    Log().Info("Starting the server ...")
    server.StartServer()
}


