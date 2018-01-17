package main

import (
    "fmt"
    "net"
    "./mqtt"
)

func main() {
    fmt.Println("Starting the server ...")
    // create listener:
    listener, err := net.Listen("tcp", "localhost:50000")
    if err != nil {
        fmt.Println("Error listening", err.Error())
        return // terminate program
    }
    // listen and accept connections from clients:
    for {
        conn, err := listener.Accept()

        if err != nil {
            fmt.Println("Error accepting", err.Error())
            return // terminate program
        }
        go doNewClient(conn)
    }
}

func doNewClient(conn net.Conn) {
    for {
        var err error
        var size int
        buf := make([]byte, 512)
        size, err = conn.Read(buf)
        if err != nil {
            fmt.Println("Error reading", err.Error())
            conn.Close()
            return // terminate program
        }
        
        //parse input data
        var restBuf []byte
        fixedHeader := new(mqtt.MqttFixedHeader)
        restBuf, err = fixedHeader.Parse(buf[:size])
        if err != nil {
            fmt.Println("Error parse", err.Error())
            conn.Close()
            return // terminate program
        }
        
        if fixedHeader.GetPacketType() == 1 {
            //connect command
            cmd := new(mqtt.MqttConnectCommand)
            //var restBuf1 []byte
            _, err = cmd.Parse(restBuf, fixedHeader)
            if err != nil {
                fmt.Println("Error parse", err.Error())
                return // terminate program
            }
            client := mqtt.NewClient(cmd.ClientId(), cmd.Username(), cmd.CleanSession(), conn)
            cmd.Process(client)
            fmt.Println("go here")
            break
        } else {
            fmt.Println("Error command type", fixedHeader.GetPacketType())
            return
        }
    }
    fmt.Println("exit doNewClient")
}