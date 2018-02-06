package server

import (
    "net"
    
    client_impl "client/impl"
    command_impl "command/impl"
    . "command"
    . "zjlog"
)

func StartServer() {
    // create listener:
    listener, err := net.Listen("tcp", ":50000")
    if err != nil {
        Log().Error("Error listening", err.Error())
        return // terminate program
    }
    // listen and accept connections from clients:
    for {
        conn, err := listener.Accept()

        if err != nil {
            Log().Error("Error accepting", err.Error())
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
            Log().Error("Error reading", err.Error())
            conn.Close()
            return // terminate program
        }
        
        //parse input data
        var restBuf []byte
        fixedHeader := new(MqttFixedHeader)
        restBuf, err = fixedHeader.Parse(buf[:size])
        if err != nil {
            Log().Error("Error parse", err.Error())
            conn.Close()
            return // terminate program
        }
        
        if fixedHeader.PacketType() == 1 {
            //connect command
            cmd := new(command_impl.MqttConnectCommand)
            //var restBuf1 []byte
            _, err = cmd.Parse(restBuf, fixedHeader)
            if err != nil {
                Log().Error("Error parse", err.Error())
                return // terminate program
            }
            client := client_impl.NewClient(cmd.ClientId(), cmd.Username(), cmd.CleanSession(), conn)
            cmd.Process(client)
            break
        } else {
            Log().Error("Error command type", fixedHeader.PacketType())
            return
        }
    }
}