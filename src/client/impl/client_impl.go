package impl

import (
    "bytes"
    "net"
    "log"
    "errors"
    "sync"
    
    . "command"
    command_impl "command/impl"
    . "client"
)

type ClientImpl struct {
    status int "Current status of this client, such as INIT, HANDSHAKED"
    
    inputBuf []byte "input data buffer"
    outputChannel chan []byte
    
    //command queue
    cmdChannel chan MqttCommand
    
    clientId string
    username string
    
    cleanSession bool
    
    conn net.Conn "client connection object"
    
    mutex sync.Mutex
    packetId uint16
    packetIdMap map[uint16]int64
}

func NewClient(clientId string, username string, cleanSession bool, conn net.Conn) Client {
    c := &ClientImpl{
        inputBuf : make([]byte, 0), 
        outputChannel : make(chan []byte, 16),
        cmdChannel : make(chan MqttCommand, 16),
        clientId : clientId, 
        username : username, 
        cleanSession : cleanSession,
        conn : conn,
        packetId : 1,
        packetIdMap : make(map[uint16]int64)}
    go c.ProcessInputCommand()
    go c.doWrite()
    return c
}

func (c *ClientImpl) ClientId() string {
    return c.clientId
}

func (c *ClientImpl) Username() string {
    return c.username
}

func (c *ClientImpl) CleanSession() bool {
    return c.cleanSession
}

func (c *ClientImpl) NextPacketId() uint16 {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    if c.packetId == 0 {
        c.packetId++
    }
    
    temp := c.packetId
    c.packetId++
    return temp
}

func (c *ClientImpl) SavePacketIdMapping(packetId uint16, msgId int64) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    c.packetIdMap[packetId] = msgId
}

func (c *ClientImpl) RemovePacketIdMapping(packetId uint16) (int64, error) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    msgId, ok := c.packetIdMap[packetId]
    if ok {
        delete(c.packetIdMap, packetId)
        return msgId, nil
    } else {
        return 0, errors.New("No packetId")
    }
    
}

func (c *ClientImpl) SendCommand(cmd MqttCommand) error {
    buf := new(bytes.Buffer)
    err := cmd.Buffer(buf)
    if err != nil {
        return err
    }
    
    c.outputChannel <- buf.Bytes()
    
    return nil
}

func (c *ClientImpl) Read() {
    go c.doRead()
}

func (c *ClientImpl) doRead() {
    for {
        buf := make([]byte, 1024)
        n, err := c.conn.Read(buf)
        if err != nil {
            log.Print("Client[", c.ClientId(), "] Read error:", err.Error())
            c.conn.Close()
            return
        }
        c.inputBuf = append(c.inputBuf, buf[:n]...)
        
        LOOP:
        for len(c.inputBuf) > 0 {
            fixedHeader := new(MqttFixedHeader)
            restBuf, err := fixedHeader.Parse(c.inputBuf)
            switch err.(type) {
            case *NotCompleteError:
                log.Print("Not complete command, need to read again")
            case *ParseError:
                log.Print("Client[", c.ClientId(), "] input fixedHeader format error:", err)
                c.Close()
                return
            }

            var cmd MqttCommand = command_impl.NewMqttCommand(fixedHeader.PacketType())
            if cmd == nil {
                return
            }
            
            tempBuf, err := cmd.Parse(restBuf, fixedHeader)
            switch err.(type) {
            case *NotCompleteError:
                log.Print("Not complete command, need to read again")
                break LOOP
            case *ParseError:
                log.Print("Client[", c.ClientId(), "] input format error:", err)
                c.Close()
                return
            }
            c.inputBuf = tempBuf
            
            c.cmdChannel <- cmd
        }
    }
}

func (c *ClientImpl) doWrite() {
    var buf []byte
    for {
        buf =<- c.outputChannel
        
        for {
            n, err := c.conn.Write(buf)
            if err != nil {
                log.Print("Send command failed, err=", err)
                c.Close()
                return
            }
            log.Print("Send command to client, n=", n, "length=", len(buf))

            if n < len(buf) {
                //there are still some data need to send
                buf = buf[n:]
            } else {
                break
            }
        }
    }
}

func (c *ClientImpl) ProcessInputCommand() {
    var cmd MqttCommand
    for {
        cmd =<- c.cmdChannel
        cmd.Process(c)
    }
}

func (c *ClientImpl) Close() {
    c.conn.Close()
    ClientMapSingleton().RemoveClient(c.ClientId())
}
