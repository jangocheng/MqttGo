package mqtt

import (
    "sync"
    "bytes"
    "net"
    "fmt"
)

type Client struct {
    status int "Current status of this client, such as INIT, HANDSHAKED"
    
    inputBuf []byte "input data buffer"
    outputChannel chan []byte
    
    //command queue
    cmdChannel chan MqttCommand
    
    clientId string
    username string
    
    cleanSession bool
    
    conn net.Conn "client connection object"
}

func NewClient(clientId string, username string, cleanSession bool, conn net.Conn) *Client {
    c := &Client{0, make([]byte, 0), make(chan []byte, 16), make(chan MqttCommand, 16), clientId, username, cleanSession, conn}
    go c.ProcessInputCommand()
    go c.doWrite()
    return c
}

func (c *Client) ClientId() string {
    return c.clientId
}

func (c *Client) Username() string {
    return c.username
}

func (c *Client) SendCommand(cmd MqttCommand) error {
    buf := new(bytes.Buffer)
    err := cmd.Buffer(buf)
    if err != nil {
        return err
    }
    
    c.outputChannel <- buf.Bytes()
    
    return nil
}

func (c *Client) doRead() {
    for {
        buf := make([]byte, 1024)
        n, err := c.conn.Read(buf)
        if err != nil {
            fmt.Println("Client[", c.ClientId(), "] Read error:", err.Error())
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
                fmt.Println("Not complete command, need to read again")
            case *ParseError:
                fmt.Println("Client[", c.ClientId(), "] input fixedHeader format error:", err)
                c.Close()
                return
            }
            
            switch fixedHeader.PacketType() {
            case MQTT_CMD_SUBSCRIBE:
                //subscribe command
                cmd := new(MqttSubscribeCommand)
                tempBuf, err := cmd.Parse(restBuf, fixedHeader)
                switch err.(type) {
                case *NotCompleteError:
                    fmt.Println("Not complete command, need to read again")
                    break LOOP
                case *ParseError:
                    fmt.Println("Client[", c.ClientId(), "] input format error:", err)
                    c.Close()
                    return
                }
                c.inputBuf = tempBuf
                
                c.cmdChannel <- cmd
            case MQTT_CMD_PINGREQ:
                cmd := new(MqttPingReqCommand)
                cmd.fixedHeader = *fixedHeader
                c.inputBuf = restBuf
                c.cmdChannel <- cmd
            case MQTT_CMD_PUBLISH:
                cmd := NewMqttPublishCommand()
                tempBuf, err := cmd.Parse(restBuf, fixedHeader)
                switch err.(type) {
                case *NotCompleteError:
                    fmt.Println("Not complete command, need to read again")
                    break LOOP
                case *ParseError:
                    fmt.Println("Client[", c.ClientId(), "] input format error:", err)
                    c.Close()
                    return
                }
                c.inputBuf = tempBuf
                
                c.cmdChannel <- cmd
            default:
                fmt.Println("Error command type", fixedHeader.PacketType())
                return
            }
        }
    }
}

func (c *Client) doWrite() {
    var buf []byte
    for {
        buf =<- c.outputChannel
        
        for {
            n, err := c.conn.Write(buf)
            if err != nil {
                fmt.Println("Send command failed, err=", err)
                c.Close()
                return
            }
            fmt.Println("Send command to client, n=", n, "length=", len(buf))

            if n < len(buf) {
                //there are still some data need to send
                buf = buf[n:]
            } else {
                break
            }
        }
    }
}

func (c *Client) ProcessInputCommand() {
    var cmd MqttCommand
    for {
        cmd =<- c.cmdChannel
        cmd.Process(c)
    }
}

func (c *Client) Close() {
    c.conn.Close()
    ClientMapSingleton().removeClient(c.ClientId())
}

var clients *ClientMap
var once sync.Once

func ClientMapSingleton() *ClientMap {
    once.Do(func() {
        clients = &ClientMap{}
        clients.clientMap = make(map[string]*Client)
    })
    return clients
}


type ClientMap struct {
    clientMap map[string]*Client
}

func (m *ClientMap) getClient(clientId string) *Client {
    //TODO: mutex
    if c, ok := m.clientMap[clientId]; ok {
        return c
    } else {
        return nil
    }
}

func (m *ClientMap) saveNewClient(clientId string, c *Client) {
    //TODO: mutex
    m.clientMap[clientId] = c
}

func (m *ClientMap) removeClient(clientId string) {
    //TODO: mutex
    delete(m.clientMap, clientId)
}