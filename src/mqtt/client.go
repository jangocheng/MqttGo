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
    outputBuf []byte "output data buffer"
    
    //command queue
    cmdQueue *[64]MqttCommand
    cmdQueueIndex int
    
    clientId string
    username string
    
    cleanSession bool
    
    conn net.Conn "client connection object"
}

func NewClient(clientId string, username string, cleanSession bool, conn net.Conn) *Client {
    return &Client{0, make([]byte, 1024), make([]byte, 0), new([64]MqttCommand), 0, clientId, username, cleanSession, conn}
}

func (c *Client) GetClientId() string {
    return c.clientId
}

func (c *Client) GetUsername() string {
    return c.username
}

func (c *Client) SendCommand(cmd MqttCommand) error {
    buf := new(bytes.Buffer)
    err := cmd.Buffer(buf)
    if err != nil {
        return err
    }
    
    temp := buf.Bytes()
    c.outputBuf = append(c.outputBuf, temp...)

    c.Send()
    
    return nil
}

func (c *Client) Send() {
    n, err := c.conn.Write(c.outputBuf)
    if err != nil {
        fmt.Println("Send command failed, err=", err)
        c.Close()
        return
    }
    fmt.Println("Send command to client, n=", n, "length=", len(c.outputBuf))

    if n < len(c.outputBuf) {
        //there are still some data need to send
        c.outputBuf = c.outputBuf[n:]
        
        go c.Send()
    } else {
        //all data send successfully
        c.outputBuf = make([]byte, 0)
    }
}

func (c *Client) Close() {
    c.conn.Close()
    ClientMapSingleton().removeClient(c.GetClientId())
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