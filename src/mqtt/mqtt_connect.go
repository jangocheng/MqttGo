package mqtt

import (
    "fmt"
    "encoding/binary"
    "errors"
    "bytes"
)

type MqttConnectCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttConnectVariableHeader
    payload MqttConnectPayload
}

type MqttConnectVariableHeader struct {
    protocolName string
    protocolLevel byte
    flags byte
    keepAlive int
}

type MqttConnectPayload struct {
    clientId string
    
    willTopic string
    willMessage []byte
    
    username string
    password string
}

func (cmd *MqttConnectCommand) ClientId() string {
    return cmd.payload.clientId
}

func (cmd *MqttConnectCommand) Username() string {
    return cmd.payload.username
}

func (cmd *MqttConnectCommand) CleanSession() bool {
    if cmd.variableHeader.flags & 0x20 == 0x20 {
        return true
    } else {
        return false
    }
}

func (cmd *MqttConnectCommand) Process(c *Client) error {
    fmt.Println("Process MQTT connect command: username[", cmd.payload.username, "] password[", cmd.payload.password, "] clientId[", cmd.payload.clientId, "]")
    
    //add new client into ClientMap
    ClientMapSingleton().saveNewClient(cmd.payload.clientId, c)
    
    //send back ConnAck command
    ackCmd := NewMqttConnackCommand()
    c.SendCommand(ackCmd)
    
    return nil
}

func (cmd *MqttConnectCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {
    cmd.fixedHeader = *fixedHeader

    var tempBuf []byte
    tempBuf, err = cmd.parseVariableHeader(buf)
    if (err != nil) {
        return
    }
    restBuf, err = cmd.parsePayload(tempBuf)
    if err != nil {
        return
    }
    
    return
}

func (cmd *MqttConnectCommand) parseVariableHeader(buf []byte) (restBuf []byte, err error) {
    index := 0
    
    //check procotol name length
    if buf[0] == 0 && buf[1] == 4 {
        //version 3.1.1
        name := make([]byte, 4)
        name[0] = buf[2]; name[1] = buf[3]; name[2] = buf[4]; name[3] = buf[5]
        cmd.variableHeader.protocolName = string(name)
        if cmd.variableHeader.protocolName != "MQTT" {
            err = errors.New("protocolName is wrong")
            return
        }
        index = 6
    } else if buf[0] == 0 && buf[1] == 6 {
        //version 3.1
        cmd.variableHeader.protocolName = string(buf[2:8])
        if cmd.variableHeader.protocolName != "MQIsdp" {
            err = errors.New("procotol name is wrong")
            return
        }
        index = 8
    } else {
        err = errors.New("procotol length is wrong")
        return
    }
    
    //procotol level
    cmd.variableHeader.protocolLevel = buf[index]
    index++
    
    //flags
    cmd.variableHeader.flags = buf[index]
    index++
    
    //keepAlive
    cmd.variableHeader.keepAlive = int(binary.BigEndian.Uint16(buf[index:index+2]))
    index += 2
    
    restBuf = buf[index:]
    fmt.Println("procotol name:", cmd.variableHeader.protocolName, ",keepAlive:", cmd.variableHeader.keepAlive)
    return
}

func (cmd *MqttConnectCommand) parsePayload(buf []byte) (restBuf []byte, err error) {
    index := 0

    //clientId
    clientIdLength := int(binary.BigEndian.Uint16(buf[0:2]))
    index += 2
    cmd.payload.clientId = string(buf[index: index+clientIdLength])
    index += clientIdLength
    fmt.Println("ClientId length:", clientIdLength, ", ClientId:", cmd.payload.clientId)
    
    //will topic
    if cmd.variableHeader.getWillFlag() {
        willTopicLength := int(binary.BigEndian.Uint16(buf[index: index+2]))
        index += 2
        
        cmd.payload.willTopic = string(buf[index: index+willTopicLength])
        index += willTopicLength
        
        willMsgLength := int(binary.BigEndian.Uint16(buf[index: index+2]))
        index += 2
        
        cmd.payload.willMessage = buf[index: index+willMsgLength]
        index += willMsgLength
    }
    
    //user name
    if cmd.variableHeader.getUserNameFlag() {
        usernameLength := int(binary.BigEndian.Uint16(buf[index: index+2]))
        index += 2
        
        cmd.payload.username = string(buf[index: index+usernameLength])
        index += usernameLength
        fmt.Println("username length:", usernameLength, ", username:", cmd.payload.username)
    }
    
    //password
    if cmd.variableHeader.getPasswordFlag() {
        passwordLength := int(binary.BigEndian.Uint16(buf[index: index+2]))
        index += 2
        
        cmd.payload.password = string(buf[index: index+passwordLength])
        index += passwordLength
        fmt.Println("password length:", passwordLength, ", password:", cmd.payload.password)
    }
    
    restBuf = buf[index:]
    return
}

func (vh *MqttConnectVariableHeader) getWillFlag() bool {
    if vh.flags & 0x04 == 0x04 {
        return true
    } else {
        return false
    }
}

func (vh *MqttConnectVariableHeader) getUserNameFlag() bool {
    if vh.flags & 0x80 == 0x80 {
        return true
    } else {
        return false
    }
}

func (vh *MqttConnectVariableHeader) getPasswordFlag() bool {
    if vh.flags & 0x40 == 0x40 {
        return true
    } else {
        return false
    }
}

func (cmd *MqttConnectCommand) Buffer(buf *bytes.Buffer) error {
    return nil
}
