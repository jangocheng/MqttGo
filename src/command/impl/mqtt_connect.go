package impl

import (
    "encoding/binary"
    "errors"
    "bytes"
    "time"
    "log"
    
    . "command"
    . "subinfo"
    . "client"
    "persistence"
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

func (cmd *MqttConnectCommand) Process(c Client) error {
    log.Print("Process MQTT connect command: username[", cmd.payload.username, "] password[", cmd.payload.password, "] clientId[", cmd.payload.clientId, "]")
    
    ackCmd := NewMqttConnackCommand()
    
    //add new client into ClientMap
    ClientMapSingleton().SaveNewClient(cmd.payload.clientId, c)
    
    id, err := persistence.SaveClientConnection(cmd.payload.clientId, "nodeId1", time.Now())
    if err != nil {
        log.Print("call SaveClientConnection error:", err)
        ackCmd.SetReturnCode(MQTT_CONNACK_SERVER_UNAVAILABLE)
        c.SendCommand(ackCmd)
        return err
    } else {
        log.Print("call SaveClientConnection insert id:", id)
    }
    
    //check if this client is clean session
    if c.CleanSession() {
        //this client is clean session, remove all session info
        topics, err := persistence.RemoveAllSubscribe(cmd.payload.clientId)
        if err != nil {
            log.Print("MqttConnectCommand RemoveAllSubscribe failed:", err)
            ackCmd.SetReturnCode(MQTT_CONNACK_SERVER_UNAVAILABLE)
            c.SendCommand(ackCmd)
            return err
        } else {
            SubscribeInfoSingleton().RemoveSubscribe(c, topics)
        }
    } else {
        //this client is not clean session, load all session info from db
        allSubscribeInfo, err := persistence.GetAllSubscribe(cmd.payload.clientId)
        if err != nil {
            log.Print("MqttConnectCommand GetAllSubscribe failed:", err)
            ackCmd.SetReturnCode(MQTT_CONNACK_SERVER_UNAVAILABLE)
            c.SendCommand(ackCmd)
            return err
        } else {
            SubscribeInfoSingleton().SaveNewSubscribe(c, allSubscribeInfo)
        }
        
        //read all message
        allMessage, err := persistence.GetClientMessage(cmd.payload.clientId)
        if err != nil {
            log.Print("MqttConnectCommand GetAllSubscribe failed:", err)
            ackCmd.SetReturnCode(MQTT_CONNACK_SERVER_UNAVAILABLE)
            c.SendCommand(ackCmd)
            return err
        } else {
            for _, msg := range allMessage {
                if msg.Topic == "" {
                    continue
                }
            
                publishCmd := NewMqttPublishCommand()
                publishCmd.fixedHeader.SetFlagDup(true)
                publishCmd.fixedHeader.SetFlagQos(msg.Qos)
                publishCmd.fixedHeader.SetFlagRetain(false)
                
                publishCmd.variableHeader.SetTopic(msg.Topic)
                packetId := c.NextPacketId()
                publishCmd.variableHeader.SetPacketId(int(packetId))
                c.SavePacketIdMapping(packetId, msg.Id)
                
                publishCmd.payload.msg = msg.Message

                c.SendCommand(publishCmd)
            }
        }
    }
    
    //send back ConnAck command
    c.SendCommand(ackCmd)
    
    //go on read input command
    c.Read()
    
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
    log.Print("procotol name:", cmd.variableHeader.protocolName, ",keepAlive:", cmd.variableHeader.keepAlive)
    return
}

func (cmd *MqttConnectCommand) parsePayload(buf []byte) (restBuf []byte, err error) {
    index := 0

    //clientId
    clientIdLength := int(binary.BigEndian.Uint16(buf[0:2]))
    index += 2
    cmd.payload.clientId = string(buf[index: index+clientIdLength])
    index += clientIdLength
    log.Print("ClientId length:", clientIdLength, ", ClientId:", cmd.payload.clientId)
    
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
        log.Print("username length:", usernameLength, ", username:", cmd.payload.username)
    }
    
    //password
    if cmd.variableHeader.getPasswordFlag() {
        passwordLength := int(binary.BigEndian.Uint16(buf[index: index+2]))
        index += 2
        
        cmd.payload.password = string(buf[index: index+passwordLength])
        index += passwordLength
        log.Print("password length:", passwordLength, ", password:", cmd.payload.password)
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
