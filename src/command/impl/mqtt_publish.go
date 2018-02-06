package impl

import (
    "log"
    "encoding/binary"
    "bytes"
    "errors"
    
    "persistence"
    . "command"
    . "subinfo"
    . "client"
)

type MqttPublishCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttPublishVariableHeader
    payload MqttPublishPayload
}

type MqttPublishVariableHeader struct {
    topic string
    packetId int
}

type MqttPublishPayload struct {
    msg []byte
}

func NewMqttPublishCommand() *MqttPublishCommand {
    return &MqttPublishCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_PUBLISH<<4, 0x00), 
        variableHeader : MqttPublishVariableHeader{"", 0},
        payload : MqttPublishPayload{nil},
    }
}

func (cmd *MqttPublishCommand) Dup() bool {
    return cmd.fixedHeader.FlagDup()
}

func (cmd *MqttPublishCommand) Qos() int {
    return cmd.fixedHeader.FlagQos()
}

func (cmd *MqttPublishCommand) Retain() bool {
    return cmd.fixedHeader.FlagRetain()
}

func (vh *MqttPublishVariableHeader) SetTopic(topic string) {
    vh.topic = topic
}

func (vh *MqttPublishVariableHeader) SetPacketId(packetId int) {
    vh.packetId = packetId
}

func (cmd *MqttPublishCommand) Process(c Client) error {
    log.Print("Process MQTT publish command: topic[", cmd.variableHeader.topic, "] qos[", cmd.fixedHeader.FlagQos(), "]")
    
    var msgId int64 = -1
    //save message into db
    if cmd.fixedHeader.FlagQos() > 0 {
        //only save Qos>0 message
        var err error
        msgId, err = persistence.SaveMessage(c.ClientId(), cmd.variableHeader.topic, cmd.payload.msg, cmd.variableHeader.packetId)
        if err != nil {
            log.Print("MqttPublishCommand save message failed:", err)
            return err
        }
    }
    
    switch cmd.Qos() {
    case 0, 1:
        return cmd.sendPublishMessageQos0or1(c, msgId)
    case 2:
        //send MQTT PubRec command
        pubRecCmd := NewMqttPubRecCommand()
        pubRecCmd.SetPacketId(cmd.variableHeader.packetId)
        return c.SendCommand(pubRecCmd)
    default:
        return errors.New("Wrong Qos")
    }
}

func (cmd *MqttPublishCommand) sendPublishMessageQos0or1(c Client, msgId int64) error {
    var clients []SubscribePair = SubscribeInfoSingleton().GetSubscribedClients(cmd.variableHeader.topic)
    if clients == nil {
        return cmd.sendPubAck(c)
    }
    
    flag := false
    for _, client := range clients {
        //check qos
        qos := client.Qos
        if qos > cmd.fixedHeader.FlagQos() {
            qos = cmd.fixedHeader.FlagQos()
        }
        
        publishCmd := NewMqttPublishCommand()
        publishCmd.fixedHeader.SetFlagDup(false)
        publishCmd.fixedHeader.SetFlagQos(qos)
        publishCmd.fixedHeader.SetFlagRetain(false)
        
        publishCmd.variableHeader.SetTopic(cmd.variableHeader.topic)
        packetId := client.C.NextPacketId()
        publishCmd.variableHeader.SetPacketId(int(packetId))
        
        publishCmd.payload.msg = cmd.payload.msg
        
        if qos > 0 && !client.C.CleanSession() {
            //save output message list
            err := persistence.SaveClientMessage(client.C.ClientId(), msgId, qos)
            if err != nil {
                log.Print("MqttPublishCommand save message list failed:", err)
                return err
            }
            client.C.SavePacketIdMapping(packetId, msgId)
        }
        
        if client.C == c {
            //the client sent Publish command still subscribe that topic, should send Puback command first
            cmd.sendPubAck(c)
            flag = true
        }
        client.C.SendCommand(publishCmd)
    }
    
    if !flag {
        cmd.sendPubAck(c)
    }
    
    return nil
}

func (cmd *MqttPublishCommand) sendPubAck(c Client) error {
    var err error
    if cmd.fixedHeader.FlagQos() > 0 {
        ackCmd := NewMqttPubackCommand()
        ackCmd.SetPacketId(cmd.variableHeader.packetId)
        err = c.SendCommand(ackCmd)
    }
    return err
}

func (cmd *MqttPublishCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {
    cmd.fixedHeader = *fixedHeader
    
    //parse variable header
    //topic
    topicLength := int(binary.BigEndian.Uint16(buf[0:2]))
    cmd.variableHeader.topic = string(buf[2:2+topicLength])
    variableHeaderLength := 2+topicLength
    
    //packet id
    if cmd.fixedHeader.FlagQos() > 0 {
        cmd.variableHeader.packetId = int(binary.BigEndian.Uint16(buf[2+topicLength:2+topicLength+2]))
        variableHeaderLength += 2
    }
    
    //parse payload
    cmd.payload.msg = buf[variableHeaderLength:cmd.fixedHeader.RemainLength()]
    
    restBuf = buf[cmd.fixedHeader.RemainLength():]
    
    return
}

func (cmd *MqttPublishCommand) Buffer(buf *bytes.Buffer) error {
    remainLength := 2 + len(cmd.variableHeader.topic) //topic
    if cmd.fixedHeader.FlagQos() > 0 {
        remainLength += 2 //packet id
    }
    remainLength += len(cmd.payload.msg)
    cmd.fixedHeader.SetRemainLength(remainLength)

    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return nil
    }
    
    var temp []byte = make([]byte, 2)
    binary.BigEndian.PutUint16(temp, uint16(len(cmd.variableHeader.topic)))
    buf.Write(temp)
    
    buf.WriteString(cmd.variableHeader.topic)
    
    if cmd.fixedHeader.FlagQos() > 0 {
        binary.BigEndian.PutUint16(temp, uint16(cmd.variableHeader.packetId))
        buf.Write(temp)
    }
    
    buf.Write(cmd.payload.msg)
    
    return nil
}
