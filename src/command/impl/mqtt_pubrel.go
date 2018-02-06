package impl

import (
    "bytes"
    "encoding/binary"
    "log"
    
    "persistence"
    . "subinfo"
    . "command"
    . "client"
)

func NewMqttPubRelCommand() *MqttPubRelCommand {
    return &MqttPubRelCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_PUBREL<<4 | 0x02, 0x00), 
        variableHeader : MqttPubRelVariableHeader{0},
    }
}

type MqttPubRelCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttPubRelVariableHeader
}

type MqttPubRelVariableHeader struct {
    packetId uint16
}

func (cmd *MqttPubRelCommand) Process(c Client) error {
    log.Print("MqttPubRelCommand ClientId[", c.ClientId(), "] PacketId[", cmd.variableHeader.packetId, "]")
    //query from db to get message
    msgId, topic, message, err := persistence.GetQos2Message(c.ClientId(), int(cmd.variableHeader.packetId))
    if err != nil {
        log.Print("MqttPubRelCommand failed,", err)
        return err
    }
    //send message to other clients
    var clients []SubscribePair = SubscribeInfoSingleton().GetSubscribedClients(topic)
    if clients == nil {
        //send PubComp command
        cmd.sendPubComp(c)
        log.Print("MqttPubRelCommand there is no subscribed clients")
        return nil
    }
    
    flag := false
    for _, client := range clients {
        //check qos
        qos := client.Qos
        
        publishCmd := NewMqttPublishCommand()
        publishCmd.fixedHeader.SetFlagDup(false)
        publishCmd.fixedHeader.SetFlagQos(qos)
        publishCmd.fixedHeader.SetFlagRetain(false)
        
        publishCmd.variableHeader.SetTopic(topic)
        packetId := client.C.NextPacketId()
        publishCmd.variableHeader.SetPacketId(int(packetId))
        
        publishCmd.payload.msg = message
        
        log.Print("MqttPubRelCommand send publish command to client[", client.C.ClientId(), "] Qos[", qos, "]")
        
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
            cmd.sendPubComp(c)
            flag = true
        }
        client.C.SendCommand(publishCmd)
    }
    
    if !flag {
        cmd.sendPubComp(c)
    }
    
    return nil
}

func (cmd *MqttPubRelCommand) sendPubComp(c Client) {
    pubCompCmd := NewMqttPubCompCommand()
    pubCompCmd.SetPacketId(int(cmd.variableHeader.packetId))
    c.SendCommand(pubCompCmd)
}

func (cmd *MqttPubRelCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {    
    cmd.fixedHeader = *fixedHeader
    
    //parse variable header    
    //packet id
    cmd.variableHeader.packetId = binary.BigEndian.Uint16(buf[0:2])
    
    restBuf = buf[2:]
    
    return restBuf, nil
}

func (cmd *MqttPubRelCommand) Buffer(buf *bytes.Buffer) error {
    cmd.fixedHeader.SetRemainLength(2)

    err := cmd.fixedHeader.Buffer(buf)
    if err != nil {
        return err
    }
    
    var packetId []byte = make([]byte, 2)
    binary.BigEndian.PutUint16(packetId, cmd.variableHeader.packetId)
    buf.Write(packetId)
    
    return nil
}

func (cmd *MqttPubRelCommand) SetPacketId(id int) {
    cmd.variableHeader.packetId = uint16(id)
}
