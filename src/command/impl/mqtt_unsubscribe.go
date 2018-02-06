package impl

import (
    "log"
    "bytes"
    "encoding/binary"
    
    "persistence"
    . "command"
    . "subinfo"
    . "client"
)

func NewMqttUnSubscribeCommand() *MqttUnSubscribeCommand {
    return &MqttUnSubscribeCommand{
        fixedHeader : NewMqttFixedHeader(MQTT_CMD_UNSUBSCRIBE<<4 | 0x02, 0x00), 
        variableHeader : MqttUnSubscribeVariableHeader{0},
        payload : MqttUnSubscribePayload{make([]string, 0)},
    }
}

type MqttUnSubscribeCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttUnSubscribeVariableHeader
    payload MqttUnSubscribePayload
}

type MqttUnSubscribeVariableHeader struct {
    packetId int
}

type MqttUnSubscribePayload struct {
    topics []string
}

func (cmd *MqttUnSubscribeCommand) Process(c Client) error {
    log.Print("Process MQTT UnSubscribe command")
    SubscribeInfoSingleton().RemoveSubscribe(c, cmd.payload.topics)
    
    if !c.CleanSession() {
        //save into persistence
        _, err := persistence.RemoveClientSubscribe(c.ClientId(), cmd.payload.topics)
        if err != nil {
            log.Print("RemoveClientSubscribe failed:", err)
            return err
        }
    }

    //send back Suback command
    ackCmd := NewMqttUnSubackCommand()
    ackCmd.SetPacketId(cmd.variableHeader.packetId)
    c.SendCommand(ackCmd)
    return nil
}

func (cmd *MqttUnSubscribeCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {
    //check the length
    if fixedHeader.RemainLength() > len(buf) {
        //there is not enough data
        err = &NotCompleteError{"Not a complete subscribe command"}
        return
    }

    cmd.fixedHeader = *fixedHeader

    tempBuf, err := cmd.parseVariableHeader(buf)
    if (err != nil) {
        return
    }
    restBuf, err = cmd.parsePayload(tempBuf)
    if err != nil {
        return
    }
    
    return
}

func (cmd *MqttUnSubscribeCommand) parseVariableHeader(buf []byte) (restBuf []byte, err error) {
    cmd.variableHeader.packetId = int(binary.BigEndian.Uint16(buf[0:2]))
    log.Print("UnSubscribe command packetId:", cmd.variableHeader.packetId)

    restBuf = buf[2:]
    return
}

func (cmd *MqttUnSubscribeCommand) parsePayload(buf []byte) (restBuf []byte, err error) {
    index := 0
    
    for index < len(buf) {
    
        topicLength := int(binary.BigEndian.Uint16(buf[index:index+2]))
        if index+2+topicLength >= len(buf) {
            err = &ParseError{index, "topic name length is too big"}
            return
        }
        
        topic := string(buf[index+2:index+2+topicLength])
        index += 2+topicLength
        log.Print("UnSubscribe command topic:", topic, ", index:",index,",length:",len(buf))
        
        cmd.payload.topics = append(cmd.payload.topics, topic)
    }
    
    restBuf = buf[index:]
    return
}

func (cmd *MqttUnSubscribeCommand) Buffer(buf *bytes.Buffer) error {    
    return nil
}
