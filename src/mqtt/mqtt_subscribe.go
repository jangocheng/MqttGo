package mqtt

import (
    "fmt"
    "bytes"
    "encoding/binary"
)

func NewMqttSubscribeCommand() *MqttSubscribeCommand {
    return &MqttSubscribeCommand{
        fixedHeader : MqttFixedHeader{0x20, 0x02}, 
        variableHeader : MqttSubscribeVariableHeader{0},
        payload : MqttSubscribePayload{make([]*MqttSubscribePacket, 16)},
    }
}

type MqttSubscribeCommand struct {
    fixedHeader MqttFixedHeader
    variableHeader MqttSubscribeVariableHeader
    payload MqttSubscribePayload
}

type MqttSubscribeVariableHeader struct {
    packetId int
}

type MqttSubscribePayload struct {
    packets []*MqttSubscribePacket
}

type MqttSubscribePacket struct {
    topic string
    qos int
}

func (cmd *MqttSubscribeCommand) Process(c *Client) error {
    fmt.Println("Process MQTT subscribe command")
    return nil
}

func (cmd *MqttSubscribeCommand) Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error) {
    //check the length
    if fixedHeader.remainLength > len(buf) {
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

func (cmd *MqttSubscribeCommand) parseVariableHeader(buf []byte) (restBuf []byte, err error) {
    cmd.variableHeader.packetId = int(binary.BigEndian.Uint16(buf[0:2]))
    fmt.Println("Subscribe command packetId:", cmd.variableHeader.packetId)

    restBuf = buf[2:]
    return
}

func (cmd *MqttSubscribeCommand) parsePayload(buf []byte) (restBuf []byte, err error) {
    index := 0
    
    for index < len(buf) {
        p := new(MqttSubscribePacket)
    
        topicLength := int(binary.BigEndian.Uint16(buf[index:index+2]))
        if index+2+topicLength >= len(buf) {
            err = &ParseError{index, "topic name length is too big"}
            return
        }
        
        p.topic = string(buf[index+2:index+2+topicLength])
        index += 2+topicLength
        fmt.Println("Subscribe command topic:", p.topic, ", index:",index,",length:",len(buf))
        
        if index+1 > len(buf) {
            err = &ParseError{index, "there is no qos field"}
            return
        }
        p.qos = int(buf[index])
        fmt.Println("Subscribe command qos:", p.qos)
        
        index++
        
        cmd.payload.packets = append(cmd.payload.packets, p)
    }
    
    restBuf = buf[index:]
    return
}

func (cmd *MqttSubscribeCommand) Buffer(buf *bytes.Buffer) error {    
    return nil
}
