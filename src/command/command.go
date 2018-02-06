package command

import (
    "fmt"
    "log"
    "bytes"
)

const (
    _ = iota
    MQTT_CMD_CONNECT
    MQTT_CMD_CONNACK
    MQTT_CMD_PUBLISH
    MQTT_CMD_PUBACK
    MQTT_CMD_PUBREC
    MQTT_CMD_PUBREL
    MQTT_CMD_PUBCOMP
    MQTT_CMD_SUBSCRIBE
    MQTT_CMD_SUBACK
    MQTT_CMD_UNSUBSCRIBE
    MQTT_CMD_UNSUBACK
    MQTT_CMD_PINGREQ
    MQTT_CMD_PINGRESP
    MQTT_CMD_DISCONNECT
)

type MqttFixedHeader struct {
    flag byte
    remainLength int
}

type MqttSubscribePacket struct {
    Topic string
    Qos int
}

func NewMqttFixedHeader(flag byte, length int) MqttFixedHeader {
    return MqttFixedHeader{flag, length}
}

func (fixedHeader *MqttFixedHeader) Parse(buf []byte) (restBuf []byte, err error) {
    if len(buf) < 2 {
        err = &NotCompleteError{"not a complete fixed header"}
        return
    }
    
    flag := buf[0]
    
    index := 1
    multiplier := 1
    value := 0
    for {
        encodedByte := buf[index]
        index++
        value += int(encodedByte & 127) * multiplier
        multiplier *= 128
        
        if multiplier > 128*128*128 {
            err = &ParseError{index, "remain length format error"}
            return
        }
        
        if encodedByte & 128 == 0 {
            break
        }

        if index >= len(buf) {
            err = &NotCompleteError{"Not a complete commmand"}
            return
        }
    }
    
    fixedHeader.flag = flag
    fixedHeader.remainLength = value
    
    restBuf = buf[index:]
    log.Print("Type:", fixedHeader.PacketType(), ", remainlength:", fixedHeader.RemainLength(), ", index:", index)
    return
}

func (fixedHeader *MqttFixedHeader) SetFlag(flag byte) {
    fixedHeader.flag = flag
}

func (fixedHeader *MqttFixedHeader) PacketType() int {
    return int(fixedHeader.flag >> 4)
}

func (fixedHeader *MqttFixedHeader) FlagDup() bool {
    if fixedHeader.flag & 0x08 == 0x08 {
        return true
    } else {
        return false
    }
}

func (fixedHeader *MqttFixedHeader) SetFlagDup(dup bool) {
    if dup {
        fixedHeader.flag |= 0x08
    } else {
        fixedHeader.flag &^= 0x08
    }
}

func (fixedHeader *MqttFixedHeader) FlagQos() int {
    return int((fixedHeader.flag & 0x06) >> 1)
}

func (fixedHeader *MqttFixedHeader) SetFlagQos(qos int) {
    v := byte(qos)
    v <<= 1
    fixedHeader.flag &^= 0x06
    fixedHeader.flag |= v
}

func (fixedHeader *MqttFixedHeader) FlagRetain() bool {
    if fixedHeader.flag & 0x01 == 0x01 {
        return true
    } else {
        return false
    }
}

func (fixedHeader *MqttFixedHeader) SetFlagRetain(retain bool) {
    if retain {
        fixedHeader.flag |= 0x01
    } else {
        fixedHeader.flag &^= 0x01
    }
}

func (fixedHeader *MqttFixedHeader) RemainLength() int {
    return fixedHeader.remainLength
}

func (fixedHeader *MqttFixedHeader) SetRemainLength(length int) {
    fixedHeader.remainLength = length
}

func (fixedHeader *MqttFixedHeader) Buffer(buf *bytes.Buffer) error {
    buf.WriteByte(fixedHeader.flag)
    
    //remain length
    remainLength := fixedHeader.remainLength
    for {
        var encodedByte byte = byte(remainLength % 128)
        remainLength /= 128
        
        if remainLength > 0 {
            encodedByte |= 0x80
        }
        
        buf.WriteByte(encodedByte)
        
        if remainLength <= 0 {
            break
        }
    }
    
    return nil
}

type ParseError struct {
    Index int
    Word string
}

func (e *ParseError) Error() string {
    return fmt.Sprintf("pkg parse:", e.Word, "at", e.Index)
}

type NotCompleteError struct {
    Word string
}

func (e *NotCompleteError) Error() string {
    return fmt.Sprintf("Not a complete command")
}