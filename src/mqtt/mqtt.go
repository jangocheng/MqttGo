package mqtt

import (
    "fmt"
    "bytes"
)

type MqttCommand interface {
    //GetFixedHeader() MqttFixedHeader
    Process(c *Client) error
    Parse(buf []byte, fixedHeader *MqttFixedHeader) (restBuf []byte, err error)
    Buffer(buf *bytes.Buffer) error
}

type MqttFixedHeader struct {
    flag byte
    remainLength int
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
    fmt.Println("Type:", fixedHeader.GetPacketType(), ", remainlength:", fixedHeader.GetRemainLength(), ", index:", index)
    return
}

func (fixedHeader *MqttFixedHeader) GetPacketType() int {
    return int(fixedHeader.flag >> 4)
}

func (fixedHeader *MqttFixedHeader) GetFlagDup() bool {
    if fixedHeader.flag & 0x08 == 0x08 {
        return true
    } else {
        return false
    }
}

func (fixedHeader *MqttFixedHeader) GetFlagQos() int {
    return int((fixedHeader.flag & 0x06) >> 1)
}

func (fixedHeader *MqttFixedHeader) GetFlagRetain() bool {
    if fixedHeader.flag & 0x01 == 0x01 {
        return true
    } else {
        return false
    }
}

func (fixedHeader *MqttFixedHeader) GetRemainLength() int {
    return fixedHeader.remainLength
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