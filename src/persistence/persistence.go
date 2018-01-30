package persistence

import (
    "fmt"
    "time"
    "sync"
    . "mqtttype"
)

type persistence interface {
    SaveClientConnection(clientId string, nodeId string, t time.Time) (insertId int64)
    SaveClientSubscribe(clientId string, info []*MqttSubscribePacket)
    RemoveAllSubscribe(clientId string) (info []*MqttSubscribePacket)
    GetAllSubscribe(clientId string) (info []*MqttSubscribePacket)
}

var p persistence
var once sync.Once

func persistenceSingleton() persistence {
    once.Do(func() {
        p = newMysqlPersistence()
    })
    return p
}

func SaveClientConnection(clientId string, nodeId string, t time.Time) (insertId int64, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("SaveClientConnection failed:%v", r)
            }
        }
    }()
    
    persistenceSingleton().SaveClientConnection(clientId, nodeId, t)
    return
}

func SaveClientSubscribe(clientId string, info []*MqttSubscribePacket) (err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("SaveClientSubscribe failed:%v", r)
            }
        }
    }()
    
    persistenceSingleton().SaveClientSubscribe(clientId, info)
    return
}

func RemoveAllSubscribe(clientId string) (info []*MqttSubscribePacket, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("RemoveAllSubscribe failed:%v", r)
            }
        }
    }()
    
    info = persistenceSingleton().RemoveAllSubscribe(clientId)
    return
}

func GetAllSubscribe(clientId string) (info []*MqttSubscribePacket, err error) {
    defer func() {
        // Println executes normally even if there is a panic
        if r := recover(); r != nil {
            var ok bool
            err, ok = r.(error)
            if !ok {
                err = fmt.Errorf("GetAllSubscribe failed:%v", r)
            }
        }
    }()
    
    info = persistenceSingleton().GetAllSubscribe(clientId)
    return
}
