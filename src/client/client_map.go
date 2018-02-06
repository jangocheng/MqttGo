package client

import (
    "sync"
)

var clients *ClientMap
var once sync.Once

func ClientMapSingleton() *ClientMap {
    once.Do(func() {
        clients = &ClientMap{}
        clients.clientMap = make(map[string]Client)
    })
    return clients
}


type ClientMap struct {
    mutex sync.Mutex
    clientMap map[string]Client
}

func (m *ClientMap) GetClient(clientId string) Client {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    if c, ok := m.clientMap[clientId]; ok {
        return c
    } else {
        return nil
    }
}

func (m *ClientMap) SaveNewClient(clientId string, c Client) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    m.clientMap[clientId] = c
}

func (m *ClientMap) RemoveClient(clientId string) {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    delete(m.clientMap, clientId)
}