package persistence

import (
    "fmt"
    "time"
    "database/sql"
    "bytes"
    "strconv"
    
    _ "github.com/go-sql-driver/mysql"
    
    . "mqtttype"
)

type mysqlPersistence struct {
    db *sql.DB
}

func newMysqlPersistence() persistence {
    p := new(mysqlPersistence)
    var err error
    p.db, err = sql.Open("mysql", "root:zhangjie@/mqtt")
    if err != nil {
        panic("init mysql persistence failed, reason:" + err.Error())
    }
    return p
}

func (p *mysqlPersistence) SaveClientConnection(clientId string, nodeId string, t time.Time) (insertId int64) {
    stmt, err := p.db.Prepare("INSERT INTO connection (client_id, node_id, time) VALUES( ?, ?, ? )")
    if err != nil {
        panic("SaveClientConnection failed, reason:" + err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec(clientId, nodeId, t)
    if err != nil {
        panic("SaveClientConnection failed, reason:" + err.Error())
    }
    n, err := result.LastInsertId()
    if err != nil {
        panic("SaveClientConnection failed, reason:" + err.Error())
    }
    fmt.Println("SaveClientConnection insert id:", n)
    return n
}

func (p *mysqlPersistence) SaveClientSubscribe(clientId string, info []*MqttSubscribePacket) {
    //t := time.Now()
    buffer := bytes.NewBufferString("INSERT INTO subscribe (client_id, topic, qos, time) VALUES")
    first := true
    for _, value := range info {
        if !first {
            buffer.WriteString(",")
        } else {
            first = false
        }
        buffer.WriteString("('")
        buffer.WriteString(clientId)
        buffer.WriteString("','")
        buffer.WriteString(value.Topic)
        buffer.WriteString("',")
        buffer.WriteString(strconv.Itoa(value.Qos))
        buffer.WriteString(",")
        buffer.WriteString("NOW(6)")
        buffer.WriteString(")")
    }
    fmt.Println("SaveClientSubscribe sql:", buffer.String())

    stmt, err := p.db.Prepare(buffer.String())
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec()
    if err != nil {
        panic(err.Error())
    }
    n, err := result.LastInsertId()
    if err != nil {
        panic(err.Error())
    }
    fmt.Println("SaveClientSubscribe insert id:", n)
    return
}

func (p *mysqlPersistence) RemoveAllSubscribe(clientId string) (info []*MqttSubscribePacket) {
    rows, err := p.db.Query("SELECT topic, qos from subscribe WHERE client_id = ?", clientId)
    if err != nil {
        panic(err.Error())
    }
    defer rows.Close()
    info = make([]*MqttSubscribePacket, 0)
    for rows.Next() {
        var topic string
        var qos int
        if err = rows.Scan(&topic, &qos); err != nil {
            panic(err.Error())
        }
        info = append(info, &MqttSubscribePacket{topic, qos})
    }
    if err = rows.Err(); err != nil {
        panic(err.Error())
    }

    stmt, err := p.db.Prepare("DELETE FROM subscribe WHERE client_id = ?")
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec(clientId)
    if err != nil {
        panic(err.Error())
    }
    n, err := result.RowsAffected()
    if err != nil {
        panic(err.Error())
    }
    fmt.Println("RemoveAllSubscribe removed rows:", n)
    return info
}

func (p *mysqlPersistence) GetAllSubscribe(clientId string) (info []*MqttSubscribePacket) {
    rows, err := p.db.Query("SELECT topic, qos from subscribe WHERE client_id = ?", clientId)
    if err != nil {
        panic(err.Error())
    }
    defer rows.Close()
    info = make([]*MqttSubscribePacket, 0)
    for rows.Next() {
        var topic string
        var qos int
        if err = rows.Scan(&topic, &qos); err != nil {
            panic(err.Error())
        }
        info = append(info, &MqttSubscribePacket{topic, qos})
    }
    if err = rows.Err(); err != nil {
        panic(err.Error())
    }

    return info
}
