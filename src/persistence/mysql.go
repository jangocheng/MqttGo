package persistence

import (
    "log"
    "time"
    "database/sql"
    "bytes"
    "strconv"
    
    _ "github.com/go-sql-driver/mysql"
    . "command"
)

type mysqlPersistence struct {
    db *sql.DB
}

func newMysqlPersistence() persistence {
    p := new(mysqlPersistence)

    var err error
    p.db, err = sql.Open("mysql", "<mysql user>:<password>@tcp(<mysql ip>:<mysql port>)/mqtt")
    if err != nil {
        panic("init mysql persistence failed, reason:" + err.Error())
    }
    p.db.SetMaxOpenConns(100)
    p.db.SetMaxIdleConns(50)

    return p
}

func (p *mysqlPersistence) SaveClientConnection(clientId string, nodeId string, t time.Time) (insertId int64) {
    stmt, err := p.db.Prepare("INSERT INTO connection (client_id, node_id, time) VALUES( ?, ?, ? )")
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec(clientId, nodeId, t)
    if err != nil {
        panic(err.Error())
    }
    n, err := result.LastInsertId()
    if err != nil {
        panic(err.Error())
    }
    log.Print("SaveClientConnection insert id:", n)
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
        buffer.WriteString(",NOW(6))")
    }
    buffer.WriteString(" ON DUPLICATE KEY UPDATE qos=VALUES(qos), time=NOW(6)")
    log.Print("SaveClientSubscribe sql:", buffer.String())

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
    log.Print("SaveClientSubscribe insert id:", n)
    return
}

func (p *mysqlPersistence) RemoveAllSubscribe(clientId string) (topics []string) {
    rows, err := p.db.Query("SELECT topic from subscribe WHERE client_id = ?", clientId)
    if err != nil {
        panic(err.Error())
    }
    defer rows.Close()

    topics = make([]string, 0, 32)

    for rows.Next() {
        var topic string
        if err = rows.Scan(&topic); err != nil {
            panic(err.Error())
        }
        topics = append(topics, topic)
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
    log.Print("RemoveAllSubscribe removed rows:", n)
    return
}

func (p *mysqlPersistence) RemoveClientSubscribe(clientId string, topics []string) int64 {
    var rows int64 = 0
    for _, topic := range topics {
        stmt, err := p.db.Prepare("DELETE FROM subscribe WHERE client_id = ? AND topic = ?")
        if err != nil {
            panic(err.Error())
        }
        
        result, err := stmt.Exec(clientId, topic)
        if err != nil {
            stmt.Close()
            panic(err.Error())
        }
        n, err := result.RowsAffected()
        if err != nil {
            stmt.Close()
            panic(err.Error())
        }
        rows += n
        
        stmt.Close()
    }
    log.Print("RemoveAllSubscribe removed rows:", rows)
    return rows
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

func (p *mysqlPersistence) SaveMessage(clientId string, topic string, msg []byte, packetId int) int64 {
    stmt, err := p.db.Prepare("INSERT INTO message (topic, message, packet_id, client_id, time) VALUES( ?, ?, ?, ?, ? )")
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec(topic, msg, packetId, clientId, time.Now())
    if err != nil {
        panic(err.Error())
    }
    n, err := result.LastInsertId()
    if err != nil {
        panic(err.Error())
    }
    return n
}

func (p *mysqlPersistence) SaveClientMessage(clientId string, msgId int64, qos int) {
    stmt, err := p.db.Prepare("INSERT INTO msg_list (client_id, message_id, qos, time) VALUES( ?, ?, ?, ? )")
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec(clientId, msgId, qos, time.Now())
    if err != nil {
        panic(err.Error())
    }
    _, err = result.LastInsertId()
    if err != nil {
        panic(err.Error())
    }
    return
}

func (p *mysqlPersistence) RemoveClientMessage(clientId string, msgId int64) int64 {
    stmt, err := p.db.Prepare("DELETE FROM msg_list WHERE client_id = ? AND message_id = ?")
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()
    
    result, err := stmt.Exec(clientId, msgId)
    if err != nil {
        panic(err.Error())
    }
    n, err := result.RowsAffected()
    if err != nil {
        panic(err.Error())
    }
    log.Print("RemoveClientMessage removed rows:", n)
    return n
}

func (p *mysqlPersistence) getMessageContent(array []*MqttCacheMessage) []*MqttCacheMessage {
    for _, msg := range array {
        rows, err := p.db.Query("SELECT topic, message FROM message WHERE id = ?", msg.Id)
        if err != nil {
            panic(err.Error())
        }

        for rows.Next() {
            var message []byte
            var topic string
            if err = rows.Scan(&topic, &message); err != nil {
                rows.Close()
                panic(err.Error())
            }
            msg.Topic = topic
            msg.Message = message
        }
        rows.Close()
    }
    return array
}

func (p *mysqlPersistence) GetClientMessage(clientId string) []*MqttCacheMessage {
    rows, err := p.db.Query("SELECT message_id, qos FROM msg_list WHERE client_id = ? ORDER BY time", clientId)
    if err != nil {
        panic(err.Error())
    }
    msgArrayCloseFlag := false
    defer func() {
        if !msgArrayCloseFlag {
            rows.Close()
        }
    }()
    
    array := make([]*MqttCacheMessage, 0)
    for rows.Next() {
        var id int64
        var qos int
        if err = rows.Scan(&id, &qos); err != nil {
            panic(err.Error())
        }
        array = append(array, &MqttCacheMessage{Id : id, Qos : qos})
    }
    rows.Close()
    msgArrayCloseFlag = true
    
    return p.getMessageContent(array)
}

func (p *mysqlPersistence) GetQos2Message(clientId string, packetId int) (id int64, topic string, message []byte) {
    rows, err := p.db.Query("SELECT id, topic, message FROM message WHERE packet_id = ? AND client_id = ?", packetId, clientId)
    if err != nil {
        panic(err.Error())
    }
    defer rows.Close()

    for rows.Next() {
        if err = rows.Scan(&id, &topic, &message); err != nil {
            panic(err.Error())
        }
    }
    
    return
}

func (p *mysqlPersistence) UpdateQos2MessageStatus(clientId string, packetId int) int64 {
    stmt, err := p.db.Prepare("UPDATE message m LEFT JOIN msg_list l ON m.id = l.message_id SET l.status = 1 WHERE m.packet_id = ? AND l.client_id = ?")
    if err != nil {
        panic(err.Error())
    }
    defer stmt.Close()

    result, err := stmt.Exec(packetId, clientId)
    if err != nil {
        panic(err.Error())
    }
    n, err := result.RowsAffected()
    if err != nil {
        panic(err.Error())
    }
    log.Print("UpdateQos2MessageStatus modified rows:", n)
    return n
}

