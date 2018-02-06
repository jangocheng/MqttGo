package zjlog

import (
    "fmt"
    "log"
    "bufio"
    "os"
    "io"
    "time"
)

type writer struct {
    writer *bufio.Writer
    timer *time.Ticker
}

func newLogWriter(w *bufio.Writer) io.Writer {
    lw := &writer{w, time.NewTicker(5e9)}
    go lw.flush()
    return lw
}

func (w *writer) Write(p []byte) (n int, err error) {
    n, err = w.writer.Write(p)
    return
}

func (w *writer) flush() {
    <- w.timer.C
    w.writer.Flush()
}

type logData struct {
    f *os.File
    w *bufio.Writer
}

var l *logData

func NewLog(logFileName string) (err error) {
    log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
    outputFile, outputError := os.OpenFile(logFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
    if outputError != nil {
        err = fmt.Errorf("Failed to open file:%s, reason:%v", logFileName, outputError)
        return
    }

	outputWriter := bufio.NewWriter(outputFile)
    
    log.SetOutput(newLogWriter(outputWriter))

    l = &logData{outputFile, outputWriter}
    return nil
}

func Log() *logData {
    return l
}

func (l *logData) Close() {
    if l.w != nil {
        l.w.Flush()
        l.w = nil
    }
    if l.f != nil {
        l.f.Close()
        l.f = nil
    }
    l = nil
}

func (l *logData) Debug(v ...interface{}) {
    log.Print("[DEBUG] ", v)
}

func (l *logData) Info(v ...interface{}) {
    log.Print("[INFO] ", v)
}

func (l *logData) Warn(v ...interface{}) {
    log.Print("[WARN] ", v)
}

func (l *logData) Error(v ...interface{}) {
    log.Print("[ERROR] ", v)
}
