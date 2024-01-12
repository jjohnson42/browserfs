package main

import (
    "crypto/rand"
    "encoding/binary"
    "encoding/hex"
    "bytes"
    "context"
    "os"
    "flag"
    "fmt"
    "bazil.org/fuse"
    "bazil.org/fuse/fs"
    "net"
    "net/http"
    "os/signal"
    "strings"
    "github.com/gorilla/websocket"
    "syscall"
)

type webHandler struct {}

type pendingtoken struct {
    authtoken string
    subdir string
    filename string
}
type readrq struct {
    offset uint64
    length uint64
}
type remfilent struct {
    filesize int
    subdir string
    filename string
    reqchan chan readrq
    rspchan chan []byte
}



var wsupgrader = websocket.Upgrader{}
var pendingtoks = make(map[string]pendingtoken)
var mappedfiles = make(map[string]map[string]remfilent) // first is the browserfs generated token, then controller generated subdir and controller conveyed filename

func read_ws(c *websocket.Conn, recvchan chan []byte, closechan chan int) {
    for {
        _, message, err := c.ReadMessage()
        if err != nil {
            closechan <- 1
            return
        }
        recvchan <- message
    }
}
func (w *webHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
    closechan := make(chan int)
    recvchan := make(chan []byte)
    c, err := wsupgrader.Upgrade(wr, r, nil)
    if err != nil {
        return
    }
    defer c.Close()
    pathparts := strings.Split(r.URL.Path, "/")
    if len(pathparts) != 4 { // leading blank, our id, subdir, and filename
        return
    }
    _, message, err := c.ReadMessage()
    pendtok, tokexists := pendingtoks[pathparts[1]]
    if ! tokexists {
        fmt.Println("no token registered")
        return
    }
    authtoken := string(message)
    fmt.Println(authtoken)
    fmt.Println(pendtok.authtoken)
    fmt.Println(pendtok.subdir)
    fmt.Println(pendtok)
    if authtoken != pendtok.authtoken {
        fmt.Println("bad auth")
        return
    }
    delete(pendingtoks, pathparts[1])
    if _, keypresent := mappedfiles[pathparts[1]]; !keypresent {
        mappedfiles[pathparts[1]] = make(map[string]remfilent)
    }
    mappedfiles[pathparts[1]][pathparts[2]] = remfilent{}
    current := remfilent{}
    current.filename = pathparts[3]
    current.subdir = pathparts[2]
    current.reqchan = make(chan readrq)
    current.rspchan = make(chan []byte)
    //TODO: current.filesize
    mappedfiles[pathparts[1]][pathparts[2]] = current
    c.WriteMessage(websocket.BinaryMessage, []byte("\x00\x00\x00\x00"))
    go read_ws(c, recvchan, closechan)
    for {
        select {
            case message := <- recvchan:
                c.WriteMessage(websocket.BinaryMessage, message)  // Not to be used
            case <- closechan:
                return
            case readrq := <- mappedfiles[pathparts[1]][pathparts[2]].reqchan:
                outbox := new(bytes.Buffer)
                binary.Write(outbox, binary.BigEndian, uint32(1)) // read request operator
                binary.Write(outbox, binary.BigEndian, readrq.offset)
                binary.Write(outbox, binary.BigEndian, readrq.length)
                c.WriteMessage(websocket.BinaryMessage, outbox.Bytes())
                _, rply, err := c.ReadMessage()
                if err != nil {
                    fmt.Println(err)
                    return
                }
                mappedfiles[pathparts[1]][pathparts[2]].rspchan <- rply
        }
    }
}

func listenWebsock(wp string, exitchan chan int) {
    sockhandler := new(webHandler)
    srv := &http.Server {
        Handler: sockhandler,
    }
    //usock, err := net.Listen("unix", wp)
    usock, err := net.Listen("tcp", ":8080")
    if err != nil {
        panic(err)
    }
    srv.Serve(usock)
    exitchan <- 0
}

func handlectlreq(conn net.Conn) {
    // Handle a Control socket connection
    // Client is expected to do an operation (32 bit integer)
    // 1 means register new handle
    // all other values are reserved
    defer func() {
        r := recover()
        if r != nil {
            fmt.Println("Recovered from bad connection: ", r)
        }
        conn.Close()
    }()
    var num uint32
    cmdbuf := make([]byte, 2048)
    n, err := conn.Read(cmdbuf)
    if err != nil {
        panic(err)
    }
    strm := bytes.NewBuffer(cmdbuf[:n])
    binary.Read(strm, binary.BigEndian, &num) // read opcode
    fmt.Println(num)
    if num == 1 {
        ptok := pendingtoken{}
        err := binary.Read(strm, binary.BigEndian, &num) // length of subdir
        if err != nil {
            panic(err)
        }
        subdir := make([]byte, num)
        _, err = strm.Read(subdir)
        if err != nil {
            panic(err)
        }
        ptok.subdir = string(subdir)
        err = binary.Read(strm, binary.BigEndian, &num) // length of filename
        if err != nil {
            panic(err)
        }
        fname := make([]byte, num)
        _, err = strm.Read(fname)
        if err != nil {
            panic(err)
        }
        ptok.filename = string(fname)
        fileid := make([]byte, 16)
        var idstr string
        for {
            rand.Read(fileid) // randomly generate a fileid
            idstr = hex.EncodeToString(fileid)
            _, alreadyexists := pendingtoks[idstr]
            if ! alreadyexists {
                _, alreadyexists = mappedfiles[idstr]
            }
            if ! alreadyexists {
                break
            }
        }
        authtok := make([]byte, 32)
        rand.Read(authtok) // random auth token
        ptok.authtoken = hex.EncodeToString(authtok) + "\n" // newline for sake of manual debug being a bit easier
        pendingtoks[idstr] = ptok
        fmt.Println(idstr)
        conn.Write([]byte("\x00\x00\x00\x00")) // 0 for success
        binary.Write(conn, binary.BigEndian, uint32(len(idstr)))
        conn.Write([]byte(idstr))
        binary.Write(conn, binary.BigEndian, uint32(len(ptok.authtoken)))
        fmt.Println(ptok.authtoken)
        conn.Write([]byte(ptok.authtoken))
        fmt.Println(pendingtoks)
    }
}

func listencontrol(ctlpath string) {
    csock, err := net.Listen("unix", ctlpath)
    if err != nil {
        panic(err)
    }
    for {
        conn, err := csock.Accept()
        if err != nil {
            fmt.Println(err)
            continue
        }
        go handlectlreq(conn)
    }
}

func main() {
    exitchan := make(chan int)
    mountdirPtr := flag.String("w", "", "Target Working Directory")
    ctlSocketPath := flag.String("c", "", "Path to control socket")
    wsSocketPath := flag.String("s", "", "Path to websocket socket")
    flag.Parse()
    if (*mountdirPtr == "") {
        fmt.Println("Must specify directory")
        os.Exit(1)
    }
    if (*ctlSocketPath == "") {
        fmt.Println("Must specify control socket path")
        os.Exit(1)
    }
    if (*wsSocketPath == "") {
        fmt.Println("Must specify websocket socket path")
        os.Exit(1)
    }
    go listencontrol(*ctlSocketPath)
    go listenWebsock(*wsSocketPath, exitchan)
    rmchan := make(chan os.Signal, 1)
    signal.Notify(rmchan, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-rmchan
        os.Remove(*ctlSocketPath)
        os.Remove(*wsSocketPath)
        os.Exit(0)
    }()
    c, _ := fuse.Mount(*mountdirPtr)
    fs.Serve(c, browserfs{})
}

type browserfs struct{}

func (browserfs) Root() (fs.Node, error) {
    return Dir{}, nil
}

type Dir struct{}

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
    a.Inode = 1
    a.Mode = os.ModeDir | 0o555
    return nil
}
