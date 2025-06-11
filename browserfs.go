package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	lamerand "math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/gorilla/websocket"
)

type webHandler struct{}

type readrsp struct {
	data []byte
	err  error
}
type pendingtoken struct {
	authtoken string
	subdir    string
	filename  string
}
type readrq struct {
	offset  uint64
	length  uint64
	rspchan chan readrsp
	txid    uint32
	relfile *remfilent
}
type remfilent struct {
	filesize     uint64
	subdir       string
	filename     string
	reqchan      chan readrq
	pendingtxids map[uint32]bool
	closed       bool
}

var wsupgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { // we are being reverse proxied, no idea what origin should be, banking       onbrowser to enforce CORS, like most web applications
		return true
	},
}
var pendingiolock = sync.RWMutex{}
var pendingios = make(map[uint32]*readrq)
var pendingtoks = make(map[string]*pendingtoken)
var mappedfiles = make(map[string]map[string]*remfilent) // first is the browserfs generated token, then controller generated subdir and controller conveyed filename

func read_ws(c *websocket.Conn, recvchan chan []byte, closechan chan int) {
	for {
		_, rply, err := c.ReadMessage()
		if err != nil {
			//fmt.Println("Error receiving client message")
			//fmt.Println(err)
			closechan <- 1
			return
		}
		bbuf := bytes.NewBuffer(rply)
		var opcode uint32 = 0
		binary.Read(bbuf, binary.BigEndian, &opcode)
		if opcode == 2 {
			// Read response opcode
			var txid uint32
			binary.Read(bbuf, binary.BigEndian, &txid)
			pendingiolock.RLock()
			rrq, validtx := pendingios[txid]
			pendingiolock.RUnlock()
			if !validtx {
				fmt.Println("Invalid txid in response!")
				fmt.Println(txid)
				fmt.Println(pendingios)
				continue
			}
			pendingiolock.Lock()
			delete(rrq.relfile.pendingtxids, txid)
			delete(pendingios, txid)
			pendingiolock.Unlock()
			rrq.rspchan <- readrsp{
				err:  nil,
				data: rply[8:],
			}

		}
	}
}
func (w *webHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var current remfilent
	current = remfilent{}
	filemapped := false
	closechan := make(chan int)
	recvchan := make(chan []byte)
	pathparts := strings.Split(r.URL.Path, "/")
	c, err := wsupgrader.Upgrade(wr, r, nil)
	if err != nil {
		//fmt.Println(err)
		return
	}
	defer func() {
		//fmt.Println("Closing out connection")
		current.closed = true

		if filemapped {
			//fmt.Println("Closing file")
			pendingiolock.Lock()
			delete(mappedfiles[pathparts[1]], pathparts[2])
			delete(mappedfiles, pathparts[1])
			pendingiolock.Unlock()

			for txid := range current.pendingtxids {
				fmt.Println("should be sending disconnect to a pending request:", txid)
				pendingios[txid].rspchan <- readrsp{
					err:  errors.New("Client disconect"),
					data: nil,
				}
				pendingiolock.Lock()
				delete(pendingios, txid)
				pendingiolock.Unlock()

			}
		}
		c.Close()
	}()
	if len(pathparts) != 4 { // leading blank, our id, subdir, and filename
		fmt.Println("Unsupported URL")
		return
	}
	_, message, err := c.ReadMessage()
	pendtok, tokexists := pendingtoks[pathparts[1]]
	if !tokexists {
		fmt.Println("no token registered")
		return
	}
	authtoken := string(message)
	if authtoken != pendtok.authtoken {
		fmt.Println("bad auth")
		return
	}
	pendingiolock.Lock()
	delete(pendingtoks, pathparts[1])
	if _, keypresent := mappedfiles[pathparts[1]]; !keypresent {
		mappedfiles[pathparts[1]] = make(map[string]*remfilent)
	}
	pendingiolock.Unlock()
	current.closed = false
	current.pendingtxids = make(map[uint32]bool)
	current.filename = pathparts[3]
	current.subdir = pathparts[2]
	current.reqchan = make(chan readrq)
	filemapped = true
	_, message, err = c.ReadMessage()
	bbuf := bytes.NewBuffer(message)
	binary.Read(bbuf, binary.BigEndian, &current.filesize)
	//current.filesize, err = strconv.ParseUint(string(message), 10, 64)
	//TODO: current.filesize
	pendingiolock.Lock()
	mappedfiles[pathparts[1]][pathparts[2]] = &current
	pendingiolock.Unlock()
	c.WriteMessage(websocket.BinaryMessage, []byte("\x00\x00\x00\x00"))
	go read_ws(c, recvchan, closechan)
	for {
		select {
		case message := <-recvchan:
			c.WriteMessage(websocket.BinaryMessage, message) // Not to be used
		case <-closechan:
			return
		case rrq := <-mappedfiles[pathparts[1]][pathparts[2]].reqchan:
			outbox := new(bytes.Buffer)
			binary.Write(outbox, binary.BigEndian, uint32(1)) // read request operator
			binary.Write(outbox, binary.BigEndian, uint32(rrq.txid))
			binary.Write(outbox, binary.BigEndian, rrq.offset)
			binary.Write(outbox, binary.BigEndian, rrq.length)
			c.WriteMessage(websocket.BinaryMessage, outbox.Bytes())
		case <-time.After(time.Second * 30):
			outbox := new(bytes.Buffer)
			binary.Write(outbox, binary.BigEndian, uint32(3)) // NOOP
			c.WriteMessage(websocket.BinaryMessage, outbox.Bytes())
		}
	}
}

func listenWebsock(wp string, exitchan chan int) {
	sockhandler := new(webHandler)
	srv := &http.Server{
		Handler: sockhandler,
	}
	var err error
	var usock net.Listener
	if strings.Contains(wp, ":") {
		// Between uneven support of unix sockets in apache and SELinux pain,
		// perhaps TCP is the righht way after all
		usock, err = net.Listen("tcp", wp)
	} else {
		usock, err = net.Listen("unix", wp)
		os.Chmod(wp, 0o777)
	}
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
			if !alreadyexists {
				_, alreadyexists = mappedfiles[idstr]
			}
			if !alreadyexists {
				break
			}
		}
		authtok := make([]byte, 32)
		rand.Read(authtok)                                  // random auth token
		ptok.authtoken = hex.EncodeToString(authtok) + "\n" // newline for sake of manual debug being a bit easier
		pendingtoks[idstr] = &ptok
		conn.Write([]byte("\x00\x00\x00\x00")) // 0 for success
		binary.Write(conn, binary.BigEndian, uint32(len(idstr)))
		conn.Write([]byte(idstr))
		binary.Write(conn, binary.BigEndian, uint32(len(ptok.authtoken)))
		conn.Write([]byte(ptok.authtoken))
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
	mountdirPtr := flag.String("w", "", "Target Mount Directory")
	ctlSocketPath := flag.String("c", "", "Path to control socket")
	wsSocketPath := flag.String("s", "", "Path to websocket socket")
	flag.Parse()
	if *mountdirPtr == "" {
		fmt.Println("Must specify mount directory")
		os.Exit(1)
	}
	if *ctlSocketPath == "" {
		fmt.Println("Must specify control socket path")
		os.Exit(1)
	}
	if *wsSocketPath == "" {
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
		fuse.Unmount(*mountdirPtr)
		os.Exit(0)
	}()
	c, _ := fuse.Mount(*mountdirPtr, fuse.MaxReadahead(256*1024), fuse.FSName("browserfs"), fuse.ReadOnly(), fuse.AsyncRead())
	thefs := &browserfs{}
	fs.Serve(c, thefs)
}

type browserfs struct{}

func (browserfs) Root() (fs.Node, error) {
	n := &Dir{1, nil}
	return n, nil
}

type Dir struct {
	baseinode uint64
	pathparts []string
}

type File struct {
	pathparts []string
	filesize  uint64
	filent    *remfilent
	mydir     *Dir
}

func (s Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = s.baseinode
	a.Mode = os.ModeDir | 0o555
	return nil
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	//fmt.Println("attr called")
	if f.filent.closed {
		return fuse.Errno(syscall.ENOENT)
	}
	a.Mode = 0o444
	a.Size = f.filesize
	return nil
}

func (f File) Open(ctx context.Context, req *fuse.OpenRequest, rsp *fuse.OpenResponse) (fs.Handle, error) {
	if f.filent.closed {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	if !req.Flags.IsReadOnly() {
		return nil, fuse.Errno(syscall.EACCES)
	}
	return f, nil
}

func (f File) Read(ctx context.Context, req *fuse.ReadRequest, rsp *fuse.ReadResponse) error {
	var txid uint32 = 0
	var txidused bool = true
	if f.filent.closed {
		return fuse.Errno(syscall.ENOENT)
	}
	pendingiolock.RLock()
	for txidused == true {
		txid = uint32(lamerand.Int31n(2147483647))
		_, txidused = pendingios[txid]
	}
	pendingiolock.RUnlock()
	rrq := readrq{
		offset:  uint64(req.Offset),
		length:  uint64(req.Size),
		rspchan: make(chan readrsp),
		relfile: f.filent,
		txid:    txid,
	}
	pendingiolock.Lock()
	f.filent.pendingtxids[txid] = true
	pendingios[txid] = &rrq
	pendingiolock.Unlock()
	f.filent.reqchan <- rrq
	rspdata := <-rrq.rspchan
	if rspdata.err != nil {
		return fuse.Errno(syscall.ENOENT)
	}
	rsp.Data = rspdata.data
	return nil
}

func (s Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if len(s.pathparts) == 0 {
		_, present := mappedfiles[name]
		if !present {
			return nil, syscall.ENOENT
		}
		pp := make([]string, 1)
		pp[0] = name
		child := &Dir{
			pathparts: pp,
		}
		return child, nil
	}
	if len(s.pathparts) == 1 {
		_, present := mappedfiles[s.pathparts[0]]
		if !present {
			return nil, syscall.ENOENT
		}
		pp := make([]string, 2)
		pp[1] = name
		pp[0] = s.pathparts[0]
		child := &Dir{
			pathparts: pp,
		}
		return child, nil
	}
	if len(s.pathparts) == 2 {
		mf := mappedfiles[s.pathparts[0]][s.pathparts[1]]
		if mf.filename != name {
			return nil, syscall.ENOENT
		}
		child := &File{
			pathparts: s.pathparts,
			filesize:  mf.filesize,
			filent:    mf,
		}
		return child, nil
	}
	return nil, syscall.ENOENT
}

func (s Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var curridx uint64
	var subdirs []fuse.Dirent
	curridx = 0
	if len(s.pathparts) == 0 {
		subdirs = make([]fuse.Dirent, len(mappedfiles))
		var ci fuse.Dirent
		for k := range mappedfiles {
			ci = fuse.Dirent{
				Name: k,
				Type: fuse.DT_Dir,
			}
			subdirs[curridx] = ci
			curridx += 1
		}
	}
	if len(s.pathparts) == 1 {
		subdirs = make([]fuse.Dirent, 1)
		for k := range mappedfiles[s.pathparts[0]] {
			ci := fuse.Dirent{
				Name: k,
				Type: fuse.DT_Dir,
			}
			subdirs[0] = ci
		}
	}
	if len(s.pathparts) == 2 {
		subdirs = make([]fuse.Dirent, 1)
		mf := mappedfiles[s.pathparts[0]][s.pathparts[1]]
		ci := fuse.Dirent{
			Name: mf.filename,
			Type: fuse.DT_File,
		}
		subdirs[0] = ci
	}
	return subdirs, nil
}
