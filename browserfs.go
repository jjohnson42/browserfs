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

	"github.com/gorilla/websocket"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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
		if len(rply) < 4 {
			continue
		}
		opcode := binary.BigEndian.Uint32(rply[:4])
		if opcode == 2 {
			// Read response opcode
			if len(rply) < 8 {
				continue
			}
			txid := binary.BigEndian.Uint32(rply[4:8])
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
	if len(message) >= 8 {
		current.filesize = binary.BigEndian.Uint64(message[:8])
	}
	//current.filesize, err = strconv.ParseUint(string(message), 10, 64)
	//TODO: current.filesize
	pendingiolock.Lock()
	mappedfiles[pathparts[1]][pathparts[2]] = &current
	pendingiolock.Unlock()
	c.WriteMessage(websocket.BinaryMessage, []byte("\x00\x00\x00\x00"))
	go read_ws(c, recvchan, closechan)
	keepalive := time.NewTimer(30 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case message := <-recvchan:
			c.WriteMessage(websocket.BinaryMessage, message) // Not to be used
		case <-closechan:
			return
		case rrq := <-mappedfiles[pathparts[1]][pathparts[2]].reqchan:
			var outbox [24]byte
			binary.BigEndian.PutUint32(outbox[0:4], 1) // read request operator
			binary.BigEndian.PutUint32(outbox[4:8], rrq.txid)
			binary.BigEndian.PutUint64(outbox[8:16], rrq.offset)
			binary.BigEndian.PutUint64(outbox[16:24], rrq.length)
			c.WriteMessage(websocket.BinaryMessage, outbox[:])
		case <-keepalive.C:
			var outbox [4]byte
			binary.BigEndian.PutUint32(outbox[:], 3) // NOOP
			c.WriteMessage(websocket.BinaryMessage, outbox[:])
		}
		if !keepalive.Stop() {
			select {
			case <-keepalive.C:
			default:
			}
		}
		keepalive.Reset(30 * time.Second)
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
	root := &Dir{}
	opts := &fs.Options{}
	opts.MountOptions.Name = "browserfs"
	opts.MountOptions.FsName = "browserfs"
	opts.MountOptions.MaxReadAhead = 1024 * 1024
	opts.MountOptions.MaxWrite = 1024 * 1024
	opts.MountOptions.Options = append(opts.MountOptions.Options, "ro")
	server, err := fs.Mount(*mountdirPtr, root, opts)
	if err != nil {
		panic(err)
	}
	go func() {
		<-rmchan
		os.Remove(*ctlSocketPath)
		os.Remove(*wsSocketPath)
		server.Unmount()
		os.Exit(0)
	}()
	server.Wait()
}

type Dir struct {
	fs.Inode
	pathparts []string
}

type File struct {
	fs.Inode
	pathparts []string
	filesize  uint64
	filent    *remfilent
	mydir     *Dir
}

var _ = (fs.NodeGetattrer)((*Dir)(nil))
var _ = (fs.NodeLookuper)((*Dir)(nil))
var _ = (fs.NodeReaddirer)((*Dir)(nil))

var _ = (fs.NodeGetattrer)((*File)(nil))
var _ = (fs.NodeOpener)((*File)(nil))
var _ = (fs.NodeReader)((*File)(nil))

func (s *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0o555
	return 0
}

func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	//fmt.Println("attr called")
	if f.filent.closed {
		return syscall.ENOENT
	}
	out.Mode = fuse.S_IFREG | 0o444
	out.Size = f.filesize
	return 0
}

func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if f.filent.closed {
		return nil, 0, syscall.ENOENT
	}
	if int(flags)&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.EACCES
	}
	return nil, 0, 0
}

func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	var txid uint32 = 0
	var txidused bool = true
	if f.filent.closed {
		return nil, syscall.ENOENT
	}
	pendingiolock.RLock()
	for txidused == true {
		txid = uint32(lamerand.Int31n(2147483647))
		_, txidused = pendingios[txid]
	}
	pendingiolock.RUnlock()
	rrq := readrq{
		offset:  uint64(off),
		length:  uint64(len(dest)),
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
		return nil, syscall.ENOENT
	}
	return fuse.ReadResultData(rspdata.data), 0
}

func (s *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
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
		return s.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
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
		return s.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR}), 0
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
		return s.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG}), 0
	}
	return nil, syscall.ENOENT
}

func (s *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	var entries []fuse.DirEntry
	if len(s.pathparts) == 0 {
		for k := range mappedfiles {
			entries = append(entries, fuse.DirEntry{
				Name: k,
				Mode: fuse.S_IFDIR,
			})
		}
	}
	if len(s.pathparts) == 1 {
		for k := range mappedfiles[s.pathparts[0]] {
			entries = append(entries, fuse.DirEntry{
				Name: k,
				Mode: fuse.S_IFDIR,
			})
		}
	}
	if len(s.pathparts) == 2 {
		mf := mappedfiles[s.pathparts[0]][s.pathparts[1]]
		entries = append(entries, fuse.DirEntry{
			Name: mf.filename,
			Mode: fuse.S_IFREG,
		})
	}
	return fs.NewListDirStream(entries), 0
}
