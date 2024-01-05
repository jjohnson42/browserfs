package main

import (
	"context"
	"os"
	"flag"
	"fmt"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"net"
	"net/http"
	"github.com/gorilla/websocket"
)

type webHandler struct {}

var wsupgrader = websocket.Upgrader{}

func (w *webHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	fmt.Println("My uncle")
	fmt.Println(r.URL.Path)

	c, err := wsupgrader.Upgrade(wr, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	msg := []byte("Wordy")
	c.WriteMessage(websocket.TextMessage, msg)
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
	go listenWebsock(*wsSocketPath, exitchan)
	_ = <- exitchan
	fmt.Println("Hello", *mountdirPtr)
	dialer := *websocket.DefaultDialer
	dialer.Dial("wss://localhost/conneectme", nil)
	c, _ := fuse.Mount("/tmp/c")
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
