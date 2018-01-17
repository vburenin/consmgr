package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/opencontainers/runc/libcontainer/utils"
	"github.com/urfave/cli"
	log "github.com/sirupsen/logrus"
)

type WriteBuffer struct {
	mu        sync.Mutex
	closed    bool
	writeChan chan []byte
	w         io.WriteCloser
}

func (wb *WriteBuffer) Write(data []byte) (int, error) {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if wb.closed {
		return 0, fmt.Errorf("buffer is closed")
	}

	select {
	case wb.writeChan <- data:
		break
	default:
		log.Debug("Buffer is full!!! Skipping.")
	}

	return len(data), nil
}

func (wb *WriteBuffer) Close() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if wb.closed {
		return
	}
	wb.closed = true
	close(wb.writeChan)
	wb.w.Close()
}

func (wb *WriteBuffer) IsClosed() bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	return wb.closed
}

func (wb *WriteBuffer) serveLoop() {
	for {
		data, ok := <-wb.writeChan
		if !ok {
			return
		}
		_, err := wb.w.Write(data)
		if err != nil {
			log.Debugf("WriteBuffer: %s", err)
			wb.Close()
			return
		}
	}
}

func NewWriteBuffer(w io.WriteCloser) *WriteBuffer {
	wb := &WriteBuffer{
		writeChan: make(chan []byte, 16384),
		w:         w,
	}
	go wb.serveLoop()
	return wb
}

type ContainerConsole struct {
	mu             sync.Mutex
	writeToBuffers []*WriteBuffer
	LogPath        string
	SockPath       string
	consFile       *os.File
}

func (cc *ContainerConsole) AddWriteBuffer(w io.WriteCloser) {
	cc.mu.Lock()
	cc.writeToBuffers = append(cc.writeToBuffers, NewWriteBuffer(w))
	cc.mu.Unlock()
}

func (cc *ContainerConsole) Write(data []byte) (int, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	rebuid := false
	for _, wb := range cc.writeToBuffers {
		_, err := wb.Write(data)
		if err != nil {
			rebuid = true
			wb.Close()
		}
	}
	if rebuid {
		writeToBuffers := make([]*WriteBuffer, 0, len(cc.writeToBuffers))
		for _, wb := range cc.writeToBuffers {
			if !wb.IsClosed() {
				writeToBuffers = append(writeToBuffers, wb)
			}
		}
		cc.writeToBuffers = writeToBuffers
	}
	log.Debugf("Write is succeeded for %d writers: %s",  len(cc.writeToBuffers), string(data))

	return len(data), nil
}

func (cc *ContainerConsole) AttachToConsole(conn net.Conn) error {
	defer conn.Close()
	cc.mu.Lock()
	cf := cc.consFile
	cc.mu.Unlock()
	if cf == nil {
		return fmt.Errorf("-ERR: container is not running")
	}

	cc.AddWriteBuffer(conn)
	buff := make([]byte, 4096)
	for {
		n, err := conn.Read(buff)
		if n > 0 {
			cf.Write(buff[:n])
		}
		if err != nil {
			return err
		}
	}
}

func (cc *ContainerConsole) StartServing() error {
	log.Infof("Start listening to: %s", cc.SockPath)
	ln, err := net.Listen("unix", cc.SockPath)
	if err != nil {
		return err
	}

	defer os.Remove(cc.SockPath)
	defer ln.Close()

	conn, err := ln.Accept()
	if err != nil {
		return err
	}

	defer conn.Close()

	// Get the fd of the connection.
	unixconn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("incorrect conn type: %s", cc.SockPath)
	}

	socket, err := unixconn.File()
	if err != nil {
		return fmt.Errorf("can't get file data: %s", cc.SockPath)
	}
	defer socket.Close()

	// Get the master file descriptor from runC.
	master, err := utils.RecvFd(socket)
	if err != nil {
		return fmt.Errorf("can't get file descriptor: %s", cc.SockPath)
	}

	log.Infof("Opening logfile: %s", cc.LogPath)
	logFile, err := os.OpenFile(cc.LogPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	if err != nil {
		log.Errorf("Could not open: %s", cc.LogPath)
		return err
	}

	log.Debugf("Adding log write buffer...")
	cc.AddWriteBuffer(logFile)

	cc.mu.Lock()
	cc.consFile = master
	cc.mu.Unlock()

	io.Copy(cc, master)
	/*buf := make([]byte, 4096)
	log.Debugf("Starting following...")
	for {
		n, err := master.Read(buf)
		log.Debugf("Received %d bytes of data", n)
		if n > 0 {
			log.Debugf("%s", string(buf[:n]))
		}
		if err != nil {
			log.Debugf("Error: %s", err)
			break
		}
	}*/

	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, wb := range cc.writeToBuffers {
		wb.Close()
	}
	return nil
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "fatal error: %v\n", err)
	os.Exit(1)
}

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
var activeServers = make(map[string]*ContainerConsole)
var mu sync.Mutex

func RandStringName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func RandName() string {
	return RandStringName(10)
}

func NewConsole(id string) (*ContainerConsole, error) {
	consPath := "/tmp/console-" + id + ".sock"
	logPath := "/tmp/console-" + id + ".log"
	ss, err := os.Stat(consPath)

	// console file should not exist
	if err == nil {
		if !ss.IsDir() {
			if err := os.Remove(consPath); err != nil {
				return nil, fmt.Errorf("-ERR: could not prepare socket %s: %s", consPath, err)
			}
		}
	}

	stats, err := os.Stat(logPath)
	if err == nil && stats.IsDir() {
		return nil, fmt.Errorf("-ERR: log path is a directory: %s", logPath)
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("-ERR: can't access log file: %s", logPath)
	}

	cc := &ContainerConsole{
		LogPath:  logPath,
		SockPath: consPath,
	}
	return cc, nil
}

func startNewConsole(id string, conn net.Conn) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := activeServers[id]; ok {
		log.Errorf("id is already in use: %s", id)
		return "", fmt.Errorf("-ERR: id is already in use")
	}
	cons, err := NewConsole(id)
	if err != nil {
		log.Errorf("Error happened: %s", err)
		return "", err
	}
	activeServers[id] = cons

	go func() {
		err := cons.StartServing()
		if err != nil {
			log.Errorf("Error while service socket: %s", err)
		}
	}()

	return cons.SockPath, nil
}

func runConn(conn net.Conn) error {
	defer conn.Close()
	_, err := conn.Write([]byte("+READY\n"))
	if err != nil {
		log.Errorf("Failed to send READY")
		return err
	}
	r := bufio.NewReader(conn)
	for {

		s, err := r.ReadString('\n')
		if err != nil {
			log.Errorf("Could not read line from the client: %s", err)
			return err
		}
		var parts []string
		for _, p := range strings.Split(s, " ") {
			if ts := strings.TrimSpace(p); ts != "" {
				parts = append(parts, ts)
			}
		}

		if len(parts) < 1 {
			continue
		}

		if parts[0] == "START" {
			sockID := ""
			if len(parts) > 1 {
				sockID = parts[1]
			} else {
				sockID = RandName()
			}
			resp, err := startNewConsole(sockID, conn)
			if err != nil {
				conn.Write([]byte(err.Error()))
			} else {
				conn.Write([]byte(resp))
			}
			_, err = conn.Write([]byte("\n"))
			return err
		}

		if parts[0] == "ATTACH" {
			if len(parts) != 2 {
				return fmt.Errorf("invalid number of arguments, attach expects 2")
			}

			cons, ok := activeServers[parts[1]]
			if !ok {
				return fmt.Errorf("unknown console id: %s", parts[1])
			}
			return cons.AttachToConsole(conn)
		}

		if parts[0] == "LIST" {
			mu.Lock()
			output := make([]string, 0, len(activeServers))

			for k, _ := range activeServers {
				output = append(output, k+"\n")
			}
			mu.Unlock()

			sort.Strings(output)
			output = append(output, "+DONE\n")
			for _, v := range output {
				_, err := conn.Write([]byte(v))
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

}

var runningFlag = true

func StartServer(ctx *cli.Context) error {
	wg := sync.WaitGroup{}
	unixSocket := ctx.String("socket")
	netSocket := ctx.String("net")

	var unixListener net.Listener
	var netListener net.Listener
	var err error

	if unixSocket != "" {
		unixListener, err = net.Listen("unix", unixSocket)
		if err != nil {
			return err
		}
	}

	if netSocket != "" {
		netListener, err = net.Listen("tcp", netSocket)
		if err != nil {
			return err
		}
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		<-sigchan
		runningFlag = false
		log.Infof("shutting down")

		if unixListener != nil {
			unixListener.Close()
			os.Remove(unixSocket)
		}
		if netListener != nil {
			netListener.Close()
		}
	}()

	if unixListener != nil {
		wg.Add(1)
		log.Infof("Listening on: %s", unixSocket)
		go func() {
			for runningFlag {
				conn, err := unixListener.Accept()
				if err != nil {
					if !runningFlag {
						break
					}
					log.Warnf("Could not accept connection: %s", err)
					continue
				}
				go runConn(conn)
			}
			wg.Done()
		}()
	}

	if netListener != nil {
		wg.Add(1)
		log.Infof("Listening on: %s", netSocket)
		go func() {
			for runningFlag {
				conn, err := netListener.Accept()
				if err != nil {
					if !runningFlag {
						break
					}
					log.Warnf("Could not accept connection: %s", err)
					continue
				}
				go runConn(conn)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

var serverCommand = cli.Command{
	Name:  "server",
	Usage: "Starts console socket management server",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "socket, s",
			Value: "/tmp/server-console.sock",
			Usage: "Attaches server to a local file socket at specified location",
		},
		cli.StringFlag{
			Name:  "net, -n",
			Value: "",
			Usage: "Attaches server to a network TCP socket",
		},
	},
	Action: func(ctx *cli.Context) error {
		go func() {
			log.Info(http.ListenAndServe("localhost:6060", nil))
		}()

		return StartServer(ctx)
	},
}

var attachCommand = cli.Command{
	Name:  "attach",
	Usage: "Attaches client to a specified container output",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "id, i",
			Value: "",
			Usage: "Container socket id",
		},
		cli.StringFlag{
			Name:  "socket,s",
			Value: "/tmp/server-console.sock",
			Usage: "Server unix socket",
		},
		cli.StringFlag{
			Name:  "net, -n",
			Value: "",
			Usage: "Attaches to a server over TCP protocol",
		},
	},
	Action: func(ctx *cli.Context) error {
		args := ctx.Args()
		if len(args) > 1 {
			return fmt.Errorf("only one attach argument can be specified")
		}
		if len(args) < 1 {
			return fmt.Errorf("attach needs a console id")
		}
		conn, err := runCmd(ctx, "ATTACH "+args[0])
		if err != nil {
			return err
		}

		defer conn.Close()
		go io.Copy(conn, os.Stdin)
		io.Copy(os.Stdout, conn)

		log.Infof("Connection has been closed")
		return nil
	},
}

func runCmd(ctx *cli.Context, cmd string) (net.Conn, error) {
	sockType := "tcp"
	netAddr := ctx.String("net")
	if netAddr == "" {
		sockType = "unix"
		netAddr = ctx.String("socket")
	}
	conn, err := net.Dial(sockType, netAddr)

	if err != nil {
		return nil, err
	}
	buf := bufio.NewReader(conn)
	s, err := buf.ReadString('\n')
	if !strings.HasPrefix(s, "+READY") {
		defer conn.Close()
		return nil, fmt.Errorf("invalid server hello string")
	}
	_, err = conn.Write([]byte(cmd + "\n"))
	if err != nil {
		defer conn.Close()
		return nil, err
	}
	return conn, nil
}

var socketCommand = cli.Command{
	Name:  "socket",
	Usage: "Create a new socket",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "socket,s",
			Value: "/tmp/server-console.sock",
			Usage: "Connects to a server unix socket",
		},
		cli.StringFlag{
			Name:  "net, -n",
			Value: "",
			Usage: "Connects to a server over TCP protocol",
		},
	},
	Action: func(ctx *cli.Context) error {
		args := ctx.Args()
		if len(args) != 1 {
			return fmt.Errorf("please specify new socket")
		} else {
			newSocketId := ctx.Args()[0]
			conn, err := runCmd(ctx, "START "+newSocketId)
			if err != nil {
				return err
			}
			defer conn.Close()
			buf := bufio.NewReader(conn)
			s, err := buf.ReadString('\n')
			if err != nil {
				return err
			}
			fmt.Print(s)
		}
		return nil
	},
}

var listCommand = cli.Command{
	Name:  "list",
	Usage: "Get a list of available socket ids",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "socket,s",
			Value: "/tmp/server-console.sock",
			Usage: "Server unix socket",
		},
		cli.StringFlag{
			Name:  "net, -n",
			Value: "",
			Usage: "Attaches to a server over TCP protocol",
		},
	},
	Action: func(ctx *cli.Context) error {
		conn, err := runCmd(ctx, "LIST")
		if err != nil {
			return err
		}
		defer conn.Close()
		buff := bufio.NewReader(conn)
		for {
			s, err := buff.ReadString('\n')
			if err != nil {
				return err
			}
			if strings.HasPrefix(s, "+DONE") {
				return nil
			}
			fmt.Print(s)
		}
	},
}

func main() {
	log.SetLevel(log.DebugLevel)

	app := cli.NewApp()
	app.Name = "consmgr"
	app.Usage = "A server/client tool to manage runc console sockets"
	app.Version = "0.0.1"
	app.Commands = []cli.Command{
		serverCommand,
		listCommand,
		attachCommand,
		socketCommand,
	}

	if err := app.Run(os.Args); err != nil {
		fail(err)
	}

}
