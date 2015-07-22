package main

import (
	"encoding/gob"
	"encoding/json"
	//"errors"
	"github.com/goibibo/peernetwork/cache"
	"fmt"
	"log"
	"github.com/goibibo/peernetwork/message"
	"net"
	"os"
	"strconv"
	//"strings"
	"reflect"
	"sync"
	"time"
)

type Acknowledgement struct {
	Status string
}

type PeerOperations interface {
	add(addr string)

	remove(addr string)

	list() []chan<- *message.Message
}

type BusSocketOperations interface {
	DialPeer(addr string)

	Listen(addr string)

	SendMsg(msg string)

	Close()

	GetValue(key string)
}

const SUCCESS = "SUCCESS"
const STALEDATA_ERROR = "STALEDATA"
const DEFAULT_TTL = 24 * 60 * 60
const SET = "SET"
const DELETE = "DELETE"
const CHAN_BUFFER_SIZE = 10000

type acknowledgement struct {
	Status string
}

type DialerConnection struct { /* Connection consists of channel corresponding to the channel and the connection object */
	channel  chan *message.Message
	conn     *(net.Conn)
	is_alive bool
}

type ListenerConnection struct {
	channel chan *message.Message
	conn *(net.Conn)
	is_alive bool
	cache *(cache.Cache)
}

type PeerListener struct { /* It consists of a list of peer addresses (to which the node has dialed ) mapped to the connection object */
	m  map[string]*ListenerConnection /* Note: Remove the connection from this map before closing it */
	mu sync.RWMutex
}
type PeerSender struct { /* It consists of a list of peer addresses (peers who have dialed to this node) mapped to the connection object */
	m  map[string]*DialerConnection
	mu sync.RWMutex
}

type BusSocket struct {
	listeners     *PeerListener         //peers listening to the BusSocket
	senders       *PeerSender           //peers sending on the BusSocket
	receiver_chan chan *message.Message // channel through which node receives messages from its peers
	sender_chan   chan *message.Message // channel through which node sends message to its peers
	address       string                // ip address of the node
	mu            sync.RWMutex          //  lock for the channel
	is_active     bool                  // If node is listening or dialing , it is considered active
	cached        *(cache.Cache)        // Each node has a storage associated with it
	Listener      net.Listener
}

func die(format string, v ...interface{}) {
	/* Kills nodes when a critical error occurs */
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, v...))
	os.Exit(1)
}

func handlePanic() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()
}

func (sock *BusSocket) CreateListener(addr string) (err error) {
	/* Listens on port specified by user */
	handlePanic()
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		sock.is_active = false
		die("Cannot create a listener on port: ", addr)
	}
	sock.address = addr
	sock.listen(listener)
	return err
}

func (sock *BusSocket) listen(listener net.Listener) {
	sock.Listener = listener
	sock.is_active = true
	log.Println("Listening on port: ", sock.address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println(err)
		}
		go sock.senders.add(conn.RemoteAddr().String(), &conn)
		go sock.serve(conn)
	}
}

func NewBusSocket(nodeName string) (sock *BusSocket) {
	/* Creates a new bus socket */
	handlePanic()
	cached, err := cache.New(nodeName)
	if err != nil {
		panic("Error occured while creating cache for node");
	}
	sock = &BusSocket{
		listeners:     &PeerListener{m: make(map[string]*ListenerConnection)},
		senders:       &PeerSender{m: make(map[string]*DialerConnection)},
		receiver_chan: make(chan *message.Message),
		sender_chan:   make(chan *message.Message),
		is_active:     false,
		address:       "",
		cached:        cached,
	}
	go sock.updatepeers() // Call updatepeers to start listening on sender_chan
	return sock
}

func (sock *BusSocket) evaluatemessage(message *message.Message) (err error, localmsg *message.Message) {
	/* Evaluate message sent by peers */
	handlePanic()
	action := message.Action
	if action == SET {
		err, localmsg = sock.cached.SetData(message, message.Ttl)
		if err != nil {
			fmt.Printf("Cannot add key-value pair ( '%s', '%s') to  database: %s\n", message.Key, message.Value, err)
		}
	} else if action == DELETE {
		err := sock.cached.DeleteData(message.Key)
		if err != nil {
			fmt.Printf("Cannot delete key '%s': %s\n", message.Key, err)
		}
	} else {
		fmt.Println("Cannot decode message! Test again\n")
	}
	return
}

func (p *PeerSender) add(addr string, con *(net.Conn)) chan *message.Message {
	/* Adds a new peer to the list */
	handlePanic()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.m[addr]; ok {
		return nil
	}
	ch := make(chan *message.Message, CHAN_BUFFER_SIZE)
	conn := &DialerConnection{
		channel:  ch,
		conn:     con,
		is_alive: true,
	}
	p.m[addr] = conn
	return ch
}

func (p *PeerListener) remove(addr string) (err error) {
	/* Removes a peer from the list */
	handlePanic()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.m[addr]; ok {
		(p.m[addr]).is_alive = false
	}
	return
}

func (p *PeerListener) list() [] ListenerConnection {
	/* Returns a list of channels associated with peer listeners  */
	handlePanic()
	p.mu.RLock()
	defer p.mu.RUnlock()
	l := make([] ListenerConnection, 0, len(p.m))
	for _, connection := range p.m {
		l = append(l, *connection)
	}
	return l
}

func (p *PeerListener) add(addr string, con *(net.Conn)) chan *message.Message {
	/* Adds a new peer sender */
	handlePanic()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.m[addr]; ok {
		return nil
	}
	ch := make(chan *message.Message, CHAN_BUFFER_SIZE)
	cache_no :=0
	var err error
	var storage *cache.Cache
	for {
		storage, err = cache.New(addr+ "_" + strconv.Itoa(cache_no))
		if (err == nil){
			break
		}
		cache_no +=1
	}
	conn := &ListenerConnection{
		channel:  ch,
		conn:     con,
		is_alive: true,
		cache:    storage,
	}
	p.m[addr]= conn
	return ch
}

func (p *PeerSender) remove(addr string) (err error) {
	/* Removes peer from list of peers */
	handlePanic()
	p.mu.Lock()
	defer p.mu.Unlock()
	err = (*p.m[addr].conn).Close()
	if err != nil {
		fmt.Printf("Cannot remove sender: %s", err)
		return
	}
	delete(p.m, addr)
	return
}

func (p *PeerSender) list() []DialerConnection {
	/* returns list of channels associated with PeerSenders */
	handlePanic()
	p.mu.RLock()
	defer p.mu.RUnlock()
	l := make([]DialerConnection, 0, len(p.m))
	for _, connection := range p.m {
		l = append(l, *connection)
	}
	return l
}

func (sock *BusSocket) sendtopeers(m *message.Message) {
	/* sends messages to peers */
	handlePanic()
	for _,connection := range sock.listeners.list() {
		select {
		case (connection).channel <- m:
		default:
			var value []byte
			msg := <-(connection).channel
			key := msg.Key
			timestamp := msg.Timestamp
			value, err := json.Marshal(msg)
			if err == nil {
				action := SET
				var ttl int64
				ttl = 24*60*60*7
				Message := *message.NewMessage(action, key, string(value), ttl)
				Message.Timestamp = timestamp
				err,_ := (connection).cache.SetData(&Message, ttl)
				if err != nil {
					log.Printf("Stale data cannot be set in peer cache! Ignoring this message")
				}
				(connection).channel <- m
			}
		}
	}
}

func (sock *BusSocket) updatepeers() {
	/* listens on the sender channel for any message from its peers */
	handlePanic()
	sender_down := false
	for {
		if sender_down == true {
			return
		}
		select {
		case send_input, ok := <-sock.sender_chan:
			if !ok {
				sender_down = true
			} else {
				sock.sendtopeers(send_input)
			}
		}
	}
}

func (sock *BusSocket) serve(conn net.Conn) {
	/* Listens to peer who has dialed to it */
	handlePanic()
	fmt.Println("Received New connection!")
	encoder := gob.NewEncoder(conn)
	d := gob.NewDecoder(conn)
	for {
		var msg *message.Message
		err := d.Decode(&msg)
		if err != nil {
			log.Printf("Cannot decode message: %s", err)
			sock.senders.remove(conn.RemoteAddr().String())
			conn.Close()
			return
		}
		ack := Acknowledgement{Status: SUCCESS}
		var msglocal *message.Message
		err, msglocal = sock.evaluatemessage(msg)
		if err != nil && err.Error() == STALEDATA_ERROR {
			ack.Status = STALEDATA_ERROR
			sock.sendtopeers(msglocal)
		}
		encoderr := encoder.Encode(ack)
		if encoderr != nil {
			log.Printf("Acknowledgement not sent! %s", encoderr)
			sock.senders.remove(conn.RemoteAddr().String())
			conn.Close()
		}
		fmt.Println("%#v Received message", msg)
		//sock.receiver_chan <- msg
	}
}

func (sock *BusSocket) SendMsg(action, key, value string, ttl_optional ...int64) {
	/* Sends messages specified by the user */
	handlePanic()
	var ttl int64
	ttl = 24 * 60 * 60
	if len(ttl_optional) > 0 {
		ttl = ttl_optional[0]
	}
	msg := message.NewMessage(action, key, value, ttl)
	sock.sender_chan <- msg
}

func (sock *BusSocket) GetValue(key string) (value string, err error) {
	/* Gets value from peer nodes */
	handlePanic()
	value, err = sock.cached.GetData(key)
	if err != nil {
		fmt.Printf("Value not found!")
	}
	return
}

func (sock *BusSocket) DialPeer(addr string) {
	/* Dials to peer */
	time.Sleep(12 * time.Second)
	handlePanic()
	fmt.Println("adding address\n", addr)
	if addr == sock.address {
		return
	}
	(sock.listeners).mu.Lock()
	if _, ok := (sock.listeners).m[addr]; ok {
		(sock.listeners).mu.Unlock()
		return
	}
	(sock.listeners).mu.Unlock()
	var conn (net.Conn)
	var err error
	for {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			log.Println(addr, err)
		} else {
			break
		}
		time.Sleep(15 * time.Minute) /* wait for 15 minutes before you redial */
	}
	(sock.listeners).m[addr].is_alive = true
	sock.writetopeer(addr, conn)
}

func (sock *BusSocket) writegobtopeers(conn net.Conn, addr string) {
	encoder := gob.NewEncoder(conn)
	connection := sock.listeners.m[addr]
	messages, err := connection.cache.GetAll()
	if err != nil || len(messages) == 0 {
		return
	}
	for _, Msg := range messages {
		var msg *message.Message
		err = json.Unmarshal([]byte(Msg),&msg)
		if err != nil {
			continue
		}
		err := encoder.Encode(msg)
		if err != nil {
			log.Println(addr, err)
			sock.listeners.remove(addr)
			conn.Close()
			log.Printf("Error occured while encoding! : %s", err)
			go sock.redial(addr) /* we need to redial here, because when the other node comes up, it would connect with this node on remote address and it won't be able to recognize which peer node it is, to dial back */
			return
		}
		var ack Acknowledgement
		dec := gob.NewDecoder(conn)
		err = dec.Decode(&ack)
		if err != nil {
			log.Printf("error writing to tcp conn: %s", err)
			sock.listeners.remove(addr)
			conn.Close()
		}
		if ack.Status == SUCCESS {
			continue
		} else if ack.Status == STALEDATA_ERROR {
			log.Printf("stale data error: %s, %s \n", addr, msg.Key)
		}
	}
}

func (sock *BusSocket) writetopeer(addr string, conn net.Conn) {
	/* check if node has sent any messages on the peer's channel, if it has, then write the message on the connection between them*/
	ch := sock.listeners.add(addr, &conn)
	if ch == nil {
		fmt.Println("cannot add address to the list of listeners\n", addr)
	}
	sock.writegobtopeers(conn, addr)
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	for m := range ch {
		err := encoder.Encode(m)
		if err != nil {
			log.Println(addr, err)
			sock.listeners.remove(addr)
			conn.Close()
			log.Printf("Error occured while encoding! : %s", err)
			go sock.redial(addr) /* we need to redial here, because when the other node comes up, it would connect with this node on remote address and it won't be able to recognize which peer node it is, to dial back */
			return
		}
		var ack Acknowledgement
		err = decoder.Decode(&ack)
		if err != nil {
			log.Printf("error writing to tcp conn: %s, %s \n", addr, err)
			sock.listeners.remove(addr)
			conn.Close()
		}
		if ack.Status == SUCCESS {
			continue
		} else if ack.Status == STALEDATA_ERROR {
			log.Printf("stale data error: %s, %s \n", addr, m.Key)
		}

	}
}

func (sock *BusSocket) redial(addr string) {
	time.Sleep(15 * time.Minute)
	sock.DialPeer(addr)
}

func (sock *BusSocket) Close() {
	handlePanic()
	for address, connection := range sock.listeners.m {
		delete(sock.listeners.m, address)
		(*connection.conn).Close()
	}
	for address, connection := range sock.senders.m {
		delete(sock.senders.m, address)
		(*connection.conn).Close()
	}
	fmt.Println("All connections to BusSocket closed!\n")
	clear(&sock.listeners)
	clear(&sock.senders)
	sock.Listener.Close()
	clear(&sock)
}

func clear(v interface{}) {
	handlePanic()
	/* clears the values of the fields of given instance */
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

func (sock *BusSocket) test() {
	time.Sleep(18 * time.Second)
	handlePanic()
	start := time.Now()

	for i := 0; i < 10000; i++ {
		number := strconv.Itoa(i)
		for i := 0; i < 100; i++ {
			go sock.SendMsg("SET", "keys"+number, ",values"+number, 86400)
		}
	}
	elapsed := time.Since(start)
	log.Printf("Total time %s", elapsed)
}

func main() {
	handlePanic()
	sock := NewBusSocket(os.Args[1])
	go sock.DialPeer(os.Args[3])
	go sock.test()
	sock.CreateListener(os.Args[2])
}
