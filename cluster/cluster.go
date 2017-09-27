/*
MIT License

Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
Experimental clustering for Dayfile. Really experimental. Not recommended for use.
*/
package cluster

import "github.com/vmihailenco/msgpack"
import "github.com/nu7hatch/gouuid"
//import "github.com/pierrec/lz4"
//import "encoding/binary"
import "github.com/hashicorp/memberlist"
import "github.com/byte-mug/articledb/server"
import "github.com/byte-mug/articledb/dbrpc"
import "github.com/byte-mug/articledb/messagedb"
import "sync"
import "net"

func netListen(addr string) (net.Listener,error) {
	return net.Listen("tcp",addr)
}
func netDial(addr string) (net.Conn,error) {
	return net.Dial("tcp",addr)
}

var dials = map[string]func(addr string) (net.Conn,error) {
	"tcp": netDial,
}
var listens = map[string]func(addr string) (net.Listener,error) {
	"tcp": netListen,
}

func AddDial(n string,f func(addr string) (net.Conn,error)) { dials[n]=f }
func AddListen(n string,f func(addr string) (net.Listener,error)) { listens[n]=f }

type NodeInfo struct{
	_msgpack struct{} `msgpack:",omitempty"`
	RpcPort int
	RpcProtocol string // Should be "tcp"
	DayfileNode *DayfileNode
}

type DayfileNode struct{
	_msgpack struct{} `msgpack:",asArray"`
	ID []byte // Must be 16 bytes.
}

type NodeObject struct{
	Info   *NodeInfo
	Client *dbrpc.Client
}


type NodeFacade struct{
	LocalName string
	LocalNode *NodeInfo
	Handler   *dbrpc.Handler
	Nodes     map[string]*NodeObject
	Dayfile   map[uuid.UUID]*NodeObject
	mutex sync.Mutex
	
	dfid  *uuid.UUID
	backup    [2]dbrpc.Handler
}

func (n *NodeFacade) NFInitialize() {
	n.LocalNode.DayfileNode = nil
	if n.Handler.DayfileDB!=nil {
		isFirst  := n.backup[0].DayfileDB != nil
		isSecond := n.backup[1].DayfileDB != nil
		if !(isFirst && isSecond) {
			n.backup[0].DayfileDB = n.Handler.DayfileDB
			
			n.dfid = n.backup[0].DayfileDB.GetDayfileNodeID()
			n.LocalNode.DayfileNode = &DayfileNode{ID:n.dfid[:]}
			n.Handler.DayfileDB = nfDayfileNode{n.Handler.DayfileDB,n}
			
			n.backup[1].DayfileDB = n.Handler.DayfileDB
		}
	}
}

// Delegate
func (n *NodeFacade) NodeMeta(limit int) []byte {
	b,e := msgpack.Marshal(n.LocalNode)
	if e!=nil { return nil } // Error!
	if len(b)>limit { return nil } // Metadata too big!
	return b
}
func (n *NodeFacade) NotifyMsg([]byte) {}
func (n *NodeFacade) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (n *NodeFacade) LocalState(join bool) []byte { return nil }
func (n *NodeFacade) MergeRemoteState(buf []byte, join bool) { return }


// EventDelegate
func (n *NodeFacade) insert(e *memberlist.Node, ni *NodeInfo) {
	if e.Name==n.LocalName { return }
	node := new(NodeObject)
	node.Info = ni
	
	fu,ok := dials[ni.RpcProtocol]
	if !ok { return }
	
	ta := net.TCPAddr{IP:e.Addr,Port:ni.RpcPort}
	node.Client = server.NewClientWithFunc(ta.String(),fu)
	
	n.Nodes[e.Name] = node
	if node.Info.DayfileNode!=nil {
		var id uuid.UUID
		copy(id[:],node.Info.DayfileNode.ID)
		n.Dayfile[id] = node
	}
}
func (n *NodeFacade) remove(e *memberlist.Node) {
	node,ok := n.Nodes[e.Name]
	if !ok { return }
	delete(n.Nodes,e.Name)
	if node.Info.DayfileNode!=nil {
		var id uuid.UUID
		copy(id[:],node.Info.DayfileNode.ID)
		delete(n.Dayfile,id)
	}
}
func (n *NodeFacade) NotifyJoin(e *memberlist.Node) {
	n.mutex.Lock(); defer n.mutex.Unlock()
	ni := new(NodeInfo)
	if msgpack.Unmarshal(e.Meta)!=nil { return }
	n.insert(e,ni)
}
func (n *NodeFacade) NotifyUpdate(e *memberlist.Node) {
	n.mutex.Lock(); defer n.mutex.Unlock()
	ni := new(NodeInfo)
	if msgpack.Unmarshal(e.Meta)!=nil { return }
	n.remove(e)
	n.insert(e,ni)
}
func (n *NodeFacade) NotifyLeave(e *memberlist.Node) {
	n.mutex.Lock(); defer n.mutex.Unlock()
	n.remove(e)
}

type nfDayfileNode struct{
	messagedb.IDayfileNode
	node *NodeFacade
}
func (n nfDayfileNode) ReadDayfileBlob(b messagedb.AbstractBlob) messagedb.AbstractBlob {
	if b==nil || b.IsDirect() { return b }
	bl,ok := b.(*messagedb.BlobLocation)
	if !ok || bl==nil { return nil }
	if *(bl.Node) == *(n.node.dfid) {
		return n.node.backup[0].DayfileDB.ReadDayfileBlob(b)
	}
	on,ok := n.node.Dayfile[*(bl.Node)]
	if !ok { return nil }
	return on.Client.ReadDayfileBlob(b)
}



