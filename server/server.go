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


package server

import "github.com/byte-mug/articledb/dbrpc"
import "github.com/valyala/fastrpc"
import "time"
import "net"

const ProtocolHeader  = "DB-RPC"
const ProtocolVersion = 0

func NewServer(h *dbrpc.Handler) *fastrpc.Server {
	srv := new(fastrpc.Server)
	
	srv.CompressType    = fastrpc.CompressNone
	srv.SniffHeader     = ProtocolHeader
	srv.ProtocolVersion = ProtocolVersion
	
	srv.NewHandlerCtx = h.Create
	srv.Handler = h.Handler
	srv.MaxBatchDelay = 10*time.Millisecond
	return srv
}

func NewClient(addr string) *dbrpc.Client {
	clt := new(fastrpc.Client)
	
	clt.CompressType    = fastrpc.CompressNone
	clt.SniffHeader     = ProtocolHeader
	clt.ProtocolVersion = ProtocolVersion
	
	clt.Addr = addr
	
	dc  := new(dbrpc.Client)
	dc.Client = clt
	return dc
}

func NewClientWithFunc(addr string, dial func(addr string) (net.Conn, error)) *dbrpc.Client {
	clt := new(fastrpc.Client)
	
	clt.CompressType    = fastrpc.CompressNone
	clt.SniffHeader     = ProtocolHeader
	clt.ProtocolVersion = ProtocolVersion
	
	clt.Addr = addr
	clt.Dial = dial
	
	dc  := new(dbrpc.Client)
	dc.Client = clt
	return dc
}



