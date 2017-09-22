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
package dbrpc

import "github.com/byte-mug/golibs/preciseio"
import "github.com/byte-mug/golibs/serializer"
import "github.com/valyala/fasthttp"
import "net"
import "bufio"
import "sync"

var pool_Writer = sync.Pool{ New: func() interface{} {
	s := new(preciseio.PreciseWriter)
	s.Initialize()
	return s
}}
func pool_Writer_Put(w *preciseio.PreciseWriter) {
	w.W = nil
	pool_Writer.Put(w)
}

type HandlerCtx struct{
	Req  *Request
	Resp *Response
}

func (h *HandlerCtx) ConcurrencyLimitError(concurrency int) {
	h.Resp = nil
}

func (h *HandlerCtx) Init(conn net.Conn, logger fasthttp.Logger) { h.Req = nil }

func (h *HandlerCtx) ReadRequest(br *bufio.Reader) error {
	r,e := serializer.Deserialize(ce_Request,preciseio.PreciseReader{br})
	h.Req,_ = r.(*Request)
	return e
}

func (h *HandlerCtx) WriteResponse(bw *bufio.Writer) error {
	w := pool_Writer.Get().(*preciseio.PreciseWriter)
	defer pool_Writer_Put(w)
	w.W = bw
	return serializer.Serialize(ce_Response,w,h.Resp)
}

func (r *Request) WriteRequest(bw *bufio.Writer) error {
	w := pool_Writer.Get().(*preciseio.PreciseWriter)
	defer pool_Writer_Put(w)
	w.W = bw
	return serializer.Serialize(ce_Request,w,r)
}

func (r *Response) ReadResponse(br *bufio.Reader) error {
	rr,e := serializer.Deserialize(ce_Response,preciseio.PreciseReader{br})
	rp,ok := rr.(*Response)
	if ok && rp!=nil { *r = *rp } else { *r = Response{} }
	return e
}

