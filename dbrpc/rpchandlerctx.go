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
//import "github.com/byte-mug/golibs/serializer"
import "github.com/valyala/fasthttp"
import "net"
import "bufio"
import "reflect"

type HandlerCtx struct{
	Req  Request
	Resp Response
}

func (h *HandlerCtx) ConcurrencyLimitError(concurrency int) {
	h.Resp.Data = nil
}

func (h *HandlerCtx) Init(conn net.Conn, logger fasthttp.Logger) {
	h.Req.Data = nil
	h.Resp.Data = nil
}

func (h *HandlerCtx) ReadRequest(br *bufio.Reader) error {
	return ce_Request.Read(preciseio.PreciseReader{br},reflect.ValueOf(h).Elem().Field(0))
}

func (h *HandlerCtx) WriteResponse(bw *bufio.Writer) error {
	w := preciseio.PreciseWriterFromPool()
	defer w.PutToPool()
	w.W = bw
	return ce_Response.Write(w,reflect.ValueOf(h).Elem().Field(1))
}

func (r *Request) WriteRequest(bw *bufio.Writer) error {
	w := preciseio.PreciseWriterFromPool()
	defer w.PutToPool()
	w.W = bw
	return ce_Request.Write(w,reflect.ValueOf(r).Elem())
}

func (r *Response) ReadResponse(br *bufio.Reader) error {
	return ce_Response.Read(preciseio.PreciseReader{br},reflect.ValueOf(r).Elem())
}

