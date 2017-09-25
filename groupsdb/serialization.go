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


package groupsdb

import "github.com/byte-mug/golibs/preciseio"
import "github.com/byte-mug/golibs/serializer"
import "bytes"


func (e *GroupEntryNRT) Bytes() []byte {
	w := preciseio.PreciseWriterFromPool()
	defer w.PutToPool()
	buf := new(bytes.Buffer)
	w.W = buf
	err := serializer.Serialize(ce_GroupEntryNRT,w,e)
	if err!=nil { panic(err) }
	return buf.Bytes()
}
func (e *GroupEntryRTP) Bytes() []byte {
	w := preciseio.PreciseWriterFromPool()
	defer w.PutToPool()
	buf := new(bytes.Buffer)
	w.W = buf
	err := serializer.Serialize(ce_GroupEntryRTP,w,e)
	if err!=nil { panic(err) }
	return buf.Bytes()
}

func ParseGroupEntryNRT(b []byte) (*GroupEntryNRT,error){
	i,e := serializer.Deserialize(ce_GroupEntryNRT,preciseio.PreciseReader{bytes.NewReader(b)})
	g,_ := i.(*GroupEntryNRT)
	return g,e
}

func ParseGroupEntryRTP(b []byte) (*GroupEntryRTP,error){
	i,e := serializer.Deserialize(ce_GroupEntryRTP,preciseio.PreciseReader{bytes.NewReader(b)})
	g,_ := i.(*GroupEntryRTP)
	return g,e
}
func CeGroupEntryNRT() serializer.CodecElement { return ce_GroupEntryNRT }
func CeGroupEntryRTP() serializer.CodecElement { return ce_GroupEntryRTP }


