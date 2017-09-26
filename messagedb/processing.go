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


package messagedb

import "github.com/pierrec/lz4"

func (c CompressionHint) Compress(b AbstractBlob) AbstractBlob {
	if c==CH_None || b==nil || !b.IsDirect() { return b }
	
	bd,ok := b.(*BlobDirect)
	if !ok { return b }
	
	l := len(bd.Content)
	if l>0x7E000000 || l==0 { return b }
	dest := make([]byte,lz4.CompressBlockBound(l))
	
	var i int
	var err error
	if c==CH_LZ4_HC {
		i,err = lz4.CompressBlockHC(bd.Content,dest,0)
	} else if c==CH_LZ4 {
		i,err = lz4.CompressBlock(bd.Content,dest,0)
	}
	
	if err!=nil || i==0 { return b }
	return &BlobLz4Compressed{l,dest[:i]}
}

