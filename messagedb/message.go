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

import "github.com/byte-mug/golibs/serializer"
import "github.com/nu7hatch/gouuid"

func cloneb(i []byte) (j []byte) {
	j = make([]byte,len(i))
	copy(j,i)
	return
}

// Funcs
func encode64(num int64) []byte{
	b := make([]byte,8)
	for i := 7; i>=0 ; i-- {
		b[i] = byte(num&0xff)
		num>>=8
	}
	return b
}
func decode64(buf []byte) (r int64) {
	for _,b := range buf {
		r = (r<<8)|int64(b)
	}
	return
}

type CompressionHint byte
const (
	CH_None   CompressionHint = 0
	CH_LZ4    CompressionHint = 'z'
	CH_LZ4_HC CompressionHint = 'H'
)
func (c CompressionHint) UseLz4() bool {
	switch c {
	case CH_LZ4,CH_LZ4_HC: return true
	}
	return false
}
func (c CompressionHint) Lz4UseHC() bool { return c==CH_LZ4_HC }


type ArticleXover struct {
	Subject []byte
	From    []byte
	Date    []byte
	MsgId   []byte
	Refs    []byte
	Bytes   int64
	Lines   int64
	
	// This timestamp is used for purging old Entries.
	TimeStamp int64 // Timestamp (UNIX-Format).
}

func CeArticleXover() serializer.CodecElement { return ce_ArticleXover }
func CeArticleXoverStruct() serializer.CodecElement { return ce_ArticleXoverStruct }

var ce_ArticleXover = serializer.With(&ArticleXover{}).
	Field("Subject").
	Field("From").
	Field("Date").
	Field("MsgId").
	Field("Refs").
	Field("Bytes").
	Field("Lines")

var ce_ArticleXoverStruct = serializer.WithInline(&ArticleXover{}).
	Field("Subject").
	Field("From").
	Field("Date").
	Field("MsgId").
	Field("Refs").
	Field("Bytes").
	Field("Lines")
//-----------------------------------------------



type ArticleRedirect struct {
	Group  []byte
	Number int64
}

func CeArticleRedirectPtr() serializer.CodecElement { return ce_ArticleRedirectPtr }
var ce_ArticleRedirectPtr = serializer.With(&ArticleRedirect{}).
	Field("Group").
	Field("Number")

var ce_ArticleRedirect = serializer.WithInline(&ArticleRedirect{}).
	Field("Group").
	Field("Number")
//-----------------------------------------------



type XoverElement struct{
	Number int64
	Xover ArticleXover
}

func CeXoverElement() serializer.CodecElement { return ce_XoverElement }
var ce_XoverElement = serializer.WithInline(&XoverElement{}).
	Field("Number").
	FieldWith("Xover",ce_ArticleXoverStruct)
//-----------------------------------------------



type AbstractBlob interface{
	IsDirect() bool
}

type BlobDirect struct{ Content []byte }
func (b *BlobDirect) IsDirect() bool { return true }

var ce_BlobDirect = serializer.StripawayPtrWith(new(BlobDirect),
	serializer.WithInline(new(BlobDirect)).Field("Content") )
//

type BlobLz4Compressed struct{
	UCLen int
	Lz4Content []byte
}
func (b *BlobLz4Compressed) IsDirect() bool { return true }

var ce_BlobLz4Compressed = serializer.StripawayPtrWith(new(BlobLz4Compressed),
	serializer.WithInline(new(BlobLz4Compressed)).Field("UCLen").Field("Lz4Content") )
//


type BlobLocation struct{
	Node *uuid.UUID
	DayID int
	Offset int64
	Length int64
}
func (b *BlobLocation) IsDirect() bool { return false }

var ce_BlobLocation = serializer.StripawayPtrWith(new(BlobLocation),
	serializer.WithInline(new(BlobLocation)).
	FieldWith("Node",serializer.StripawayPtr(new(uuid.UUID))).
	Field("DayID").
	Field("Offset").
	Field("Length") )
//


func CeAbstractBlob() serializer.CodecElement { return ce_AbstractBlob }

var ce_AbstractBlob = serializer.Switch(0).
	AddTypeWith('b',new(BlobDirect),ce_BlobDirect).
	AddTypeWith('C',new(BlobLz4Compressed),ce_BlobLz4Compressed).
	AddTypeWith('L',new(BlobLocation),ce_BlobLocation)
//-----------------------------------------------


type ArticleLocation struct {
	Head  AbstractBlob
	Body  AbstractBlob
}

var ce_ArticleLocation = serializer.WithInline(new(ArticleLocation)).
	FieldWith("Head",ce_AbstractBlob).
	FieldWith("Body",ce_AbstractBlob)
var ce_ArticleLocationPtr = serializer.StripawayPtrWith(new(ArticleLocation),ce_ArticleLocation)
//-----------------------------------------------

type ArticlePosting struct {
	Xover    ArticleXover
	Redir    *ArticleRedirect
	Head     AbstractBlob
	Body     AbstractBlob
	
	// Compression hints.
	HeadComp CompressionHint
	BodyComp CompressionHint
}
var ce_ArticlePosting = serializer.WithInline(new(ArticlePosting)).
	FieldWith("Xover",ce_ArticleXoverStruct).
	FieldWith("Redir",ce_ArticleRedirectPtr).
	Field("HeadComp").
	Field("BodyComp").
	FieldWith("Head",ce_AbstractBlob).
	FieldWith("Body",ce_AbstractBlob)
//-----------------------------------------------

func CeArticlePosting() serializer.CodecElement { return ce_ArticlePosting }


