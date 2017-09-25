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

import "github.com/byte-mug/golibs/serializer"
import "github.com/byte-mug/articledb/groupsdb"
import "github.com/byte-mug/articledb/messagedb"
import "github.com/valyala/fastrpc"

type Boolean byte
func (b Boolean) Bool() bool { return b!=0 }
func (pb *Boolean) From(b bool) { if b { *pb=0xff }else{ *pb=0 } }
func ToBoolean(b bool) Boolean {
	if b { return 0xff }
	return 0
}

type BITS byte
const (
	BIT_HEAD BITS = 1<<iota
	BIT_BODY
)
func (b BITS) Has(flags BITS) bool {
	return flags==(b&flags)
}


// ----------- BEGIN IGrpArtDB ----------------------

type ReqPutArticle struct{
	Group []byte
	Number int64
	Posting *messagedb.ArticlePosting
}
var ce_ReqPutArticle = serializer.StripawayPtrWith(new(ReqPutArticle),serializer.WithInline(new(ReqPutArticle)).
	Field("Group").
	Field("Number").
	FieldWith("Posting",serializer.StripawayPtrWith(new(messagedb.ArticlePosting),messagedb.CeArticlePosting())))
//

type ReqGetArticle struct{
	Group []byte
	Number int64
	Bits   BITS
}
var ce_ReqGetArticle = serializer.StripawayPtrWith(new(ReqGetArticle),serializer.WithInline(new(ReqGetArticle)).
	Field("Group").
	Field("Number").
	Field("Bits"))
//

type ReqGetXover struct{
	Group []byte
	First  int64
	Last   int64
	Max    int
}
var ce_ReqGetXover = serializer.StripawayPtrWith(new(ReqGetXover),serializer.WithInline(new(ReqGetXover)).
	Field("Group").
	Field("First").
	Field("Last").
	Field("Max"))
//


// ----------- END IGrpArtDB ----------------------

// ----------- BEGIN IGroupNRT ----------------------
type ReqGetGroupNRT struct{
	Group []byte
}
var ce_ReqGetGroupNRT = serializer.StripawayPtrWith(new(ReqGetGroupNRT),serializer.WithInline(new(ReqGetGroupNRT)).
	Field("Group"))

type ReqGetGroupBulkNRT struct{
	Groups [][]byte
}
var ce_ReqGetGroupBulkNRT = serializer.StripawayPtrWith(new(ReqGetGroupBulkNRT),serializer.WithInline(new(ReqGetGroupBulkNRT)).
	Field("Groups"))
//

type ReqGetGroupsNRT struct{
	After, Prefix, Suffix []byte
}
var ce_ReqGetGroupsNRT = serializer.StripawayPtrWith(new(ReqGetGroupsNRT),serializer.WithInline(new(ReqGetGroupsNRT)).
	Field("After").
	Field("Prefix").
	Field("Suffix"))
//

type ReqPutGroupNRT struct{
	Group []byte
	Entry *groupsdb.GroupEntryNRT
}
var ce_ReqPutGroupNRT = serializer.StripawayPtrWith(new(ReqPutGroupNRT),serializer.WithInline(new(ReqPutGroupNRT)).
	Field("Group").
	FieldWith("Entry",groupsdb.CeGroupEntryNRT()))
//
// ----------- END IGrpArtDB ----------------------

// ----------- BEGIN IGroupNRT ----------------------
const (
	NRT_GetGroupRTP byte = iota
	NRT_IncrementRTP
	NRT_RollbackArticleRTP
)
type ReqGroupNRT struct{
	Cmd    byte
	Group  []byte
	Artnum int64
}
var ce_ReqGroupNRT = serializer.With(new(ReqGroupNRT)).
	Field("Cmd").
	Field("Group").
	Field("Artnum")
// ----------- End IGroupRTP ----------------------



var ce_RequestData = serializer.Switch(0).
	AddTypeWith(0x01,new(ReqPutArticle),ce_ReqPutArticle).
	AddTypeWith(0x02,new(ReqGetArticle),ce_ReqGetArticle).
	AddTypeWith(0x03,new(ReqGetXover),ce_ReqGetXover).
	
	AddTypeWith(0x21,new(ReqGetGroupNRT),ce_ReqGetGroupNRT).
	AddTypeWith(0x22,new(ReqGetGroupBulkNRT),ce_ReqGetGroupBulkNRT).
	AddTypeWith(0x23,new(ReqPutGroupNRT),ce_ReqPutGroupNRT).
	
	AddTypeWith(0x30,new(ReqGroupNRT),ce_ReqGroupNRT)
//


type Request struct{
	Data interface{}
}

var ce_Request = serializer.WithInline(&Request{}).
	FieldWith("Data",ce_RequestData)
//-----------------------------------------------


// ----------- BEGIN IGrpArtDB ----------------------

type RespPutArticle struct{
	Ok Boolean
}
var ce_RespPutArticle = serializer.StripawayPtrWith(new(RespPutArticle),serializer.WithInline(new(RespPutArticle)).
	Field("Ok"))
//

type RespGetArticle struct{
	HeadPtr messagedb.AbstractBlob
	BodyPtr messagedb.AbstractBlob
	Ok Boolean
}
var ce_RespGetArticle = serializer.StripawayPtrWith(new(RespGetArticle),serializer.WithInline(new(RespGetArticle)).
	FieldWith("HeadPtr",messagedb.CeAbstractBlob()).
	FieldWith("BodyPtr",messagedb.CeAbstractBlob()).
	Field("Ok"))
//

// ----------- END IGrpArtDB ----------------------

// ----------- BEGIN IGroupNRT ----------------------

type RespPutGroupNRT struct{
	Other  *groupsdb.GroupEntryNRT
	Ok     Boolean
}
var ce_RespPutGroupNRT = serializer.StripawayPtrWith(new(RespPutGroupNRT),serializer.WithInline(new(RespPutGroupNRT)).
	FieldWith("Other",groupsdb.CeGroupEntryNRT()).
	Field("Ok"))
// ----------- END IGroupNRT ----------------------

// ----------- BEGIN IGroupRTP ----------------------
type RespIncrementRTP struct{
	Artnum int64
	Ok Boolean
}
var ce_RespIncrementRTP = serializer.StripawayPtrWith(new(RespIncrementRTP),serializer.WithInline(new(RespIncrementRTP)).
	Field("Artnum").Field("Ok"))

type RespRollbackArticleRTP struct{
	Ok Boolean
}
var ce_RespRollbackArticleRTP = serializer.StripawayPtrWith(new(RespRollbackArticleRTP),serializer.WithInline(new(RespRollbackArticleRTP)).
	Field("Ok"))
// ----------- END IGroupRTP ----------------------


var ce_ResponseData = serializer.Switch(0).
	AddTypeWith          (0x01,new(RespPutArticle),ce_RespPutArticle).
	AddTypeWith          (0x02,new(RespGetArticle),ce_RespGetArticle).
	AddTypeContainerWithP(0x03,new([]messagedb.XoverElement),messagedb.CeArticleXoverStruct()).

	AddTypeWith          (0x21,new(groupsdb.GroupEntryNRT),groupsdb.CeGroupEntryNRT()).
	AddTypeContainerWith (0x22,[]groupsdb.GroupPairNRT{},groupsdb.CeGroupPairNRT()).
	AddTypeWith          (0x23,new(RespPutGroupNRT),ce_RespPutGroupNRT).

	AddTypeWith          (0x31,new(groupsdb.GroupEntryRTP),groupsdb.CeGroupEntryRTP()).
	AddTypeWith          (0x32,new(RespIncrementRTP),ce_RespIncrementRTP).
	AddTypeWith          (0x33,new(RespRollbackArticleRTP),ce_RespRollbackArticleRTP)
//


type Response struct{
	Data interface{}
}

var ce_Response = serializer.WithInline(&Response{}).
	FieldWith("Data",ce_ResponseData)
//-----------------------------------------------



type Handler struct{
	MessageDB messagedb.IGrpArtDB
	GroupsNRT groupsdb.IGroupNRT
	GroupsRTP groupsdb.IGroupRTP
}
func (h *Handler) Handler(ctx fastrpc.HandlerCtx) (ctx0 fastrpc.HandlerCtx) {
	ctx0 = ctx
	hctx := ctx.(*HandlerCtx)
	hctx.Resp.Data = nil
	
	//-----------------------------------------------
	switch v := hctx.Req.Data.(type) {
	// -----------  messagedb.IGrpArtDB -------------
	case *ReqPutArticle:
		if h.MessageDB==nil { return }
		hctx.Resp.Data = &RespPutArticle{
			ToBoolean(h.MessageDB.PutArticle(v.Group,v.Number,v.Posting)) }
	case *ReqGetArticle:
		if h.MessageDB==nil { return }
		headPtr,bodyPtr,ok := h.MessageDB.GetArticle(v.Group, v.Number, v.Bits.Has(BIT_HEAD), v.Bits.Has(BIT_BODY))
		hctx.Resp.Data = &RespGetArticle{headPtr,bodyPtr,ToBoolean(ok)}
	case *ReqGetXover:
		if h.MessageDB==nil { return }
		hctx.Resp.Data = h.MessageDB.GetXover(v.Group, v.First, v.Last, v.Max)
	
	// -----------  messagedb.IGroupNRT -------------
	case *ReqGetGroupNRT:
		if h.GroupsNRT==nil { return }
		hctx.Resp.Data = h.GroupsNRT.GetGroupNRT(v.Group)
	case *ReqGetGroupBulkNRT:
		if h.GroupsNRT==nil { return }
		hctx.Resp.Data = h.GroupsNRT.GetGroupBulkNRT(v.Groups)
	case *ReqGetGroupsNRT:
		if h.GroupsNRT==nil { return }
		hctx.Resp.Data = h.GroupsNRT.GetGroupsNRT( v.After,v.Prefix,v.Suffix )
	case *ReqPutGroupNRT:
		if h.GroupsNRT==nil || v.Entry==nil { return }
		grpnrte, ok := h.GroupsNRT.PutGroupNRT(v.Group, v.Entry)
		hctx.Resp.Data = &RespPutGroupNRT{ grpnrte, ToBoolean(ok) }
	
	// -----------  messagedb.IGroupRTP -------------
	case *ReqGroupNRT:
		if h.GroupsRTP==nil { return }
		switch v.Cmd {
		case NRT_GetGroupRTP:
			hctx.Resp.Data = h.GroupsRTP.GetGroupRTP(v.Group)
		case NRT_IncrementRTP:
			nartnum, ok := h.GroupsRTP.IncrementRTP(v.Group)
			hctx.Resp.Data = &RespIncrementRTP{ nartnum, ToBoolean(ok) }
		case NRT_RollbackArticleRTP:
			hctx.Resp.Data = &RespRollbackArticleRTP{
				ToBoolean(h.GroupsRTP.RollbackArticleRTP(v.Group,v.Artnum))}
		}
	}
	return
}


type Client struct{
	Client *fastrpc.Client
}

