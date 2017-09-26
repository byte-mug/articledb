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

import "github.com/nu7hatch/gouuid"
import "github.com/byte-mug/articledb/timeconst"
import "github.com/byte-mug/golibs/serializer"
import "github.com/byte-mug/articledb/groupsdb"
import "github.com/byte-mug/articledb/messagedb"
import "github.com/valyala/fastrpc"
import "time"

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
func (b BITS) Set(flags BITS,data bool) BITS {
	if data { return b|flags }
	return b&^flags
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

// ----------- BEGIN IDayfileNode ----------------------

const (
	DF_NodeID       byte = iota
	DF_FreeStorage
)
type ReqDayfileNodeInfo struct{
	Cmd byte
}
var ce_ReqDayfileNodeInfo = serializer.StripawayPtrWith(new(ReqDayfileNodeInfo),serializer.WithInline(new(ReqDayfileNodeInfo)).
	Field("Cmd"))
//

type ReqAddDayfileBlob struct{
	DayID int
	Comp  messagedb.CompressionHint
	Data  messagedb.AbstractBlob
}
var ce_ReqAddDayfileBlob = serializer.StripawayPtrWith(new(ReqAddDayfileBlob),serializer.WithInline(new(ReqAddDayfileBlob)).
	Field("DayID").
	Field("Comp").
	FieldWith("Data",messagedb.CeAbstractBlob()))
//

type ReqReadDayfileBlob struct{
	Data  messagedb.AbstractBlob
}
var ce_ReqReadDayfileBlob = serializer.StripawayPtrWith(new(ReqReadDayfileBlob),serializer.WithInline(new(ReqReadDayfileBlob)).
	FieldWith("Data",messagedb.CeAbstractBlob()))
//

// ----------- END IDayfileNode ----------------------

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
	RTP_GetGroupRTP         byte = iota
	RTP_IncrementRTP
	RTP_RollbackArticleRTP
)
type ReqGroupRTP struct{
	Cmd    byte
	Group  []byte
	Artnum int64
}
var ce_ReqGroupRTP = serializer.With(new(ReqGroupRTP)).
	Field("Cmd").
	Field("Group").
	Field("Artnum")
// ----------- End IGroupRTP ----------------------

// ----------- BEGIN IMsgidIndexDB ----------------------

type ReqGetMessageLocation struct{
	MessageID []byte
}
var ce_ReqGetMessageLocation = serializer.StripawayPtrWith(new(ReqGetMessageLocation),serializer.WithInline(new(ReqGetMessageLocation)).
	Field("MessageID"))
//

type ReqUpdateMessageLocation struct{
	MessageID []byte
	ArticlePos *messagedb.ArticleRedirect
	Timestamp int64
}
var ce_ReqUpdateMessageLocation = serializer.StripawayPtrWith(new(ReqUpdateMessageLocation),serializer.WithInline(new(ReqUpdateMessageLocation)).
	Field("MessageID").
	FieldWith("ArticlePos",messagedb.CeArticleRedirectPtr()).
	Field("Timestamp"))
//

// ----------- END IMsgidIndexDB ----------------------



var ce_RequestData = serializer.Switch(0).
	AddTypeWith(0x01,new(ReqPutArticle),ce_ReqPutArticle).
	AddTypeWith(0x02,new(ReqGetArticle),ce_ReqGetArticle).
	AddTypeWith(0x03,new(ReqGetXover  ),ce_ReqGetXover).

	AddTypeWith(0x11,new(ReqDayfileNodeInfo),ce_ReqDayfileNodeInfo).
	AddTypeWith(0x12,new(ReqAddDayfileBlob),ce_ReqAddDayfileBlob).
	AddTypeWith(0x13,new(ReqReadDayfileBlob),ce_ReqReadDayfileBlob).

	AddTypeWith(0x21,new(ReqGetGroupNRT),ce_ReqGetGroupNRT).
	AddTypeWith(0x22,new(ReqGetGroupBulkNRT),ce_ReqGetGroupBulkNRT).
	AddTypeWith(0x23,new(ReqPutGroupNRT),ce_ReqPutGroupNRT).
	AddTypeWith(0x24,new(ReqGetGroupsNRT),ce_ReqGetGroupsNRT).

	AddTypeWith(0x30,new(ReqGroupRTP),ce_ReqGroupRTP).

	AddTypeWith(0x41,new(ReqGetMessageLocation),ce_ReqGetMessageLocation).
	AddTypeWith(0x42,new(ReqUpdateMessageLocation),ce_ReqUpdateMessageLocation)
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

// ----------- BEGIN IDayfileNode ----------------------
type RespFreeDayfileStorage struct{
	FreeStorage int64
}
var ce_RespFreeDayfileStorage = serializer.StripawayPtrWith(new(RespFreeDayfileStorage),serializer.WithInline(new(RespFreeDayfileStorage)).
	Field("FreeStorage"))
//

type RespDayfileBlob struct{
	Data  messagedb.AbstractBlob
}
var ce_RespDayfileBlob = serializer.StripawayPtrWith(new(RespDayfileBlob),serializer.WithInline(new(RespDayfileBlob)).
	FieldWith("Data",messagedb.CeAbstractBlob()))
//

// ----------- END IDayfileNode ----------------------

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
	AddTypeContainerWithP(0x03,new([]messagedb.XoverElement),messagedb.CeXoverElement()).

	AddTypeWith          (0x11,new(RespFreeDayfileStorage),ce_RespFreeDayfileStorage).
	AddTypeWith          (0x12,new(RespDayfileBlob),ce_RespDayfileBlob).
	AddTypeWith          (0x13,new(uuid.UUID),serializer.StripawayPtr(new(uuid.UUID))).

	AddTypeWith          (0x21,new(groupsdb.GroupEntryNRT),groupsdb.CeGroupEntryNRT()).
	AddTypeContainerWith (0x22,[]groupsdb.GroupPairNRT{},groupsdb.CeGroupPairNRT()).
	AddTypeWith          (0x23,new(RespPutGroupNRT),ce_RespPutGroupNRT).

	AddTypeWith          (0x31,new(groupsdb.GroupEntryRTP),groupsdb.CeGroupEntryRTP()).
	AddTypeWith          (0x32,new(RespIncrementRTP),ce_RespIncrementRTP).
	AddTypeWith          (0x33,new(RespRollbackArticleRTP),ce_RespRollbackArticleRTP).
	
	AddTypeWith          (0x41,new(messagedb.ArticleRedirect),messagedb.CeArticleRedirectPtr())
//


type Response struct{
	Data interface{}
}

var ce_Response = serializer.WithInline(&Response{}).
	FieldWith("Data",ce_ResponseData)
//-----------------------------------------------



type Handler struct{
	MessageDB messagedb.IGrpArtDB
	DayfileDB messagedb.IDayfileNode
	GroupsNRT groupsdb.IGroupNRT
	GroupsRTP groupsdb.IGroupRTP
	MessageID messagedb.IMsgidIndexDB
}
func (h *Handler) Create() fastrpc.HandlerCtx { return new(HandlerCtx) }
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
	
	// -----------  messagedb.IDayfileNode -------------
	case *ReqDayfileNodeInfo:
		if h.DayfileDB==nil { return }
		switch v.Cmd {
		case DF_NodeID:
			hctx.Resp.Data = h.DayfileDB.GetDayfileNodeID()
		case DF_FreeStorage:
			hctx.Resp.Data = &RespFreeDayfileStorage{ h.DayfileDB.FreeDayfileStorage() }
		}
	case *ReqAddDayfileBlob:
		if h.DayfileDB==nil { return }
		hctx.Resp.Data = &RespDayfileBlob{h.DayfileDB.AddDayfileBlob(v.DayID,v.Comp,v.Data)}
		
	case *ReqReadDayfileBlob:
		if h.DayfileDB==nil { return }
		hctx.Resp.Data = &RespDayfileBlob{h.DayfileDB.ReadDayfileBlob(v.Data)}
	
	// -----------  groupsdb.IGroupNRT -------------
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
	
	// -----------  groupsdb.IGroupRTP -------------
	case *ReqGroupRTP:
		if h.GroupsRTP==nil { return }
		switch v.Cmd {
		case RTP_GetGroupRTP:
			hctx.Resp.Data = h.GroupsRTP.GetGroupRTP(v.Group)
		case RTP_IncrementRTP:
			nartnum, ok := h.GroupsRTP.IncrementRTP(v.Group)
			hctx.Resp.Data = &RespIncrementRTP{ nartnum, ToBoolean(ok) }
		case RTP_RollbackArticleRTP:
			hctx.Resp.Data = &RespRollbackArticleRTP{
				ToBoolean(h.GroupsRTP.RollbackArticleRTP(v.Group,v.Artnum))}
		}
	// -----------  messagedb.IMsgidIndexDB -------------
	case *ReqGetMessageLocation:
		if h.MessageID==nil { return }
		hctx.Resp.Data = h.MessageID.GetMessageLocation(v.MessageID)
	case *ReqUpdateMessageLocation:
		if h.MessageID==nil { return }
		hctx.Resp.Data = &RespRollbackArticleRTP{ // Reuse datatype
			ToBoolean(h.MessageID.UpdateMessageLocation(v.MessageID,v.ArticlePos,v.Timestamp))}
	}
	return
}


type Client struct{
	Client  *fastrpc.Client
	Timeout time.Duration
	Write   time.Duration
}

func(c *Client) Initialize() error {
	if c.Timeout<=0 {
		c.Timeout = timeconst.NetworkTimeout
	}
	if c.Write<=0 {
		c.Write = timeconst.WriteOverhead
	}
	return nil
}


// -----------  messagedb.IGrpArtDB -------------
func(c *Client) PutArticle(group []byte, num int64, ap *messagedb.ArticlePosting) (ok bool){
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqPutArticle{group,num,ap}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout+c.Write) )
	if err!=nil { return }
	respo,_ := resp.Data.(*RespPutArticle)
	if respo!=nil { return respo.Ok.Bool() }
	return
}

func(c *Client) GetArticle(group []byte, num int64, head, body bool) (headPtr, bodyPtr messagedb.AbstractBlob, ok bool) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGetArticle{group,num, BITS(0).Set(BIT_HEAD,head).Set(BIT_BODY,body) }
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	respo,_ := resp.Data.(*RespGetArticle)
	if respo==nil { return }
	return respo.HeadPtr, respo.BodyPtr, respo.Ok.Bool()
}

func(c *Client) GetXover(group []byte, first, last int64, max int) (result []messagedb.XoverElement){
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGetXover{group,first,last,max}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	respo,_ := resp.Data.([]messagedb.XoverElement)
	return respo
}

// -----------  messagedb.IDayfileNode -------------

func(c *Client) GetDayfileNodeID() *uuid.UUID {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqDayfileNodeInfo{DF_NodeID}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return nil }
	respo,_ := resp.Data.(*uuid.UUID)
	return respo
}
func(c *Client) FreeDayfileStorage() int64 {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqDayfileNodeInfo{DF_FreeStorage}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return 0 }
	respo,_ := resp.Data.(*RespFreeDayfileStorage)
	if respo==nil { return 0 }
	return respo.FreeStorage
}
func(c *Client) AddDayfileBlob(dayid int, ch messagedb.CompressionHint, b messagedb.AbstractBlob) messagedb.AbstractBlob {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqAddDayfileBlob{dayid,ch,b}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout+c.Write) )
	if err!=nil { return nil }
	respo,_ := resp.Data.(*RespDayfileBlob)
	if respo==nil { return nil }
	return respo.Data
}
func(c *Client) ReadDayfileBlob(b messagedb.AbstractBlob) messagedb.AbstractBlob {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqReadDayfileBlob{b}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return nil }
	respo,_ := resp.Data.(*RespDayfileBlob)
	return respo.Data
}

// -----------  groupsdb.IGroupNRT -------------

func(c *Client) GetGroupNRT(group []byte) (entry *groupsdb.GroupEntryNRT) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGetGroupNRT{group}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	entry,_ = resp.Data.(*groupsdb.GroupEntryNRT)
	return
}
func(c *Client) GetGroupBulkNRT(groups [][]byte) (entries []groupsdb.GroupPairNRT) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGetGroupBulkNRT{groups}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	entries,_ = resp.Data.([]groupsdb.GroupPairNRT)
	return
}
func(c *Client) GetGroupsNRT(after, prefix, suffix []byte) (entries []groupsdb.GroupPairNRT) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGetGroupsNRT{after,prefix, suffix}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	entries,_ = resp.Data.([]groupsdb.GroupPairNRT)
	return
}
func(c *Client) PutGroupNRT(group []byte, entry *groupsdb.GroupEntryNRT) (other *groupsdb.GroupEntryNRT, ok bool) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqPutGroupNRT{group, entry}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout+c.Write) )
	if err!=nil { return }
	respo, _ := resp.Data.(*RespPutGroupNRT)
	if respo==nil { return }
	return respo.Other, respo.Ok.Bool()
}


// -----------  groupsdb.IGroupRTP -------------

func(c *Client) GetGroupRTP(group []byte) (entry *groupsdb.GroupEntryRTP) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGroupRTP{RTP_GetGroupRTP,group,0}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	entry,_ = resp.Data.(*groupsdb.GroupEntryRTP)
	return
}
func(c *Client) IncrementRTP(group []byte) (artnum int64, ok bool) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGroupRTP{RTP_IncrementRTP,group,0}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout+c.Write) )
	if err!=nil { return }
	respo,_ := resp.Data.(*RespIncrementRTP)
	if respo==nil { return }
	return respo.Artnum,respo.Ok.Bool()
}
func(c *Client) RollbackArticleRTP(group []byte, artnum int64) (ok bool) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGroupRTP{RTP_RollbackArticleRTP,group,artnum}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout+c.Write) )
	if err!=nil { return }
	respo,_ := resp.Data.(*RespRollbackArticleRTP)
	if respo==nil { return }
	return respo.Ok.Bool()
}

// -----------  messagedb.IMsgidIndexDB -------------

func(c *Client) GetMessageLocation(messageID []byte) (articlePos *messagedb.ArticleRedirect) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqGetMessageLocation{messageID}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout) )
	if err!=nil { return }
	articlePos,_ = resp.Data.(*messagedb.ArticleRedirect)
	return
}

func(c *Client) UpdateMessageLocation(messageID []byte,articlePos *messagedb.ArticleRedirect,timestamp int64) (ok bool) {
	req := new(Request)
	resp := new(Response)
	req.Data = &ReqUpdateMessageLocation{messageID,articlePos,timestamp}
	err := c.Client.DoDeadline(req, resp, time.Now().Add(c.Timeout+c.Write) )
	if err!=nil { return }
	respo,_ := resp.Data.(*RespRollbackArticleRTP)
	if respo==nil { return }
	return respo.Ok.Bool()
}


