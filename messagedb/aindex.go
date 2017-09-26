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

import "github.com/byte-mug/golibs/preciseio"
import "github.com/boltdb/bolt"
import "bytes"
import "reflect"
import "time"

type IMsgidIndexDB interface{
	GetMessageLocation(messageID []byte) (articlePos *ArticleRedirect)
	UpdateMessageLocation(messageID []byte,articlePos *ArticleRedirect,timestamp int64) (ok bool)
}

var tMsgidIndex    = []byte("MSGID.INDEX")
var tMsgidTimeidx  = []byte("MSGID.TIMES")
type MsgidIndexDB struct{
	DB *bolt.DB
}

func (g *MsgidIndexDB) Initialize() error {
	return g.DB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(tMsgidIndex)
		tx.CreateBucketIfNotExists(tMsgidTimeidx)
		
		return nil
	})
}
func (g *MsgidIndexDB) GetMessageLocation(messageID []byte) (articlePos *ArticleRedirect) {
	g.DB.View(func(tx *bolt.Tx) error {
		redir := new(ArticleRedirect)
		bkt := tx.Bucket(tMsgidIndex)
		if bkt==nil { return nil }
		err := ce_ArticleRedirect.Read(preciseio.PreciseReader{bytes.NewReader(bkt.Get(messageID))}, reflect.ValueOf(redir).Elem())
		if err!=nil { return nil }
		articlePos = redir
		return nil
	})
	return
}
func (g *MsgidIndexDB) UpdateMessageLocation(messageID []byte,articlePos *ArticleRedirect,timestamp int64) (ok bool) {
	if len(messageID)==0 || articlePos==nil { return }
	innerOk := false
	
	// Timestamp ++ Timebased-Pseudorandom
	TimeID := append(encode64(timestamp),encode64(int64(time.Now().UnixNano()))...)
	
	ok = g.DB.Batch(func(tx *bolt.Tx) error {
		buf := new(bytes.Buffer)
		w := preciseio.PreciseWriterFromPool()
		defer w.PutToPool()
		w.W = buf
		ce_ArticleRedirect.Write(w,reflect.ValueOf(articlePos).Elem())
		tx.Bucket(tMsgidTimeidx).Put(TimeID,messageID)
		innerOk = tx.Bucket(tMsgidIndex).Put(messageID,buf.Bytes())!=nil
		return nil
	})!=nil && innerOk
	return
}


