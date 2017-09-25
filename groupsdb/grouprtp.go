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

import "github.com/boltdb/bolt"

type IGroupRTP interface{
	GetGroupRTP(group []byte) (entry *GroupEntryRTP)
	IncrementRTP(group []byte) (artnum int64,ok bool)
	RollbackArticleRTP(group []byte,artnum int64) (ok bool)
}

var tGroupRTP = []byte("GRP.RTP")
type GroupRTP struct{
	DB *bolt.DB
}
func (g *GroupRTP) Initialize() error {
	return g.DB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(tGroupRTP)
		return nil
	})
}
func (g *GroupRTP) GetGroupRTP(group []byte) (entry *GroupEntryRTP) {
	g.DB.View(func(tx *bolt.Tx) error {
		var err error
		entry,err = ParseGroupEntryRTP(tx.Bucket(tGroupRTP).Get(group))
		if err!=nil { entry = nil }
		return nil
	})
	return
}
func (g *GroupRTP) IncrementRTP(group []byte) (artnum int64,ok bool) {
	ok = g.DB.Batch(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(tGroupRTP)
		entry,err := ParseGroupEntryRTP(bkt.Get(group))
		if err!=nil || entry==nil { entry = new(GroupEntryRTP) }
		artnum = entry.High+1
		entry.High=artnum
		entry.Count++
		if entry.Low == 0 { entry.Low = artnum }
		bkt.Put(group,entry.Bytes())
		return nil
	})==nil
	return
}
func (g *GroupRTP) RollbackArticleRTP(group []byte,artnum int64) (ok bool) {
	ok = g.DB.Batch(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(tGroupRTP)
		entry,err := ParseGroupEntryRTP(bkt.Get(group))
		if err!=nil || entry==nil { entry = new(GroupEntryRTP) }
		
		entry.Count--
		isHigh := entry.High==artnum
		isLow  := entry.Low==artnum
		
		if entry.Count<1 { // Reset count.
			entry.High = 0
			entry.Low  = 0
		} else if isHigh {
			entry.High--
		} else if isLow {
			entry.Low++
		}
		bkt.Put(group,entry.Bytes())
		return nil
	})==nil
	return
}

