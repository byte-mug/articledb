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
import "bytes"

type IGroupNRT interface{
	GetGroupNRT(group []byte) (entry *GroupEntryNRT)
	GetGroupBulkNRT(groups [][]byte) (entries []GroupPairNRT)
	GetGroupsNRT(after, prefix, suffix []byte) (entries []GroupPairNRT)
	UpdateGroupNRT(group []byte, entry *GroupEntryNRT) (other *GroupEntryNRT,ok bool)
	CreateGroupNRT(group []byte, entry *GroupEntryNRT) (ok bool)
}

var tGroupNRT = []byte("GRP.NRT")
type GroupNRT struct{
	DB *bolt.DB
}
func (g *GroupNRT) Initialize() error {
	return g.DB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(tGroupNRT)
		return nil
	})
}
func (g *GroupNRT) GetGroupNRT(group []byte) (entry *GroupEntryNRT) {
	g.DB.View(func(tx *bolt.Tx) error {
		var err error
		entry,err = ParseGroupEntryNRT(tx.Bucket(tGroupNRT).Get(group))
		if err!=nil { entry = nil }
		return nil
	})
	return
}
func (g *GroupNRT) GetGroupBulkNRT(groups [][]byte) (entries []GroupPairNRT) {
	entries = make([]GroupPairNRT,0,len(groups))
	g.DB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(tGroupNRT)
		for _,group := range groups { 
			entry,err := ParseGroupEntryNRT(bkt.Get(group))
			if err!=nil || entry==nil { continue }
			entries = append(entries,GroupPairNRT{group,*entry})
		}
		return nil
	})
	return
}
func (g *GroupNRT) GetGroupsNRT(after, prefix, suffix []byte) (entries []GroupPairNRT) {
	var k,v []byte
	g.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(tGroupNRT).Cursor()
		//var err error
		na := after
		if bytes.Compare(na,prefix)<0 { na = prefix }
		if len(na)>0 {
			k,v = c.Seek(na)
			if len(k)!=0 && bytes.Compare(after,k) == 0 { k,v = c.Next() }
		} else {
			k,v = c.First()
		}
		for len(k)>0 {
			
			// -------------------------------------------------
			if !bytes.HasPrefix(k,prefix) { break }    // We are beyond our range.
			if !bytes.HasSuffix(k,suffix) { continue } // Wrong suffix... Skip it.
			je,err := ParseGroupEntryNRT(v)
			if err!=nil { continue }
			entries = append(entries,GroupPairNRT{cloneb(k),*je})
			// -------------------------------------------------
			
			k,v = c.Next()
		}
		return nil
	})
	return
}
func (g *GroupNRT) UpdateGroupNRT(group []byte, entry *GroupEntryNRT) (other *GroupEntryNRT,ok bool) {
	err := g.DB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(tGroupNRT)
		oldEntry,err := ParseGroupEntryNRT(bkt.Get(group))
		if err!=nil || oldEntry==nil { return nil }
		if oldEntry.TimeStamp < entry.TimeStamp {
			err = bkt.Put(group,entry.Bytes()) // Update.
			if err!=nil { return nil }
		}
		ok = true
		other = oldEntry
		return nil
	})
	if err!=nil { ok = false }
	return
}
func (g *GroupNRT) CreateGroupNRT(group []byte, entry *GroupEntryNRT) (ok bool) {
	err := g.DB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(tGroupNRT)
		oldEntry,err := ParseGroupEntryNRT(bkt.Get(group))
		if err==nil && oldEntry!=nil { return nil }
		ok = true
		bkt.Put(group,entry.Bytes())
		return nil
	})
	if err!=nil { ok = false }
	return
}
