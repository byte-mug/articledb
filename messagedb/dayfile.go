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
import "reflect"
import "github.com/nu7hatch/gouuid"
import "github.com/hashicorp/golang-lru/simplelru"
import "sync"
import "os"
import "fmt"

import (
	"io"
	"bufio"
	"bytes"
)

type LruCache interface{
	Add(key, value interface{}) bool
	Contains(key interface{}) (ok bool)
	Get(key interface{}) (value interface{}, ok bool)
	Purge()
	Remove(key interface{}) bool
}
type LruCacheFactory func (e simplelru.EvictCallback) (LruCache,error)

// Returns a simplistic LRU-Cache instance.
func NewLruCache(size int) LruCacheFactory {
	return func (e simplelru.EvictCallback) (LruCache,error) {
		return simplelru.NewLRU(size,e)
	}
}

type DayfileCache struct{
	Folder string
	NodeID *uuid.UUID
	
	c LruCache
	mutex sync.Mutex
}
func (dfc *DayfileCache) Init(f LruCacheFactory) error {
	c,e := f(closeDayfile)
	if e!=nil { return e }
	dfc.c = c
	return nil
}
func (dfc *DayfileCache) GetFile(dayid int) *Dayfile {
	dfc.mutex.Lock(); defer dfc.mutex.Unlock()
	obj,ok := dfc.c.Get(dayid)
	if ok { return obj.(*Dayfile).Grab() }
	
	f,err := os.OpenFile(fmt.Sprintf("%s/%x",dfc.Folder,dayid),os.O_RDWR|os.O_CREATE,0600)
	if err!=nil {
		return nil
	}
	
	dayfile := (&Dayfile{ File: f }).Grab().Grab()
	
	dfc.c.Add(dayid,dayfile)
	return dayfile
}
func (dfc *DayfileCache) Close() error {
	dfc.c.Purge()
	return nil
}


func closeDayfile(key interface{}, value interface{}) {
	value.(*Dayfile).Drop()
}
type Dayfile struct{
	File *os.File
	mutex sync.Mutex
	refc  int
}
func (d *Dayfile) Grab() *Dayfile {
	d.mutex.Lock(); defer d.mutex.Unlock()
	d.refc++
	return d
}
func (d *Dayfile) Drop() {
	d.mutex.Lock(); defer d.mutex.Unlock()
	d.refc--
	if d.refc<1 { d.File.Close() }
}
func (d *Dayfile) put(buf *bytes.Buffer) (int64,error) {
	d.mutex.Lock(); defer d.mutex.Unlock()
	offset,e := d.File.Seek(0,2)
	if e!=nil { return offset,e }
	
	_,e = buf.WriteTo(d.File)
	if e!=nil { return offset,e }
	
	return offset,nil
}
func (d *Dayfile) Add(node *uuid.UUID, dayid int, ch CompressionHint, b AbstractBlob) (AbstractBlob,error) {
	b = ch.Compress(b)
	if b==nil || !b.IsDirect() { return b,nil }
	
	buf := new(bytes.Buffer)
	w := preciseio.PreciseWriterFromPool()
	defer w.PutToPool()
	w.W = buf
	e := ce_AbstractBlob.Write(w,reflect.ValueOf(b))
	if e!=nil { return nil,e }
	blob := &BlobLocation{node,dayid,0,int64(buf.Len())}
	
	blob.Offset,e = d.put(buf)
	if e!=nil { return nil,e }
	
	return blob,nil
}
func (d *Dayfile) Read(b *BlobLocation) (res AbstractBlob,err error) {
	sr := io.NewSectionReader(d.File,b.Offset,b.Length)
	br := bufio.NewReader(sr)
	err = ce_AbstractBlob.Read(preciseio.PreciseReader{br},reflect.ValueOf(&res).Elem())
	return
}

type IDayfileNode interface{
	GetDayfileNodeID() *uuid.UUID
	FreeDayfileStorage() int64
	AddDayfileBlob(dayid int, ch CompressionHint, b AbstractBlob) AbstractBlob
	ReadDayfileBlob(b AbstractBlob) AbstractBlob
}

func (dfc *DayfileCache) GetDayfileNodeID() *uuid.UUID { return dfc.NodeID }
func (dfc *DayfileCache) FreeDayfileStorage() int64 { return -1 }

func (dfc *DayfileCache) AddDayfileBlob(dayid int, ch CompressionHint, b AbstractBlob) AbstractBlob {
	df := dfc.GetFile(dayid)
	
	if df==nil { return nil }
	defer df.Drop()
	
	b,_ = df.Add(dfc.NodeID,dayid,ch,b)
	return b
}

func (dfc *DayfileCache) ReadDayfileBlob(b AbstractBlob) AbstractBlob {
	if b==nil || !b.IsDirect() { return b }
	bl,ok := b.(*BlobLocation)
	if !ok || bl==nil { return nil }
	
	df := dfc.GetFile(bl.DayID)
	if df==nil { return nil }
	defer df.Drop()
	
	fmt.Println(bl)
	res,_ := df.Read(bl)
	return res
}


