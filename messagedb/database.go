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

type IGrpArtDB interface{
	PutArticle(group []byte,num int64, ap *ArticlePosting) (ok bool)
	GetArticle(group []byte,num int64, head, body bool) (headPtr, bodyPtr AbstractBlob, ok bool)
	GetXover(group []byte,first,last int64, max int) (result []XoverElement)
}

var tXover = []byte("GRP.ART.XOVER")
var tRedir = []byte("GRP.ART.REDIR")
var tLocal = []byte("GRP.ART.LOCAL")
var tHead  = []byte("GRP.ART.HEAD" )
var tBody  = []byte("GRP.ART.BODY" )
type GrpArtDB struct{
	DB *bolt.DB
}

func (g *GrpArtDB) Initialize() error {
	return g.DB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(tXover)
		tx.CreateBucketIfNotExists(tRedir)
		tx.CreateBucketIfNotExists(tLocal)
		tx.CreateBucketIfNotExists(tHead)
		tx.CreateBucketIfNotExists(tBody)
		return nil
	})
}

func (g *GrpArtDB) PutArticle(group []byte,num int64, ap *ArticlePosting) (ok bool) {
	ok = g.DB.Batch(func(tx *bolt.Tx) error {
		buf := new(bytes.Buffer)
		w := preciseio.PreciseWriterFromPool()
		defer w.PutToPool()
		w.W = buf
		xoverDB := tx.Bucket(tXover)
		redirDB := tx.Bucket(tRedir)
		locaDB := tx.Bucket(tLocal)
		headDB := tx.Bucket(tHead)
		bodyDB := tx.Bucket(tBody)
		
		numbuf := encode64(num)
		
		var location ArticleLocation
		
		if ap.Head!=nil && !ap.Head.IsDirect() { location.Head = ap.Head }
		if ap.Body!=nil && !ap.Body.IsDirect() { location.Body = ap.Body }
		
		{
			bkt,err := xoverDB.CreateBucketIfNotExists(group)
			if err!=nil { return err }
			ce_ArticleXoverStruct.Write(w, reflect.ValueOf(ap.Xover))
			bkt.Put(numbuf,cloneb(buf.Bytes()))
			buf.Reset()
		}
		
		{
			bkt,err := redirDB.CreateBucketIfNotExists(group)
			if err!=nil { return err }
			ce_ArticleRedirect.Write(w, reflect.ValueOf(ap.Redir).Elem())
			bkt.Put(numbuf,cloneb(buf.Bytes()))
			buf.Reset()
		}
		
		if ap.Head!=nil && location.Head==nil {
			bkt,err := headDB.CreateBucketIfNotExists(group)
			if err!=nil { return err }
			ce_AbstractBlob.Write(w, reflect.ValueOf(ap.Head))
			bkt.Put(numbuf,cloneb(buf.Bytes()))
			buf.Reset()
		}
		
		if ap.Body!=nil && location.Body==nil {
			bkt,err := bodyDB.CreateBucketIfNotExists(group)
			if err!=nil { return err }
			ce_AbstractBlob.Write(w, reflect.ValueOf(ap.Body))
			bkt.Put(numbuf,cloneb(buf.Bytes()))
			buf.Reset()
		}
		
		if location.Head!=nil || location.Body!=nil {
			bkt,err := locaDB.CreateBucketIfNotExists(group)
			if err!=nil { return err }
			ce_ArticleLocation.Write(w, reflect.ValueOf(location))
			bkt.Put(numbuf,cloneb(buf.Bytes()))
			buf.Reset()
		}
		return nil
	})==nil
	return
}

func (g *GrpArtDB) GetArticle(group []byte,num int64, head, body bool) (headPtr, bodyPtr AbstractBlob, ok bool) {
	g.DB.View(func(tx *bolt.Tx) error {
		enc := encode64(num)
		location := new(ArticleLocation)
		bkt := tx.Bucket(tLocal).Bucket(group)
		if bkt!=nil {
			ce_ArticleLocationPtr.Read(preciseio.PreciseReader{bytes.NewReader(bkt.Get(enc))}, reflect.ValueOf(location))
		}
		
		if head {
			headPtr = location.Head
			if headPtr == nil {
				bkt = tx.Bucket(tHead).Bucket(group)
				if bkt==nil { return nil }
				ce_AbstractBlob.Read(preciseio.PreciseReader{bytes.NewReader(bkt.Get(enc))}, reflect.ValueOf(&headPtr).Elem())
				if headPtr==nil { return nil }
			}
		}
		
		if body {
			bodyPtr = location.Body
			if bodyPtr == nil {
				bkt = tx.Bucket(tBody).Bucket(group)
				if bkt==nil { return nil }
				ce_AbstractBlob.Read(preciseio.PreciseReader{bytes.NewReader(bkt.Get(enc))}, reflect.ValueOf(&bodyPtr).Elem())
				if bodyPtr==nil { return nil }
			}
		}
		
		ok = true
		
		return nil
	})
	return
}

func (g *GrpArtDB) GetXover(group []byte,first,last int64, max int) (result []XoverElement) {
	g.DB.View(func(tx *bolt.Tx) error {
		count := max
		kfirst := encode64(first)
		xoverBuk := tx.Bucket(tXover).Bucket(group)
		if xoverBuk==nil { return nil }
		var element XoverElement
		elemXover := reflect.ValueOf(&(element.Xover)).Elem()
		
		c := xoverBuk.Cursor()
		k,v := c.Seek(kfirst)
		for ; len(k)>0 ; k,v = c.Next() {
			element.Number = decode64(k)
			if element.Number>last { break }
			err := ce_ArticleXoverStruct.Read(preciseio.PreciseReader{bytes.NewReader(v)}, elemXover)
			if err!=nil { continue }
			count--
			result = append(result,element)
			if count<1 { break } // Stop loop after $max$ entries.
		}
		
		return nil
	})
	return
}


