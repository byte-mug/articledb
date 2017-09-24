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

import "github.com/byte-mug/golibs/serializer"
import "fmt"

func cloneb(i []byte) (j []byte) {
	j = make([]byte,len(i))
	copy(j,i)
	return
}

// Group Entry, Non-Realtime-Part
type GroupEntryNRT struct{
	Description []byte
	Status byte
	
	// The timestamp is mandatory to propagate updates.
	TimeStamp int64 // Timestamp (UNIX-format)
}
func (g GroupEntryNRT) String() string {
	//if g==nil { return "{\"\" '' 0}" }
	return fmt.Sprintf("{%q %q %d}",g.Description,g.Status,g.TimeStamp)
}

type GroupPairNRT struct{
	Key []byte
	Value GroupEntryNRT
}
func (g GroupPairNRT) String() string {
	return fmt.Sprintf("{%q %v}",g.Key,g.Value)
}

var ce_GroupEntryNRT = serializer.With(&GroupEntryNRT{}).
	Field("Description").
	Field("Status").
	Field("TimeStamp")
//-----------------------------------------------

// Group Entry, Realtime-Part
type GroupEntryRTP struct{
	Count int64
	Low int64
	High int64
}

type GroupPairRTP struct{
	Key []byte
	Value GroupEntryRTP
}

var ce_GroupEntryRTP = serializer.With(&GroupEntryRTP{}).
	Field("Count").
	Field("Low").
	Field("High")
//-----------------------------------------------


