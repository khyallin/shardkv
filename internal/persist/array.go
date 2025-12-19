package persist

import (
	"encoding/gob"
	"io"
	"log"
	"os"
)

type Array[T any] struct {
	value []T
	file  *os.File
}

func NewArray[T any](path string) *Array[T] {
	a := &Array[T]{value: make([]T, 0)}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatalf("NewArray()|OpenFile|Fail|path=%s|err=%v", path, err)
	}
	a.file = file

	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		log.Fatalf("NewArray()|SeekStart|Fail|path=%s|err=%v", path, err)
	}

	dec := gob.NewDecoder(a.file)
	for {
		var item T
		err := dec.Decode(&item)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("NewArray()|Decode|Fail|path=%s|err=%v", path, err)
		}
		a.value = append(a.value, item)
	}
	return a
}

func (a *Array[T]) Close() {
	a.file.Close()
}

func (a *Array[T]) Len() int {
	return len(a.value)
}

func (a *Array[T]) Clear() {
	a.value = make([]T, 0)
	if err := a.file.Truncate(0); err != nil {
		log.Fatalf("Array.Clear()|Truncate|Fail|path=%s|err=%v", a.file.Name(), err)
	}
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		log.Fatalf("Array.Clear()|SeekStart|Fail|path=%s|err=%v", a.file.Name(), err)
	}
	if err := a.file.Sync(); err != nil {
		log.Fatalf("Array.Clear()|Sync|Fail|path=%s|err=%v", a.file.Name(), err)
	}
}

func (a *Array[T]) Set(values []T) {
	a.Clear()
	a.value = values
	for _, v := range values {
		enc := gob.NewEncoder(a.file)
		if err := enc.Encode(v); err != nil {
			log.Fatalf("Array.Set()|Encode|Fail|path=%s|err=%v", a.file.Name(), err)
		}
	}
	if err := a.file.Sync(); err != nil {
		log.Fatalf("Array.Set()|Sync|Fail|path=%s|err=%v", a.file.Name(), err)
	}
}

func (a *Array[T]) Append(value T) {
	a.value = append(a.value, value)
	enc := gob.NewEncoder(a.file)
	if err := enc.Encode(value); err != nil {
		log.Fatalf("Array.Append()|Encode|Fail|path=%s|err=%v", a.file.Name(), err)
	}
	if err := a.file.Sync(); err != nil {
		log.Fatalf("Array.Append()|Sync|Fail|path=%s|err=%v", a.file.Name(), err)
	}
}

func (a *Array[T]) Item(i int) T {
	return a.value[i]
}

func (a *Array[T]) Range(l, r int) []T {
	if l < 0 || r > a.Len() || l > r {
		log.Fatalf("Array.Range()|InvalidRange|l=%d|r=%d|len=%d", l, r, len(a.value))
	}
	return append([]T(nil), a.value[l:r]...)
}

func (a *Array[T]) Get() []T {
	return append([]T(nil), a.value...)
}

func (a *Array[T]) ByteSize() int {
	info, err := a.file.Stat()
	if err != nil {
		log.Fatalf("Array.ByteSize()|Stat|Fail|path=%s|err=%v", a.file.Name(), err)
	}
	return int(info.Size())
}
