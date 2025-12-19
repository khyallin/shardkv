package persist

import (
	"encoding/binary"
	"log"
	"os"
)

type Int struct {
	value int
	file  string
}

func NewInt(file string) *Int {
	f, err := os.Open(file)
	if err != nil {
		return &Int{file: file}
	}
	defer f.Close()

	var v int32
	err = binary.Read(f, binary.LittleEndian, &v)
	if err != nil {
		log.Fatalf("Int.NewInt()|Read|Fail|file=%s|err=%v", file, err)
	}
	return &Int{value: int(v), file: file}
}

func (i *Int) Set(value int) {
	f, err := os.Create(i.file)
	if err != nil {
		log.Fatalf("Int.Set()|Create|Fail|file=%s|err=%v", i.file, err)
	}
	defer f.Close()

	i.value = value
	err = binary.Write(f, binary.LittleEndian, uint32(value))
	if err != nil {
		log.Fatalf("Int.Set()|Write|Fail|file=%s|err=%v", i.file, err)
	}
}

func (i *Int) Get() int {
	return i.value
}
