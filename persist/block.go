package persist

import (
	"errors"
	"log"
	"os"
)

type Block struct {
	path string
}

func NewBlock(path string) *Block {
	return &Block{path: path}
}

func (b *Block) Clear() {
	file, err := os.Create(b.path)
	if err != nil {
		log.Fatalf("Block.Clear()|Create|Fail|path=%s|err=%v", b.path, err)
	}
	file.Close()
}

func (b *Block) Size() int {
	info, err := os.Stat(b.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		log.Fatalf("Block.Size()|Stat|Fail|path=%s|err=%v", b.path, err)
	}
	return int(info.Size())
}

func (b *Block) Read() []byte {
	data, err := os.ReadFile(b.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []byte{}
		}
		log.Fatalf("Block.Read()|ReadFile|Fail|path=%s|err=%v", b.path, err)
	}
	return data
}

func (b *Block) Write(data []byte) {
	if err := os.WriteFile(b.path, data, 0o644); err != nil {
		log.Fatalf("Block.Write()|WriteFile|Fail|path=%s|err=%v", b.path, err)
	}
}
