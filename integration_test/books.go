package integration

import (
	"strconv"
	"sync/atomic"
)

type Book struct {
	Name string `json:"name"`
	ID   int    `json:"id"`
}

func (b *Book) Array() []any {
	return []any{
		int32(b.ID),
		b.Name,
	}
}

func CreateBooks(count int) []Book {
	var idCounter atomic.Int64
	res := make([]Book, count)
	for i := range count {
		id := int(idCounter.Add(1))
		res[i] = Book{
			ID:   id,
			Name: "book-no-" + strconv.Itoa(id),
		}
	}
	return res
}
