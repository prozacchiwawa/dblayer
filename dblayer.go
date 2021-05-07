package dblayer

import (
)

type DBPair struct {
	Id string
	Value interface {}
}

type DBQuery interface {
	FilterEqual(string, interface {})
	FilterGreater(string, interface {})
	FilterLess(string, interface {})

	Order(string)
	Limit(int)
	Offset(int)

	Execute() ([]DBPair, error)
}

type DBLayer interface {
	CreateQuery(table string) DBQuery

	GetDocument(table string, key string) (interface {}, bool, error)
	InsertDocument(table string, key string, value interface {}) error
	InsertDocuments(table string, keyValues []DBPair) error
	UpdateDocument(table string, key string, value interface {}) error
	DeleteDocument(table string, key string) error
	DeleteDocuments(table string, keys []string) error

	Close()
}

type DBTable struct {
	Breakouts []string
	Decoder func([]byte) (interface {}, error)
}
