package firestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"github.com/prozacchiwawa/dblayer"
	"google.golang.org/api/iterator"
	// "google.golang.org/api/option"
)

type FireDBLayer struct {
	client *firestore.Client

	desc map[string] dblayer.DBTable
	payloadField string
}

const queryOpEq = "=="
const queryOpLt = "<"
const queryOpGt = ">"

type FireDBQueryFilter struct {
	operator string
	column string
	value interface {}
}

type FireDBQuery struct {
	parent *FireDBLayer
	table string

	filters []FireDBQueryFilter
	limitOffset int
	limitMax int
	orderBy string
}

func NewFireDBLayer(client *firestore.Client, payloadField string, desc map[string] dblayer.DBTable) (dblayer.DBLayer, error) {
	return &FireDBLayer {
		client: client,
		desc: desc,
		payloadField: payloadField,
	}, nil
}

func (db *FireDBLayer) Close() {
	db.client.Close()
}


func (db *FireDBLayer) GetDocument(table string, key string) (interface {}, bool, error) {
	doc, err := db.client.Collection(table).Doc(key).Get(context.Background())
	if err != nil {
		return nil, false, err
	}

	decodedMap := make(map[string]interface {})
	doc.DataTo(&decodedMap)

	if payloadData, ok := decodedMap[db.payloadField]; !ok {
		return nil, false, nil
	} else {
		resultObject, err := db.desc[table].Decoder([]byte(fmt.Sprintf("%v", payloadData)))
		if err != nil {
			return nil, false, err
		}

		return resultObject, true, nil
	}
}

func (db *FireDBLayer) InsertDocument(table string, key string, value interface {}) error {
	encoded, err := json.Marshal(&value)
	if err != nil {
		return err
	}

	decmap := map[string]interface {} {}
	err = json.Unmarshal(encoded, &decmap)
	if err != nil {
		return err
	}

	finalMap := map[string]interface {} {}
	for _, c := range db.desc[table].Breakouts {
		finalMap[c] = decmap[c]
	}
	finalMap[db.payloadField] = string(encoded)

	collection := db.client.Collection(table)
	_, err = collection.Doc(key).Create(context.Background(), &finalMap)
	return err
}

func (db *FireDBLayer) UpdateDocument(table string, key string, value interface {}) error {
	err := db.DeleteDocument(table, key)
	if err != nil {
		return err
	}
	return db.InsertDocument(table, key, value)
}

func (db *FireDBLayer) InsertDocuments(table string, pairs []dblayer.DBPair) error {
	for _, p := range pairs {
		err := db.InsertDocument(table, p.Id, p.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *FireDBLayer) DeleteDocument(table string, key string) error {
	_, err := db.client.Collection(table).Doc(key).Delete(context.Background())
	return err
}

func (db *FireDBLayer) DeleteDocuments(table string, keys []string) error {
	for _, k := range keys {
		err := db.DeleteDocument(table, k)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *FireDBLayer) CreateQuery(table string) dblayer.DBQuery {
	return &FireDBQuery {
		parent: db,
		table: table,
		filters: []FireDBQueryFilter {},
		limitOffset: -1,
		limitMax: -1,
		orderBy: db.payloadField,
	}
}

func (db *FireDBQuery) FilterEqual(column string, value interface {}) {
	db.filters = append(db.filters, FireDBQueryFilter {
		operator: queryOpEq,
		column: column,
		value: value,
	})
}

func (db *FireDBQuery) FilterGreater(column string, value interface {}) {
	db.filters = append(db.filters, FireDBQueryFilter {
		operator: queryOpGt,
		column: column,
		value: value,
	})
}

func (db *FireDBQuery) FilterLess(column string, value interface {}) {
	db.filters = append(db.filters, FireDBQueryFilter {
		operator: queryOpLt,
		column: column,
		value: value,
	})
}

func (db *FireDBQuery) Order(orderBy string) {
	db.orderBy = orderBy
}

func (db *FireDBQuery) Limit(lim int) {
	db.limitMax = lim
}

func (db *FireDBQuery) Offset(off int) {
	db.limitOffset = off
}

func (db *FireDBQuery) Execute() ([]dblayer.DBPair, error) {
	collection := db.parent.client.Collection(db.table)

	var query firestore.Query
	query = collection.OrderBy(db.orderBy, firestore.Asc)

	if db.limitOffset != -1 {
		query = query.StartAt(db.limitOffset)
	}

	if db.limitMax != -1 {
		query = query.Limit(db.limitMax)
	}

	for _, f := range db.filters {
		query = query.Where(f.column, f.operator, f.value)
	}

	iter := query.Documents(context.Background())

	results := []dblayer.DBPair {}
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return []dblayer.DBPair {}, err
		}

		recovered := make(map[string]interface {})
		snap.DataTo(&recovered)

		payload, ok := recovered[db.parent.payloadField]
		if !ok {
			log.Printf("no payload field in table %s\n", db.table)
			continue
		}

		resultObject, err := db.parent.desc[db.table].Decoder([]byte(fmt.Sprintf("%v", payload)))
		if err != nil {
			return []dblayer.DBPair {}, err
		}

		results = append(results, dblayer.DBPair { Id: snap.Ref.ID, Value: resultObject })
	}

	return results, nil
}
