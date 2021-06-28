package firestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"cloud.google.com/go/firestore"
	"github.com/prozacchiwawa/dblayer"
	"google.golang.org/api/iterator"
	// "google.golang.org/api/option"
)

type FireDBLayer struct {
	client *firestore.Client

	desc map[string] dblayer.DBTable
	idField string
	payloadField string
	less func (string, string, interface {}, interface {}) bool
}

const queryOpEq = "=="
const queryOpLt = "<"
const queryOpGt = ">"
const batchSize = 200

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
	reversed bool
}

func NewFireDBLayer(client *firestore.Client, idField string, payloadField string, less func (string, string, interface {}, interface {}) bool, desc map[string] dblayer.DBTable) (dblayer.DBLayer, error) {
	return &FireDBLayer {
		client: client,
		desc: desc,
		idField: idField,
		payloadField: payloadField,
		less: less,
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

	finalMap[db.idField] = key
	finalMap[db.payloadField] = string(encoded)

	collection := db.client.Collection(table)
	_, err = collection.Doc(key).Create(context.Background(), &finalMap)
	return err
}

func (db *FireDBLayer) UpdateDocument(table string, key string, value interface {}) error {
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

	finalMap[db.idField] = key
	finalMap[db.payloadField] = string(encoded)

	docRef := db.client.Collection(table).Doc(key)
	_, err = docRef.Set(context.Background(), &finalMap)
	return err
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
		orderBy: "",
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

func (db *FireDBQuery) ReverseOrder(orderBy string) {
	db.orderBy = orderBy
	db.reversed = true
}

func (db *FireDBQuery) Limit(lim int) {
	db.limitMax = lim
}

func (db *FireDBQuery) Offset(off int) {
	db.limitOffset = off
}

func (db *FireDBQuery) prepareQueryIterator() (*firestore.DocumentIterator, bool) {
	collection := db.parent.client.Collection(db.table)

	var query firestore.Query
	var iter *firestore.DocumentIterator

	// I didn't realize how many ordering rules firebase has on queries
	// that are not expressed in the Query type.
	//
	// We're not allowed to use OrderBy when there's an equals comparison
	// but we must use it when there's a relational comparison.
	hasEquals := false

	if len(db.filters) > 0 {
		for i, f := range db.filters {
			if f.operator == queryOpEq {
				hasEquals = true
			}

			if i == 0 {
				query = collection.Where(f.column, f.operator, f.value)
			} else {
				query = query.Where(f.column, f.operator, f.value)
			}
		}

		if db.limitOffset != -1 {
			query = query.StartAt(db.limitOffset)
		}

		if db.limitMax != -1 {
			query = query.Limit(db.limitMax)
		}

		iter = query.Documents(context.Background())
	} else {
		if db.orderBy == "" && (db.limitOffset != -1 || db.limitMax != -1) {
			db.orderBy = db.parent.idField
		}

		if db.limitOffset != -1 || db.limitMax != -1 {
			fsorder := firestore.Asc
			if db.reversed {
				fsorder = firestore.Desc
			}
			query = collection.OrderBy(db.orderBy, fsorder)

			if db.limitOffset != -1 {
				query = query.StartAt(db.limitOffset)
			}

			if db.limitMax != -1 {
				query = query.Limit(db.limitMax)
			}

			iter = query.Documents(context.Background())
		} else {
			// No filters, apply limits to the collection itself.
			iter = collection.Documents(context.Background())
		}
	}

	return iter, hasEquals
}

func (db *FireDBQuery) Execute() ([]dblayer.DBPair, error) {
	iter, hasEquals := db.prepareQueryIterator()

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
			log.Printf("no payload field (%s) in table %s, record data: %v\n", db.table, db.parent.payloadField, recovered)
			continue
		}

		resultObject, err := db.parent.desc[db.table].Decoder([]byte(fmt.Sprintf("%v", payload)))
		if err != nil {
			return []dblayer.DBPair {}, err
		}

		results = append(results, dblayer.DBPair { Id: snap.Ref.ID, Value: resultObject })
	}

	// Sort results if we couldn't before
	if hasEquals && db.orderBy != "" {
		sort.Slice(results, func(i, j int) bool {
			return db.parent.less(db.table, db.orderBy, results[i].Value, results[j].Value)
		})
	}

	return results, nil
}

func (db *FireDBQuery) Delete() error {
	iter, _ := db.prepareQueryIterator()
	todelete := []string {}

	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return err
		}

		todelete = append(todelete, snap.Ref.ID)
	}

	for _, d := range todelete {
		err := db.parent.DeleteDocument(db.table, d)
		if err != nil {
			return err
		}
	}

	return nil
}
