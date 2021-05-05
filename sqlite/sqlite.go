package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/prozacchiwawa/dblayer"
)

type SqliteDBLayer struct {
	db *sql.DB
	desc map[string] dblayer.DBTable
}

const queryOpEq = "=="
const queryOpLt = "<"
const queryOpGt = ">"

type SqliteDBQueryFilter struct {
	operator string
	column string
	value interface {}
}

type SqliteDBQuery struct {
	parent *SqliteDBLayer
	table string

	filters []SqliteDBQueryFilter
	limitOffset int
	limitMax int
}

func NewSqliteDBLayer(db *sql.DB, desc map[string] dblayer.DBTable) (dblayer.DBLayer, error) {
	for t, table := range desc {
		columnDefs := make([]string, 2 + len(table.Breakouts))
		columnDefs[0] = "id text primary key"
		for i, name := range table.Breakouts {
			columnDefs[i + 1] = fmt.Sprintf("%s text", name)
		}
		columnDefs[len(table.Breakouts) + 1] = "payload text"
		createStmt := fmt.Sprintf("create table if not exists %s (%s)", t, strings.Join(columnDefs, ","))

			_, err := db.Exec(createStmt)
		if err != nil {
			return nil, err
		}
	}

	return &SqliteDBLayer {
		db: db,
		desc: desc,
	}, nil
}

func (db *SqliteDBLayer) Close() {
	db.db.Close()
}

func (db *SqliteDBLayer) GetDocument(table string, key string) (interface {}, bool, error) {
	query, err := db.db.QueryContext(context.Background(), fmt.Sprintf("select payload from %s where id = ?", table), key)
	if err != nil {
		return nil, false, err
	}
	defer query.Close()

	if !query.Next() {
		return nil, false, nil
	}

	var payload string
	err = query.Scan(&payload)
	if err != nil {
		return nil, false, err
	}

	resultObject, err := db.desc[table].Decoder([]byte(payload))
	if err != nil {
		return nil, false, err
	}

	return resultObject, true, nil
}

func (db *SqliteDBLayer) InsertDocument(table string, key string, value interface {}) error {
	encoded, err := json.Marshal(&value)
	if err != nil {
		return err
	}

	decmap := map[string]interface {} {}
	err = json.Unmarshal(encoded, &decmap)
	if err != nil {
		return err
	}

	breakouts := db.desc[table].Breakouts
	insertArguments := make([]interface {}, len(breakouts) + 2)
	insertColumns := make([]string, len(breakouts) + 2)
	insertDummies := make([]string, len(breakouts) + 2)

	insertArguments[0] = key
	insertColumns[0] = "id"
	insertDummies[0] = "?"

	for i, breakout := range db.desc[table].Breakouts {
		insertArguments[i+1] = decmap[breakout]
		insertColumns[i+1] = breakout
		insertDummies[i+1] = "?"
	}

	insertArguments[len(breakouts) + 1] = string(encoded)
	insertColumns[len(breakouts) + 1] = "payload"
	insertDummies[len(breakouts) + 1] = "?"

	queryString := fmt.Sprintf("insert into %s (%s) values (%s)", table, strings.Join(insertColumns, ","), strings.Join(insertDummies, ","))
	_, err = db.db.Exec(queryString, insertArguments...)
	return err
}

func (db *SqliteDBLayer) InsertDocuments(table string, pairs []dblayer.DBPair) error {
	for _, p := range pairs {
		err := db.InsertDocument(table, p.Id, p.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *SqliteDBLayer) DeleteDocument(table string, key string) error {
	_, err := db.db.Exec(fmt.Sprintf("delete from %s where id = ?", table), key)
	return err
}

func (db *SqliteDBLayer) DeleteDocuments(table string, keys []string) error {
	for _, k := range keys {
		err := db.DeleteDocument(table, k)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *SqliteDBLayer) CreateQuery(table string) dblayer.DBQuery {
	return &SqliteDBQuery {
		parent: db,
		table: table,
		filters: []SqliteDBQueryFilter {},
		limitOffset: -1,
		limitMax: -1,
	}
}

func (db *SqliteDBQuery) FilterEqual(column string, value interface {}) {
	db.filters = append(db.filters, SqliteDBQueryFilter {
		operator: queryOpEq,
		column: column,
		value: value,
	})
}

func (db *SqliteDBQuery) FilterGreater(column string, value interface {}) {
	db.filters = append(db.filters, SqliteDBQueryFilter {
		operator: queryOpGt,
		column: column,
		value: value,
	})
}

func (db *SqliteDBQuery) FilterLess(column string, value interface {}) {
	db.filters = append(db.filters, SqliteDBQueryFilter {
		operator: queryOpLt,
		column: column,
		value: value,
	})
}

func (db *SqliteDBQuery) Limit(lim int) {
	db.limitMax = lim
}

func (db *SqliteDBQuery) Offset(off int) {
	db.limitOffset = off
}

func (db *SqliteDBQuery) Execute() ([]interface {}, error) {
	queryWhereClauses := []string {}
	queryArguments := []interface {} {}

	for _, f := range db.filters {
		queryArguments = append(queryArguments, f.value)
		queryWhereClauses = append(queryWhereClauses, fmt.Sprintf("(%s %s ?)", f.column, f.operator))
	}

	queryWhere := strings.Join(queryWhereClauses, " and ")
	var queryLimit string

	if db.limitOffset == -1 && db.limitMax == -1 {
		queryLimit = ""
	} else if db.limitOffset != -1 {
		queryLimit = fmt.Sprintf("limit 1000000000 offset %d", db.limitOffset)
	} else if db.limitMax != -1 {
		queryLimit = fmt.Sprintf("limit %d", db.limitMax)
	} else {
		queryLimit = fmt.Sprintf("limit %d offset %d", db.limitMax, db.limitOffset)
	}

	doQuery := fmt.Sprintf("select payload from %s where %s %s", db.table, queryWhere, queryLimit)

	rows, err := db.parent.db.QueryContext(context.Background(), doQuery, queryArguments...)
	if err != nil {
		return []interface {} {}, err
	}
	defer rows.Close()

	results := []interface {} {}
	var payload string
	for rows.Next() {
		err = rows.Scan(&payload)
		if err != nil {
			return []interface {} {}, err
		}

		resultObject, err := db.parent.desc[db.table].Decoder([]byte(payload))
		if err != nil {
			return []interface {} {}, err
		}


		results = append(results, resultObject)
	}

	return results, nil
}
