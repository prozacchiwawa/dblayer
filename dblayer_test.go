package dblayer_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/prozacchiwawa/dblayer"
	"github.com/prozacchiwawa/dblayer/sqlite"
	"github.com/prozacchiwawa/dblayer/firestore"

	_ "rsc.io/sqlite"
)

type Employee struct {
	Name string `json:"name"`
	Occupation string `json:"occupation"`
	Company string `json:"company"`
	Salary int `json:"salary"`
}

type Company struct {
	Name string `json:"name"`
	Product string `json:"product"`
}

func TestDBLayer(t *testing.T) {
	_ = os.Remove("test.db")
	sdb, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		panic(fmt.Sprintf("error creating sqlite db %v", err))
	}

	dbdesc := map[string] dblayer.DBTable {
		"employee": dblayer.DBTable {
			Breakouts: []string { "name", "occupation", "company", "salary" },
			Decoder: func(enc []byte) (interface {}, error) {
				emp := Employee {}
				err := json.Unmarshal(enc, &emp)
				if err != nil {
					return nil, err
				}
				return emp, nil
			},
		},
		"company": dblayer.DBTable {
			Breakouts: []string { "name", "product" },
			Decoder: func(enc []byte) (interface {}, error) {
				com := Company {}
				err = json.Unmarshal(enc, &com)
				if err != nil {
					return nil, err
				}
				return com, nil
			},
		},
	}

	db, err := sqlite.NewSqliteDBLayer(sdb, "id", "payload", dbdesc)
	if err != nil {
		panic(fmt.Sprintf("error getting db layer %v", err))
	}
	defer db.Close()

	// Reference so we can test compile.
	_, _ = firestore.NewFireDBLayer(nil, "payload", dbdesc)

	companies := []Company {
		Company {
			Name: "comco",
			Product: "despair",
		},
		Company {
			Name: "prodbux",
			Product: "doubt",
		},
	}

	for i, c := range companies {
		err = db.InsertDocument("company", fmt.Sprintf("c%d", i), c)
		if err != nil {
			panic(fmt.Sprintf("error adding company %v", err))
		}
	}

	employees := []Employee {
		Employee {
			Name: "e1",
			Occupation: "o1",
			Company: "c1",
			Salary: 100000,
		},
		Employee {
			Name: "e2",
			Occupation: "o1",
			Company: "c2",
			Salary: 90000,
		},
		Employee {
			Name: "e3",
			Occupation: "o2",
			Company: "c1",
			Salary: 50000,
		},
	}

	for i, e := range employees {
		err = db.InsertDocument("employee", fmt.Sprintf("e%d", i), e)
		if err != nil {
			panic(fmt.Sprintf("error adding employee %v", err))
		}
	}

	// Make normal query
	q := db.CreateQuery("employee")
	q.FilterEqual("company", "c1")
	results, err := q.Execute()

	resultEmps := make([]Employee, len(results))
	for i, r := range results {
		resultEmps[i] = r.(Employee)
	}

	if len(results) != 2 {
		panic(fmt.Sprintf("c1 has 2 emps %v", results))
	}

	enames := []string {}
	for _, r := range resultEmps {
		enames = append(enames, r.Name)
	}

	sort.Strings(enames)
	if enames[0] != "e1" || enames[1] != "e3" {
		panic(fmt.Sprintf("not the right employees: %v", results))
	}
}

