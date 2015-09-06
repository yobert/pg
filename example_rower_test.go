package pg_test

import (
	"fmt"
	"github.com/yobert/pg"
	"os"
)

func Example_rower() {
	db := pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
	})
	defer db.Close()

	type Stuff struct {
		Id         int
		FirstName  string  `pg:"first_name"`
		LastName   string  `pg:"last_name"`
		MiddleName *string `pg:"middle_name"`
		Blah       bool
	}

	_, err := db.Exec(`create temp table stuff (id bigint, first_name text, last_name text,  middle_name text);`)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5; i++ {
		s := Stuff{Id: i, FirstName: "kim", LastName: "smith"}
		if i == 2 {
			a := "test"
			s.MiddleName = &a
		}
		_, err := db.ExecOne(`insert into stuff (id, first_name, last_name, middle_name) values (?id, ?first_name, ?last_name, ?middle_name);`, &s)
		if err != nil {
			panic(err)
		}
	}

	row := Stuff{}

	err = db.Rower(&row, func() {
		fmt.Fprintf(os.Stderr, "%#v\n", row)
		if row.MiddleName != nil {
			fmt.Fprintln(os.Stderr, "middle name: ["+*row.MiddleName+"]")
		}
	}, `select id, first_name, last_name, middle_name from stuff;`)

	fmt.Println("hi")
	// Output: hi
}
