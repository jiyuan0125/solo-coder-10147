package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type User struct {
	ID   int32  `db:"id"`
	Name string `db:"name"`
	Age  *int32 `db:"age"`
	Bio  pgtype.Text
}

func main() {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		create temporary table users (
			id int primary key generated always as identity,
			name text not null,
			age int,
			bio text
		)
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create table: %v\n", err)
		os.Exit(1)
	}

	_, err = conn.Exec(ctx, `
		insert into users (name, age, bio) values
			('Alice', 30, 'Loves Go programming'),
			('Bob', null, null),
			('Charlie', 25, 'Enjoys PostgreSQL')
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to insert data: %v\n", err)
		os.Exit(1)
	}

	rows, _ := conn.Query(ctx, `select id, name, age, bio from users order by id`)
	users, err := pgx.CollectRowsToStruct[User](rows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to collect rows: %v\n", err)
		os.Exit(1)
	}

	for _, u := range users {
		fmt.Printf("ID: %d, Name: %s", u.ID, u.Name)
		if u.Age != nil {
			fmt.Printf(", Age: %d", *u.Age)
		} else {
			fmt.Printf(", Age: NULL")
		}
		if u.Bio.Valid {
			fmt.Printf(", Bio: %s", u.Bio.String)
		} else {
			fmt.Printf(", Bio: NULL")
		}
		fmt.Println()
	}

	// Output:
	// ID: 1, Name: Alice, Age: 30, Bio: Loves Go programming
	// ID: 2, Name: Bob, Age: NULL, Bio: NULL
	// ID: 3, Name: Charlie, Age: 25, Bio: Enjoys PostgreSQL
}
