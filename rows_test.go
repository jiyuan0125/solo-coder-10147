package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

type testRowScanner struct {
	name string
	age  int32
}

func (rs *testRowScanner) ScanRow(rows pgx.Rows) error {
	return rows.Scan(&rs.name, &rs.age)
}

func TestRowScanner(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var s testRowScanner
		err := conn.QueryRow(ctx, "select 'Adam' as name, 72 as height").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "Adam", s.name)
		require.Equal(t, int32(72), s.age)
	})
}

type testErrRowScanner string

func (ers *testErrRowScanner) ScanRow(rows pgx.Rows) error {
	return errors.New(string(*ers))
}

// https://github.com/jackc/pgx/issues/1654
func TestRowScannerErrorIsFatalToRows(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		s := testErrRowScanner("foo")
		err := conn.QueryRow(ctx, "select 'Adam' as name, 72 as height").Scan(&s)
		require.EqualError(t, err, "foo")
	})
}

func TestForEachRow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var actualResults []any

		rows, _ := conn.Query(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachRow(rows, []any{&a, &b}, func() error {
			actualResults = append(actualResults, []any{a, b})
			return nil
		})
		require.NoError(t, err)

		expectedResults := []any{
			[]any{1, 2},
			[]any{2, 4},
			[]any{3, 6},
		}
		require.Equal(t, expectedResults, actualResults)
		require.EqualValues(t, 3, ct.RowsAffected())
	})
}

func TestForEachRowScanError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var actualResults []any

		rows, _ := conn.Query(
			context.Background(),
			"select 'foo', 'bar' from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachRow(rows, []any{&a, &b}, func() error {
			actualResults = append(actualResults, []any{a, b})
			return nil
		})
		require.EqualError(t, err, "can't scan into dest[0]: cannot scan text (OID 25) in text format into *int")
		require.Equal(t, pgconn.CommandTag{}, ct)
	})
}

func TestForEachRowAbort(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			3,
		)
		var a, b int
		ct, err := pgx.ForEachRow(rows, []any{&a, &b}, func() error {
			return errors.New("abort")
		})
		require.EqualError(t, err, "abort")
		require.Equal(t, pgconn.CommandTag{}, ct)
	})
}

func ExampleForEachRow() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(
		context.Background(),
		"select n, n * 2 from generate_series(1, $1) n",
		3,
	)
	var a, b int
	_, err = pgx.ForEachRow(rows, []any{&a, &b}, func() error {
		fmt.Printf("%v, %v\n", a, b)
		return nil
	})
	if err != nil {
		fmt.Printf("ForEachRow error: %v", err)
		return
	}

	// Output:
	// 1, 2
	// 2, 4
	// 3, 6
}

func TestCollectRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}

func TestCollectRowsEmpty(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(1, 0) n`)
		numbers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)
		require.NotNil(t, numbers)

		assert.Empty(t, numbers)
	})
}

// This example uses CollectRows with a manually written collector function. In most cases RowTo, RowToAddrOf,
// RowToStructByPos, RowToAddrOfStructByPos, or another generic function would be used.
func ExampleCollectRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `select n from generate_series(1, 5) n`)
	numbers, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	fmt.Println(numbers)

	// Output:
	// [1 2 3 4 5]
}

func TestCollectOneRow(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42`)
		n, err := pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(42), n)
	})
}

func TestCollectOneRowNotFound(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42 where false`)
		n, err := pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		assert.Equal(t, int32(0), n)
	})
}

func TestCollectOneRowIgnoresExtraRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(42, 99) n`)
		n, err := pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)

		assert.NoError(t, err)
		assert.Equal(t, int32(42), n)
	})
}

// https://github.com/jackc/pgx/issues/1334
func TestCollectOneRowPrefersPostgreSQLErrorOverErrNoRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (name text not null unique)`)
		require.NoError(t, err)

		var name string
		rows, _ := conn.Query(ctx, `insert into t (name) values ('foo') returning name`)
		name, err = pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (string, error) {
			var n string
			err := row.Scan(&n)
			return n, err
		})
		require.NoError(t, err)
		require.Equal(t, "foo", name)

		rows, _ = conn.Query(ctx, `insert into t (name) values ('foo') returning name`)
		name, err = pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (string, error) {
			var n string
			err := row.Scan(&n)
			return n, err
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23505", pgErr.Code)
		require.Equal(t, "", name)
	})
}

func TestCollectExactlyOneRow(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42`)
		n, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.NoError(t, err)
		assert.Equal(t, int32(42), n)
	})
}

func TestCollectExactlyOneRowNotFound(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 42 where false`)
		n, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		assert.Equal(t, int32(0), n)
	})
}

func TestCollectExactlyOneRowExtraRows(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(42, 99) n`)
		n, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (int32, error) {
			var n int32
			err := row.Scan(&n)
			return n, err
		})
		assert.ErrorIs(t, err, pgx.ErrTooManyRows)
		assert.Equal(t, int32(0), n)
	})
}

func TestRowTo(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgx.CollectRows(rows, pgx.RowTo[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}

func ExampleRowTo() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `select n from generate_series(1, 5) n`)
	numbers, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	fmt.Println(numbers)

	// Output:
	// [1 2 3 4 5]
}

func TestRowToAddrOf(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgx.CollectRows(rows, pgx.RowToAddrOf[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), *numbers[i])
		}
	})
}

func ExampleRowToAddrOf() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `select n from generate_series(1, 5) n`)
	pNumbers, err := pgx.CollectRows(rows, pgx.RowToAddrOf[int32])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range pNumbers {
		fmt.Println(*p)
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func TestRowToMap(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToMap)
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i]["name"])
			assert.EqualValues(t, i, slice[i]["age"])
		}
	})
}

func TestRowToStructByPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestRowToStructByPosIgnoredField(t *testing.T) {
	type person struct {
		Name string
		Age  int32 `db:"-"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
		}
	})
}

func TestRowToStructByPosEmbeddedStruct(t *testing.T) {
	type Name struct {
		First string
		Last  string
	}

	type person struct {
		Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].Name.First)
			assert.Equal(t, "Smith", slice[i].Name.Last)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestRowToStructByPosMultipleEmbeddedStruct(t *testing.T) {
	type Sandwich struct {
		Bread string
		Salad string
	}
	type Drink struct {
		Ml int
	}

	type meal struct {
		Sandwich
		Drink
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Baguette' as bread, 'Lettuce' as salad, drink_ml from generate_series(0, 9) drink_ml`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[meal])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Baguette", slice[i].Sandwich.Bread)
			assert.Equal(t, "Lettuce", slice[i].Sandwich.Salad)
			assert.EqualValues(t, i, slice[i].Drink.Ml)
		}
	})
}

func TestRowToStructByPosEmbeddedUnexportedStruct(t *testing.T) {
	type name struct {
		First string
		Last  string
	}

	type person struct {
		name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].name.First)
			assert.Equal(t, "Smith", slice[i].name.Last)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

// Pointer to struct is not supported. But check that we don't panic.
func TestRowToStructByPosEmbeddedPointerToStruct(t *testing.T) {
	type Name struct {
		First string
		Last  string
	}

	type person struct {
		*Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		_, err := pgx.CollectRows(rows, pgx.RowToStructByPos[person])
		require.EqualError(t, err, "got 3 values, but dst struct has only 2 fields")
	})
}

func ExampleRowToStructByPos() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Price int32
	}

	rows, _ := conn.Query(ctx, "select * from products where price < $1 order by price desc", 12)
	products, err := pgx.CollectRows(rows, pgx.RowToStructByPos[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

func TestRowToAddrOfStructPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestRowToStructByName(t *testing.T) {
	type person struct {
		Last      string
		First     string
		Age       int32
		AccountID string
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'Smith' as last, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.ErrorContains(t, err, "cannot find field First in returned row")

		// check missing field in a destination struct
		rows, _ = conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, 'd5e49d3f' as account_id, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameDbTags(t *testing.T) {
	type person struct {
		Last             string `db:"last_name"`
		First            string `db:"first_name"`
		Age              int32  `db:"age"`
		AccountID        string `db:"account_id"`
		AnotherAccountID string `db:"account__id"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, 'd5e49d3f' as account_id, '5e49d321' as account__id from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
			assert.Equal(t, "d5e49d3f", slice[i].AccountID)
			assert.Equal(t, "5e49d321", slice[i].AnotherAccountID)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.ErrorContains(t, err, "cannot find field first_name in returned row")

		// check missing field in a destination struct
		rows, _ = conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, 'd5e49d3f' as account_id, '5e49d321' as account__id, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameEmbeddedStruct(t *testing.T) {
	type Name struct {
		Last  string `db:"last_name"`
		First string `db:"first_name"`
	}

	type person struct {
		Ignore bool `db:"-"`
		Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Name.Last)
			assert.Equal(t, "John", slice[i].Name.First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToStructByName[person])
		assert.ErrorContains(t, err, "cannot find field first_name in returned row")

		// check missing field in a destination struct
		rows, _ = conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func ExampleRowToStructByName() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Price int32
	}

	rows, _ := conn.Query(ctx, "select * from products where price < $1 order by price desc", 12)
	products, err := pgx.CollectRows(rows, pgx.RowToStructByName[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

func TestRowToStructByNameLax(t *testing.T) {
	type person struct {
		Last   string
		First  string
		Age    int32
		Ignore bool `db:"-"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Last)
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first, n as age from generate_series(0, 9) n`)
		slice, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check extra fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first, 'Smith' as last, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")

		// check missing fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last, 'D.' as middle, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field middle")

		// check ignored fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameLaxEmbeddedStruct(t *testing.T) {
	type Name struct {
		Last  string `db:"last_name"`
		First string `db:"first_name"`
	}

	type person struct {
		Ignore bool `db:"-"`
		Name
		Age int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age from generate_series(0, 9) n`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Smith", slice[i].Name.Last)
			assert.Equal(t, "John", slice[i].Name.First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check missing fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first_name, n as age from generate_series(0, 9) n`)
		slice, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[person])
		assert.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "John", slice[i].Name.First)
			assert.EqualValues(t, i, slice[i].Age)
		}

		// check extra fields in a returned row
		rows, _ = conn.Query(ctx, `select 'John' as first_name, 'Smith' as last_name, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")

		// check missing fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, 'D.' as middle_name, n as age from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field middle_name")

		// check ignored fields in a destination struct
		rows, _ = conn.Query(ctx, `select 'Smith' as last_name, n as age, null as ignore from generate_series(0, 9) n`)
		_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[person])
		assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	})
}

func TestRowToStructByNameLaxRowValue(t *testing.T) {
	type AnotherTable struct{}
	type User struct {
		UserID int    `json:"userId" db:"user_id"`
		Name   string `json:"name" db:"name"`
	}
	type UserAPIKey struct {
		UserAPIKeyID int `json:"userApiKeyId" db:"user_api_key_id"`
		UserID       int `json:"userId" db:"user_id"`

		User         *User         `json:"user" db:"user"`
		AnotherTable *AnotherTable `json:"anotherTable" db:"another_table"`
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "")

		rows, _ := conn.Query(ctx, `
		WITH user_api_keys AS (
			SELECT 1 AS user_id, 101 AS user_api_key_id, 'abc123' AS api_key
		), users AS (
			SELECT 1 AS user_id, 'John Doe' AS name
		)
		SELECT user_api_keys.user_api_key_id, user_api_keys.user_id, row(users.*) AS user
		FROM user_api_keys
		LEFT JOIN users ON users.user_id = user_api_keys.user_id
		WHERE user_api_keys.api_key = 'abc123';
		`)
		slice, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[UserAPIKey])

		assert.NoError(t, err)
		assert.ElementsMatch(t, slice, []UserAPIKey{{UserAPIKeyID: 101, UserID: 1, User: &User{UserID: 1, Name: "John Doe"}, AnotherTable: nil}})
	})
}

func ExampleRowToStructByNameLax() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Type  string
		Price int32
	}

	rows, _ := conn.Query(ctx, "select * from products where price < $1 order by price desc", 12)
	products, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

type TestID int64
type TestTimestamp int64

type TestEmbedBase struct {
	X int32 `db:"x"`
	Y int32 `db:"y"`
}

type TestOuterEmbedPtrBase struct {
	X int32 `db:"x"`
	Z int32 `db:"z"`
}

type TestNode struct {
	Val  int32 `db:"val"`
	Next *TestNode
}

func TestCollectStructRows_CaseInsensitive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type row struct {
			FirstName string `db:"first_name"`
			LastName  string
			AGE       int32
		}

		rows, _ := conn.Query(ctx, `select 'Ada' as FIRST_NAME, 'Lovelace' as LastName, 36 as age from generate_series(1,2)`)
		got, err := pgx.CollectStructRows[row](rows)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, r := range got {
			assert.Equal(t, "Ada", r.FirstName)
			assert.Equal(t, "Lovelace", r.LastName)
			assert.Equal(t, int32(36), r.AGE)
		}
	})
}

func TestCollectStructRows_DbTagCommaTruncation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type row struct {
			Name string `db:"the_name,omitempty"`
			Num  int32  `db:"the_num,omitempty"`
		}

		rows, _ := conn.Query(ctx, `select 'hello' as the_name, 42 as the_num from generate_series(1,1)`)
		got, err := pgx.CollectStructRows[row](rows)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "hello", got[0].Name)
		assert.Equal(t, int32(42), got[0].Num)
	})
}

func TestCollectStructRows_EmptyResultSet(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type row struct {
			Foo int32 `db:"no_such_column"`
			Bar string
		}

		rows, _ := conn.Query(ctx, `select n from generate_series(1,0) n`)
		got, err := pgx.CollectStructRows[row](rows)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, 0, len(got))
	})
}

func TestCollectStructRows_EmbedStruct(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type outer struct {
			TestEmbedBase
			Y int32 `db:"y_alias"`
		}

		rows, _ := conn.Query(ctx, `select 1 as x, 2 as y_alias from generate_series(1,3)`)
		got, err := pgx.CollectStructRows[outer](rows)
		require.NoError(t, err)
		require.Len(t, got, 3)
		for _, r := range got {
			assert.Equal(t, int32(1), r.X)
			assert.Equal(t, int32(2), r.Y)
			assert.Equal(t, int32(0), r.TestEmbedBase.Y)
		}
	})
}

func TestCollectStructRows_EmbedPtrToStruct(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type outer struct {
			*TestOuterEmbedPtrBase
			Y int32 `db:"y"`
		}

		rows, _ := conn.Query(ctx, `select 10 as x, 20 as z, 30 as y from generate_series(1,2)`)
		got, err := pgx.CollectStructRows[outer](rows)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, r := range got {
			require.NotNil(t, r.TestOuterEmbedPtrBase)
			assert.Equal(t, int32(10), r.X)
			assert.Equal(t, int32(20), r.Z)
			assert.Equal(t, int32(30), r.Y)
		}
	})
}

func TestCollectStructRows_EmbedPtrToStruct_SelfRefSkip(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 7 as val from generate_series(1,3)`)
		got, err := pgx.CollectStructRows[TestNode](rows)
		require.NoError(t, err)
		require.Len(t, got, 3)
		for _, r := range got {
			assert.Equal(t, int32(7), r.Val)
			assert.Nil(t, r.Next)
		}
	})
}

func TestCollectStructRows_EmbedNamedType(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type row struct {
			TestID
			TestTimestamp
			Name string `db:"name"`
		}

		rows, _ := conn.Query(ctx, `select 99 as test_id, 1000 as test_timestamp, 'n' as name from generate_series(1,1)`)
		got, err := pgx.CollectStructRows[row](rows)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, TestID(99), got[0].TestID)
		assert.Equal(t, TestTimestamp(1000), got[0].TestTimestamp)
		assert.Equal(t, "n", got[0].Name)
	})
}

func TestCollectStructRows_MissingStructField_ErrorHasLocation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type row struct {
			Name string `db:"name"`
			Num  int32  `db:"num_that_does_not_exist_in_result"`
		}

		rows, _ := conn.Query(ctx, `select 'hi' as name from generate_series(1,2)`)
		got, err := pgx.CollectStructRows[row](rows)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "row")
		assert.Contains(t, err.Error(), "num_that_does_not_exist_in_result")
		assert.Nil(t, got)
	})
}

func TestCollectStructRows_ScanErrorMidway_PreservesRows(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `
create temporary table t_midway_ok (n int, txt text);
insert into t_midway_ok(n, txt) values (1, 'a');
insert into t_midway_ok(n, txt) values (2, 'b');
insert into t_midway_ok(n, txt) values (3, 'c');
`)
		require.NoError(t, err)

		rows, err := conn.Query(ctx, `select n, case when n = 3 then 99::text else txt end as txt from t_midway_ok order by n`)
		require.NoError(t, err)

		type goodRow struct {
			N   int32  `db:"n"`
			Txt string `db:"txt"`
		}
		got, scanErr := pgx.CollectStructRows[goodRow](rows)
		require.NoError(t, scanErr)
		require.Len(t, got, 3)
		assert.Equal(t, int32(1), got[0].N)
		assert.Equal(t, "a", got[0].Txt)
		assert.Equal(t, int32(2), got[1].N)
		assert.Equal(t, "b", got[1].Txt)
		assert.Equal(t, int32(3), got[2].N)
		assert.Equal(t, "99", got[2].Txt)
	})
}

func TestCollectStructRows_ScanErrorMidway_RowsClosed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `
create temporary table t_midway_bad (n int, val text);
insert into t_midway_bad(n, val) values (1, '1');
insert into t_midway_bad(n, val) values (2, 'not_an_integer');
insert into t_midway_bad(n, val) values (3, '3');
`)
		require.NoError(t, err)

		rows, err := conn.Query(ctx, `select n, val from t_midway_bad order by n`)
		require.NoError(t, err)

		type badTarget struct {
			N int32 `db:"n"`
			X int32 `db:"val"`
		}
		got, scanErr := pgx.CollectStructRows[badTarget](rows)
		require.Error(t, scanErr)
		assert.Contains(t, scanErr.Error(), "can't scan")
		require.Len(t, got, 1)
		if len(got) >= 1 {
			assert.Equal(t, int32(1), got[0].N)
			assert.Equal(t, int32(1), got[0].X)
		}
	})
}

func TestCollectStructRows_ConcurrentRace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	defaultConnTestRunner.RunTest(ctx, t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type typeA struct {
			ID   int32  `db:"id"`
			Name string `db:"name"`
		}
		type typeB struct {
			Blob []byte      `db:"blob"`
			Msg  pgtype.Text `db:"msg"`
		}
		type typeC struct {
			Score *int32 `db:"score"`
			TestEmbedBase
		}
		type typeD struct {
			When time.Time `db:"when_"`
			Tags []string  `db:"tags"`
		}

		connStr := os.Getenv("PGX_TEST_DATABASE")
		require.NotEmpty(t, connStr, "PGX_TEST_DATABASE must be set for concurrent test")

		_, err := conn.Exec(ctx, `
drop table if exists t_race_a_perm;
drop table if exists t_race_b_perm;
drop table if exists t_race_c_perm;
drop table if exists t_race_d_perm;

create table t_race_a_perm (id int, name text);
insert into t_race_a_perm (id, name) values (1, 'a1'), (2, 'a2');

create table t_race_b_perm (blob bytea, msg text);
insert into t_race_b_perm (blob, msg) values (E'\\x000102', 'hello');

create table t_race_c_perm (score int, x int, y int);
insert into t_race_c_perm (score, x, y) values (95, 1, 2);

create table t_race_d_perm (when_ timestamptz, tags text[]);
insert into t_race_d_perm (when_, tags) values ('2025-01-02T03:04:05+00', array['x','y']);
`)
		require.NoError(t, err)
		defer func() {
			_, _ = conn.Exec(ctx, `
drop table if exists t_race_a_perm;
drop table if exists t_race_b_perm;
drop table if exists t_race_c_perm;
drop table if exists t_race_d_perm;
`)
		}()

		type querySpec struct {
			name string
			sql  string
		}
		queries := []querySpec{
			{"A1", "select id, name from t_race_a_perm"},
			{"A2", "select name, id from t_race_a_perm"},
			{"B1", "select blob, msg from t_race_b_perm"},
			{"C1", "select score, x, y from t_race_c_perm"},
			{"D1", "select when_, tags from t_race_d_perm"},
			{"A3", "select id, name from t_race_a_perm"},
			{"B2", "select msg, blob from t_race_b_perm"},
			{"C2", "select y, score, x from t_race_c_perm"},
		}

		pgx.StructRowFieldCacheClear()

		var wg sync.WaitGroup
		var errorCount atomic.Int64
		var typeCounters [4]atomic.Int64

		const rounds = 20
		for r := 0; r < rounds; r++ {
			for gi, q := range queries {
				wg.Add(1)
				go func(goroutineID int, query querySpec) {
					defer wg.Done()

					gConn, gErr := pgx.Connect(ctx, connStr)
					if gErr != nil {
						errorCount.Add(1)
						t.Errorf("goroutine %d: connect err: %v", goroutineID, gErr)
						return
					}
					defer gConn.Close(ctx)

					for iter := 0; iter < 5; iter++ {
						switch goroutineID % 4 {
						case 0, 1:
							rows, qErr := gConn.Query(ctx, query.sql)
							if qErr != nil {
								errorCount.Add(1)
								t.Errorf("query err: %v", qErr)
								return
							}
							as, aErr := pgx.CollectStructRows[typeA](rows)
							if aErr != nil {
								errorCount.Add(1)
								t.Errorf("A err[%s]: %v", query.name, aErr)
								return
							}
							typeCounters[0].Add(int64(len(as)))
							for _, a := range as {
								if a.ID != 1 && a.ID != 2 {
									errorCount.Add(1)
									t.Errorf("A[%s] bad id=%d", query.name, a.ID)
								}
							}
						case 2:
							rows, qErr := gConn.Query(ctx, query.sql)
							if qErr != nil {
								errorCount.Add(1)
								t.Errorf("query err: %v", qErr)
								return
							}
							bs, bErr := pgx.CollectStructRows[typeB](rows)
							if bErr != nil {
								errorCount.Add(1)
								t.Errorf("B err[%s]: %v", query.name, bErr)
								return
							}
							typeCounters[1].Add(int64(len(bs)))
							_ = bs
						case 3:
							rows, qErr := gConn.Query(ctx, query.sql)
							if qErr != nil {
								errorCount.Add(1)
								t.Errorf("query err: %v", qErr)
								return
							}
							cs, cErr := pgx.CollectStructRows[typeC](rows)
							if cErr != nil {
								errorCount.Add(1)
								t.Errorf("C err[%s]: %v", query.name, cErr)
								return
							}
							typeCounters[2].Add(int64(len(cs)))
							for _, c := range cs {
								if c.Score == nil || *c.Score != 95 {
									errorCount.Add(1)
									t.Errorf("C[%s] bad score", query.name)
								}
							}
						}
						switch goroutineID % 2 {
						case 0:
							if goroutineID%4 != 3 {
								rows, qErr := gConn.Query(ctx, queries[4].sql)
								if qErr != nil {
									errorCount.Add(1)
									t.Errorf("query err: %v", qErr)
									return
								}
								ds, dErr := pgx.CollectStructRows[typeD](rows)
								if dErr != nil {
									errorCount.Add(1)
									t.Errorf("D err[%s]: %v", query.name, dErr)
									return
								}
								typeCounters[3].Add(int64(len(ds)))
								_ = ds
							}
						}
					}
				}(gi, q)
			}
		}
		wg.Wait()

		assert.Equal(t, int64(0), errorCount.Load())

		aCount := typeCounters[0].Load()
		bCount := typeCounters[1].Load()
		cCount := typeCounters[2].Load()
		dCount := typeCounters[3].Load()
		assert.Greater(t, aCount, int64(0))
		assert.Greater(t, bCount, int64(0))
		assert.Greater(t, cCount, int64(0))
		assert.Greater(t, dCount, int64(0))

		cacheLen := pgx.StructRowFieldCacheLen()
		expectedMin := 10
		assert.GreaterOrEqual(t, cacheLen, expectedMin)
	})
}
