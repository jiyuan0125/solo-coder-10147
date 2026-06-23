package pgx

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestComputeCollectRowsToStructFields_Basic(t *testing.T) {
	type Person struct {
		ID   int32  `db:"id"`
		Name string `db:"name"`
		Age  int32  `db:"age,omitempty"`
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Person{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap["id"]; !ok {
		t.Error("expected to find column 'id'")
	}
	if _, ok := colToFieldMap["name"]; !ok {
		t.Error("expected to find column 'name'")
	}
	if _, ok := colToFieldMap["age"]; !ok {
		t.Error("expected to find column 'age' (tag with comma modifier should be stripped)")
	}

	if len(structFieldNames) != 3 {
		t.Errorf("expected 3 struct fields, got %d", len(structFieldNames))
	}
}

func TestComputeCollectRowsToStructFields_CaseInsensitive(t *testing.T) {
	type Person struct {
		FirstName string
		LastName  string
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Person{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap[strings.ToLower("FirstName")]; !ok {
		t.Error("expected to find column 'firstname'")
	}
	if _, ok := colToFieldMap[strings.ToLower("LastName")]; !ok {
		t.Error("expected to find column 'lastname'")
	}
}

func TestComputeCollectRowsToStructFields_IgnoredField(t *testing.T) {
	type Person struct {
		ID   int32 `db:"id"`
		Name string
		Temp string `db:"-"`
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Person{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap["temp"]; ok {
		t.Error("expected column 'temp' to be ignored (db:\"-\")")
	}

	if len(structFieldNames) != 2 {
		t.Errorf("expected 2 struct fields, got %d", len(structFieldNames))
	}
}

func TestComputeCollectRowsToStructFields_EmbeddedStruct(t *testing.T) {
	type Address struct {
		Street string `db:"street"`
		City   string `db:"city"`
	}

	type Person struct {
		ID int32 `db:"id"`
		Address
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Person{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap["id"]; !ok {
		t.Error("expected to find column 'id'")
	}
	if _, ok := colToFieldMap["street"]; !ok {
		t.Error("expected to find column 'street' from embedded struct")
	}
	if _, ok := colToFieldMap["city"]; !ok {
		t.Error("expected to find column 'city' from embedded struct")
	}
}

func TestComputeCollectRowsToStructFields_EmbeddedPointerNotExpanded(t *testing.T) {
	type Address struct {
		Street string `db:"street"`
		City   string `db:"city"`
	}

	type Person struct {
		ID int32 `db:"id"`
		*Address
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Person{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap["street"]; ok {
		t.Error("expected column 'street' NOT to be found from embedded pointer")
	}
	if _, ok := colToFieldMap["city"]; ok {
		t.Error("expected column 'city' NOT to be found from embedded pointer")
	}
}

func TestComputeCollectRowsToStructFields_AnonymousNonStruct(t *testing.T) {
	type Timestamp int64

	type Event struct {
		ID int32 `db:"id"`
		Timestamp
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Event{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap["id"]; !ok {
		t.Error("expected to find column 'id'")
	}
	if _, ok := colToFieldMap[strings.ToLower("Timestamp")]; !ok {
		t.Error("expected to find column 'timestamp' from anonymous non-struct embedding")
	}
}

func TestComputeCollectRowsToStructFields_RecursiveSelfEmbedding(t *testing.T) {
	type Node struct {
		ID    int32  `db:"id"`
		Name  string `db:"name"`
		Child *Node  `db:"-"`
	}

	fieldStack := make([]int, 0, 1)
	visitedTypes := make(map[reflect.Type]struct{})
	colToFieldMap := make(map[string]*collectRowsToStructField)
	structFieldNames := make(map[string]string)

	err := computeCollectRowsToStructFields(
		reflect.TypeOf(Node{}),
		&fieldStack,
		visitedTypes,
		colToFieldMap,
		structFieldNames,
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := colToFieldMap["id"]; !ok {
		t.Error("expected to find column 'id'")
	}
	if _, ok := colToFieldMap["name"]; !ok {
		t.Error("expected to find column 'name'")
	}
}

func TestIsNullableType(t *testing.T) {
	tests := []struct {
		name     string
		typ      reflect.Type
		expected bool
	}{
		{"int", reflect.TypeOf(int(0)), false},
		{"string", reflect.TypeOf(""), false},
		{"*int", reflect.TypeOf((*int)(nil)), true},
		{"*string", reflect.TypeOf((*string)(nil)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNullableType(tt.typ); got != tt.expected {
				t.Errorf("isNullableType(%v) = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}

func TestLookupCollectRowsToStructFields_MissingStructField(t *testing.T) {
	type Person struct {
		ID   int32  `db:"id"`
		Name string `db:"name"`
		Age  int32  `db:"age"`
	}

	fldDescs := []pgconn.FieldDescription{
		{Name: "id"},
		{Name: "name"},
	}

	_, err := lookupCollectRowsToStructFields(reflect.TypeOf(Person{}), fldDescs)
	if err == nil {
		t.Fatal("expected error for missing struct field 'age'")
	}

	if !strings.Contains(err.Error(), "Age") && !strings.Contains(err.Error(), "age") {
		t.Errorf("expected error to mention 'Age' or 'age', got: %v", err)
	}
}

func TestLookupCollectRowsToStructFields_ExtraColumnNoError(t *testing.T) {
	type Person struct {
		ID   int32  `db:"id"`
		Name string `db:"name"`
	}

	fldDescs := []pgconn.FieldDescription{
		{Name: "id"},
		{Name: "name"},
		{Name: "extra_column"},
	}

	fields, err := lookupCollectRowsToStructFields(reflect.TypeOf(Person{}), fldDescs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(fields.fields) != 3 {
		t.Errorf("expected 3 fields (including nil for extra), got %d", len(fields.fields))
	}

	if fields.fields[2].path != nil {
		t.Error("expected extra column to have nil path (ignored)")
	}
}
