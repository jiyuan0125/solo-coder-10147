package pgx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Rows is the result set returned from [Conn.Query]. Rows must be closed before
// the [Conn] can be used again. Rows are closed by explicitly calling [Rows.Close],
// calling [Rows.Next] until it returns false, or when a fatal error occurs.
//
// Once a Rows is closed the only methods that may be called are [Rows.Close], [Rows.Err],
// and [Rows.CommandTag].
//
// Rows is an interface instead of a struct to allow tests to mock Query. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Rows interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while executing a query or reading its results. Err must be called after the
	// Rows is closed (either by calling Close or by Next returning false) to check if the query was successful. If it is
	// called before the Rows is closed it may return nil even if the query failed on the server.
	Err() error

	// CommandTag returns the command tag from this query. It is only available after Rows is closed.
	CommandTag() pgconn.CommandTag

	// FieldDescriptions returns the field descriptions of the columns. It may return nil. In particular this can occur
	// when there was an error executing the query.
	FieldDescriptions() []pgconn.FieldDescription

	// Next prepares the next row for reading. It returns true if there is another row and false if no more rows are
	// available or a fatal error has occurred. It automatically closes rows upon returning false (whether due to all rows
	// having been read or due to an error).
	//
	// Callers should check rows.Err() after rows.Next() returns false to detect whether result-set reading ended
	// prematurely due to an error. See [Conn.Query] for details.
	//
	// For simpler error handling, consider using the higher-level pgx v5 [CollectRows()] and [ForEachRow()] helpers instead.
	Next() bool

	// Scan reads the values from the current row into dest values positionally. dest can include pointers to core types,
	// values implementing the Scanner interface, and nil. nil will skip the value entirely. It is an error to call Scan
	// without first calling Next() and checking that it returned true. Rows is automatically closed upon error.
	Scan(dest ...any) error

	// Values returns the decoded row values. As with Scan(), it is an error to
	// call Values without first calling Next() and checking that it returned
	// true.
	Values() ([]any, error)

	// RawValues returns the unparsed bytes of the row values. The returned data is only valid until the next Next
	// call or the Rows is closed.
	RawValues() [][]byte

	// Conn returns the underlying *Conn on which the query was executed. This may return nil if Rows did not come from a
	// *Conn (e.g. if it was created by RowsFromResultReader)
	Conn() *Conn
}

// Row is a convenience wrapper over [Rows] that is returned by [Conn.QueryRow].
//
// Row is an interface instead of a struct to allow tests to mock QueryRow. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Row interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Row interface {
	// Scan works the same as Rows. with the following exceptions. If no
	// rows were found it returns ErrNoRows. If multiple rows are returned it
	// ignores all but the first.
	Scan(dest ...any) error
}

// RowScanner scans an entire row at a time into the RowScanner.
type RowScanner interface {
	// ScanRows scans the row.
	ScanRow(rows Rows) error
}

// connRow implements the Row interface for Conn.QueryRow.
type connRow baseRows

func (r *connRow) Scan(dest ...any) (err error) {
	rows := (*baseRows)(r)

	if rows.Err() != nil {
		return rows.Err()
	}

	for _, d := range dest {
		if _, ok := d.(*pgtype.DriverBytes); ok {
			rows.Close()
			return fmt.Errorf("cannot scan into *pgtype.DriverBytes from QueryRow")
		}
	}

	if !rows.Next() {
		if rows.Err() == nil {
			return ErrNoRows
		}
		return rows.Err()
	}

	rows.Scan(dest...)
	rows.Close()
	return rows.Err()
}

// baseRows implements the Rows interface for Conn.Query.
type baseRows struct {
	typeMap      *pgtype.Map
	resultReader *pgconn.ResultReader

	values [][]byte

	commandTag pgconn.CommandTag
	err        error
	closed     bool

	scanPlans []pgtype.ScanPlan
	scanTypes []reflect.Type

	conn              *Conn
	multiResultReader *pgconn.MultiResultReader

	queryTracer QueryTracer
	batchTracer BatchTracer
	ctx         context.Context
	startTime   time.Time
	sql         string
	args        []any
	rowCount    int
}

func (rows *baseRows) FieldDescriptions() []pgconn.FieldDescription {
	return rows.resultReader.FieldDescriptions()
}

func (rows *baseRows) Close() {
	if rows.closed {
		return
	}

	rows.closed = true

	if rows.resultReader != nil {
		var closeErr error
		rows.commandTag, closeErr = rows.resultReader.Close()
		if rows.err == nil {
			rows.err = closeErr
		}
	}

	if rows.multiResultReader != nil {
		closeErr := rows.multiResultReader.Close()
		if rows.err == nil {
			rows.err = closeErr
		}
	}

	if rows.err != nil && rows.conn != nil && rows.sql != "" {
		if sc := rows.conn.statementCache; sc != nil {
			sc.Invalidate(rows.sql)
		}

		if sc := rows.conn.descriptionCache; sc != nil {
			sc.Invalidate(rows.sql)
		}
	}

	if rows.batchTracer != nil {
		rows.batchTracer.TraceBatchQuery(rows.ctx, rows.conn, TraceBatchQueryData{SQL: rows.sql, Args: rows.args, CommandTag: rows.commandTag, Err: rows.err})
	} else if rows.queryTracer != nil {
		rows.queryTracer.TraceQueryEnd(rows.ctx, rows.conn, TraceQueryEndData{rows.commandTag, rows.err})
	}

	// Zero references to other memory allocations. This allows them to be GC'd even when the Rows still referenced. In
	// particular, when using pgxpool GC could be delayed as pgxpool.poolRows are allocated in large slices.
	//
	// https://github.com/jackc/pgx/pull/2269
	rows.values = nil
	rows.scanPlans = nil
	rows.scanTypes = nil
	rows.ctx = nil
	rows.sql = ""
	rows.args = nil
}

func (rows *baseRows) CommandTag() pgconn.CommandTag {
	return rows.commandTag
}

func (rows *baseRows) Err() error {
	return rows.err
}

// fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *baseRows) fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

func (rows *baseRows) Next() bool {
	if rows.closed {
		return false
	}

	if rows.resultReader.NextRow() {
		rows.rowCount++
		rows.values = rows.resultReader.Values()
		return true
	} else {
		rows.Close()
		return false
	}
}

func (rows *baseRows) Scan(dest ...any) error {
	m := rows.typeMap
	fieldDescriptions := rows.FieldDescriptions()
	values := rows.values

	if len(fieldDescriptions) != len(values) {
		err := fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(values))
		rows.fatal(err)
		return err
	}

	if len(dest) == 1 {
		if rc, ok := dest[0].(RowScanner); ok {
			err := rc.ScanRow(rows)
			if err != nil {
				rows.fatal(err)
			}
			return err
		}
	}

	if len(fieldDescriptions) != len(dest) {
		err := fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
		rows.fatal(err)
		return err
	}

	if rows.scanPlans == nil {
		rows.scanPlans = make([]pgtype.ScanPlan, len(values))
		rows.scanTypes = make([]reflect.Type, len(values))
		for i := range dest {
			rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			rows.scanTypes[i] = reflect.TypeOf(dest[i])
		}
	}

	for i, dst := range dest {
		if dst == nil {
			continue
		}

		if rows.scanTypes[i] != reflect.TypeOf(dst) {
			rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			rows.scanTypes[i] = reflect.TypeOf(dest[i])
		}

		err := rows.scanPlans[i].Scan(values[i], dst)
		if err != nil {
			err = ScanArgError{ColumnIndex: i, FieldName: fieldDescriptions[i].Name, Err: err}
			rows.fatal(err)
			return err
		}
	}

	return nil
}

func (rows *baseRows) Values() ([]any, error) {
	if rows.closed {
		return nil, errors.New("rows is closed")
	}

	values := make([]any, 0, len(rows.FieldDescriptions()))

	for i := range rows.FieldDescriptions() {
		buf := rows.values[i]
		fd := &rows.FieldDescriptions()[i]

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.typeMap.TypeForOID(fd.DataTypeOID); ok {
			value, err := dt.Codec.DecodeValue(rows.typeMap, fd.DataTypeOID, fd.Format, buf)
			if err != nil {
				rows.fatal(err)
			}
			values = append(values, value)
		} else {
			switch fd.Format {
			case TextFormatCode:
				values = append(values, string(buf))
			case BinaryFormatCode:
				newBuf := make([]byte, len(buf))
				copy(newBuf, buf)
				values = append(values, newBuf)
			default:
				rows.fatal(errors.New("unknown format code"))
			}
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}

	return values, rows.Err()
}

func (rows *baseRows) RawValues() [][]byte {
	return rows.values
}

func (rows *baseRows) Conn() *Conn {
	return rows.conn
}

type ScanArgError struct {
	ColumnIndex int
	FieldName   string
	Err         error
}

func (e ScanArgError) Error() string {
	if e.FieldName == "?column?" { // Don't include the fieldname if it's unknown
		return fmt.Sprintf("can't scan into dest[%d]: %v", e.ColumnIndex, e.Err)
	}

	return fmt.Sprintf("can't scan into dest[%d] (col: %s): %v", e.ColumnIndex, e.FieldName, e.Err)
}

func (e ScanArgError) Unwrap() error {
	return e.Err
}

// ScanRow decodes raw row data into dest. It can be used to scan rows read from the lower level [pgconn] interface.
//
// typeMap - OID to Go type mapping.
// fieldDescriptions - OID and format of values
// values - the raw data as returned from the PostgreSQL server
// dest - the destination that values will be decoded into
func ScanRow(typeMap *pgtype.Map, fieldDescriptions []pgconn.FieldDescription, values [][]byte, dest ...any) error {
	if len(fieldDescriptions) != len(values) {
		return fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(values))
	}
	if len(fieldDescriptions) != len(dest) {
		return fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
	}

	for i, d := range dest {
		if d == nil {
			continue
		}

		err := typeMap.Scan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, values[i], d)
		if err != nil {
			return ScanArgError{ColumnIndex: i, FieldName: fieldDescriptions[i].Name, Err: err}
		}
	}

	return nil
}

// RowsFromResultReader returns a [Rows] that will read from values resultReader and decode with typeMap. It can be used
// to read from the lower level [pgconn] interface.
func RowsFromResultReader(typeMap *pgtype.Map, resultReader *pgconn.ResultReader) Rows {
	return &baseRows{
		typeMap:      typeMap,
		resultReader: resultReader,
	}
}

// ForEachRow iterates through rows. For each row it scans into the elements of scans and calls fn. If any row
// fails to scan or fn returns an error the query will be aborted and the error will be returned. Rows will be closed
// when ForEachRow returns.
func ForEachRow(rows Rows, scans []any, fn func() error) (pgconn.CommandTag, error) {
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		err = fn()
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}

	if err := rows.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}

	return rows.CommandTag(), nil
}

// CollectableRow is the subset of Rows methods that a RowToFunc is allowed to call.
type CollectableRow interface {
	FieldDescriptions() []pgconn.FieldDescription
	Scan(dest ...any) error
	Values() ([]any, error)
	RawValues() [][]byte
}

// RowToFunc is a function that scans or otherwise converts row to a T.
type RowToFunc[T any] func(row CollectableRow) (T, error)

// AppendRows iterates through rows, calling fn for each row, and appending the results into a slice of T.
//
// This function closes the rows automatically on return.
func AppendRows[T any, S ~[]T](slice S, rows Rows, fn RowToFunc[T]) (S, error) {
	defer rows.Close()

	for rows.Next() {
		value, err := fn(rows)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}

// CollectRows iterates through rows, calling fn for each row, and collecting the results into a slice of T.
//
// This function closes the rows automatically on return.
func CollectRows[T any](rows Rows, fn RowToFunc[T]) ([]T, error) {
	return AppendRows([]T{}, rows, fn)
}

// CollectOneRow calls fn for the first row in rows and returns the result. If no rows are found returns an error where errors.Is(ErrNoRows) is true.
// CollectOneRow is to [CollectRows] as [Conn.QueryRow] is to [Conn.Query].
//
// This function closes the rows automatically on return.
func CollectOneRow[T any](rows Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()

	var value T
	var err error

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return value, err
		}
		return value, ErrNoRows
	}

	value, err = fn(rows)
	if err != nil {
		return value, err
	}

	// The defer rows.Close() won't have executed yet. If the query returned more than one row, rows would still be open.
	// rows.Close() must be called before rows.Err() so we explicitly call it here.
	rows.Close()
	return value, rows.Err()
}

// CollectExactlyOneRow calls fn for the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
//
// This function closes the rows automatically on return.
func CollectExactlyOneRow[T any](rows Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
	)

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return value, err
		}

		return value, ErrNoRows
	}

	value, err = fn(rows)
	if err != nil {
		return value, err
	}

	if rows.Next() {
		var zero T

		return zero, ErrTooManyRows
	}

	return value, rows.Err()
}

// RowTo returns a T scanned from row.
func RowTo[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&value)
	return value, err
}

// RowToAddrOf returns the address of a T scanned from row.
func RowToAddrOf[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&value)
	return &value, err
}

// RowToMap returns a map scanned from row.
func RowToMap(row CollectableRow) (map[string]any, error) {
	var value map[string]any
	err := row.Scan((*mapRowScanner)(&value))
	return value, err
}

type mapRowScanner map[string]any

func (rs *mapRowScanner) ScanRow(rows Rows) error {
	values, err := rows.Values()
	if err != nil {
		return err
	}

	*rs = make(mapRowScanner, len(values))

	for i := range values {
		(*rs)[rows.FieldDescriptions()[i].Name] = values[i]
	}

	return nil
}

// RowToStructByPos returns a T scanned from row. T must be a struct. T must have the same number of public fields as row
// has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then the field will be
// ignored.
func RowToStructByPos[T any](row CollectableRow) (T, error) {
	var value T
	err := (&positionalStructRowScanner{ptrToStruct: &value}).ScanRow(row)
	return value, err
}

// RowToAddrOfStructByPos returns the address of a T scanned from row. T must be a struct. T must have the same number a
// public fields as row has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then
// the field will be ignored.
func RowToAddrOfStructByPos[T any](row CollectableRow) (*T, error) {
	var value T
	err := (&positionalStructRowScanner{ptrToStruct: &value}).ScanRow(row)
	return &value, err
}

type positionalStructRowScanner struct {
	ptrToStruct any
}

func (rs *positionalStructRowScanner) ScanRow(rows CollectableRow) error {
	typ := reflect.TypeOf(rs.ptrToStruct).Elem()
	fields := lookupStructFields(typ)
	if len(rows.RawValues()) > len(fields) {
		return fmt.Errorf(
			"got %d values, but dst struct has only %d fields",
			len(rows.RawValues()),
			len(fields),
		)
	}
	scanTargets := setupStructScanTargets(rs.ptrToStruct, fields)
	return rows.Scan(scanTargets...)
}

// Map from reflect.Type -> []structRowField
var positionalStructFieldMap sync.Map

func lookupStructFields(t reflect.Type) []structRowField {
	if cached, ok := positionalStructFieldMap.Load(t); ok {
		return cached.([]structRowField)
	}

	fieldStack := make([]int, 0, 1)
	fields := computeStructFields(t, make([]structRowField, 0, t.NumField()), &fieldStack)
	fieldsIface, _ := positionalStructFieldMap.LoadOrStore(t, fields)
	return fieldsIface.([]structRowField)
}

func computeStructFields(
	t reflect.Type,
	fields []structRowField,
	fieldStack *[]int,
) []structRowField {
	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			fields = computeStructFields(sf.Type, fields, fieldStack)
		} else if sf.PkgPath == "" {
			dbTag, _ := sf.Tag.Lookup(structTagKey)
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			fields = append(fields, structRowField{
				path: append([]int(nil), *fieldStack...),
			})
		}
	}
	*fieldStack = (*fieldStack)[:tail]
	return fields
}

// RowToStructByName returns a T scanned from row. T must be a struct. T must have the same number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByName[T any](row CollectableRow) (T, error) {
	var value T
	err := (&namedStructRowScanner{ptrToStruct: &value}).ScanRow(row)
	return value, err
}

// RowToAddrOfStructByName returns the address of a T scanned from row. T must be a struct. T must have the same number
// of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByName[T any](row CollectableRow) (*T, error) {
	var value T
	err := (&namedStructRowScanner{ptrToStruct: &value}).ScanRow(row)
	return &value, err
}

// RowToStructByNameLax returns a T scanned from row. T must be a struct. T must have greater than or equal number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByNameLax[T any](row CollectableRow) (T, error) {
	var value T
	err := (&namedStructRowScanner{ptrToStruct: &value, lax: true}).ScanRow(row)
	return value, err
}

// RowToAddrOfStructByNameLax returns the address of a T scanned from row. T must be a struct. T must have greater than or
// equal number of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByNameLax[T any](row CollectableRow) (*T, error) {
	var value T
	err := (&namedStructRowScanner{ptrToStruct: &value, lax: true}).ScanRow(row)
	return &value, err
}

type namedStructRowScanner struct {
	ptrToStruct any
	lax         bool
}

func (rs *namedStructRowScanner) ScanRow(rows CollectableRow) error {
	typ := reflect.TypeOf(rs.ptrToStruct).Elem()
	fldDescs := rows.FieldDescriptions()
	namedStructFields, err := lookupNamedStructFields(typ, fldDescs)
	if err != nil {
		return err
	}
	if !rs.lax && namedStructFields.missingField != "" {
		return fmt.Errorf("cannot find field %s in returned row", namedStructFields.missingField)
	}
	fields := namedStructFields.fields
	scanTargets := setupStructScanTargets(rs.ptrToStruct, fields)
	return rows.Scan(scanTargets...)
}

// Map from namedStructFieldMap -> *namedStructFields
var namedStructFieldMap sync.Map

type namedStructFieldsKey struct {
	t        reflect.Type
	colNames string
}

type namedStructFields struct {
	fields []structRowField
	// missingField is the first field from the struct without a corresponding row field.
	// This is used to construct the correct error message for non-lax queries.
	missingField string
}

func lookupNamedStructFields(
	t reflect.Type,
	fldDescs []pgconn.FieldDescription,
) (*namedStructFields, error) {
	key := namedStructFieldsKey{
		t:        t,
		colNames: joinFieldNames(fldDescs),
	}
	if cached, ok := namedStructFieldMap.Load(key); ok {
		return cached.(*namedStructFields), nil
	}

	// We could probably do two-levels of caching, where we compute the key -> fields mapping
	// for a type only once, cache it by type, then use that to compute the column -> fields
	// mapping for a given set of columns.
	fieldStack := make([]int, 0, 1)
	fields, missingField := computeNamedStructFields(
		fldDescs,
		t,
		make([]structRowField, len(fldDescs)),
		&fieldStack,
	)
	for i, f := range fields {
		if f.path == nil {
			return nil, fmt.Errorf(
				"struct doesn't have corresponding row field %s",
				fldDescs[i].Name,
			)
		}
	}

	fieldsIface, _ := namedStructFieldMap.LoadOrStore(
		key,
		&namedStructFields{fields: fields, missingField: missingField},
	)
	return fieldsIface.(*namedStructFields), nil
}

func joinFieldNames(fldDescs []pgconn.FieldDescription) string {
	switch len(fldDescs) {
	case 0:
		return ""
	case 1:
		return fldDescs[0].Name
	}

	totalSize := len(fldDescs) - 1 // Space for separator bytes.
	for _, d := range fldDescs {
		totalSize += len(d.Name)
	}
	var b strings.Builder
	b.Grow(totalSize)
	b.WriteString(fldDescs[0].Name)
	for _, d := range fldDescs[1:] {
		b.WriteByte(0) // Join with NUL byte as it's (presumably) not a valid column character.
		b.WriteString(d.Name)
	}
	return b.String()
}

func computeNamedStructFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	fields []structRowField,
	fieldStack *[]int,
) ([]structRowField, string) {
	var missingField string
	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		if sf.PkgPath != "" && !sf.Anonymous {
			// Field is unexported, skip it.
			continue
		}
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			var missingSubField string
			fields, missingSubField = computeNamedStructFields(
				fldDescs,
				sf.Type,
				fields,
				fieldStack,
			)
			if missingField == "" {
				missingField = missingSubField
			}
		} else {
			dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
			if dbTagPresent {
				dbTag, _, _ = strings.Cut(dbTag, ",")
			}
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			colName := dbTag
			if !dbTagPresent {
				colName = sf.Name
			}
			fpos := fieldPosByName(fldDescs, colName, !dbTagPresent)
			if fpos == -1 {
				if missingField == "" {
					missingField = colName
				}
				continue
			}
			fields[fpos] = structRowField{
				path: append([]int(nil), *fieldStack...),
			}
		}
	}
	*fieldStack = (*fieldStack)[:tail]

	return fields, missingField
}

const structTagKey = "db"

func fieldPosByName(fldDescs []pgconn.FieldDescription, field string, normalize bool) (i int) {
	i = -1

	if normalize {
		field = strings.ReplaceAll(field, "_", "")
	}
	for i, desc := range fldDescs {
		if normalize {
			if strings.EqualFold(strings.ReplaceAll(desc.Name, "_", ""), field) {
				return i
			}
		} else {
			if desc.Name == field {
				return i
			}
		}
	}
	return i
}

// structRowField describes a field of a struct.
//
// TODO: It would be a bit more efficient to track the path using the pointer
// offset within the (outermost) struct and use unsafe.Pointer arithmetic to
// construct references when scanning rows. However, it's not clear it's worth
// using unsafe for this.
type structRowField struct {
	path []int
}

func setupStructScanTargets(receiver any, fields []structRowField) []any {
	scanTargets := make([]any, len(fields))
	v := reflect.ValueOf(receiver).Elem()
	for i, f := range fields {
		scanTargets[i] = v.FieldByIndex(f.path).Addr().Interface()
	}
	return scanTargets
}

// RowsToStructs scans all rows into a slice of T. T must be a struct. Rows are matched to struct fields by name
// (case-insensitive). The "db" struct tag overrides the column name. If the "db" struct tag is "-" then the field is
// ignored. Struct fields without a corresponding column in the result set produce an error. Columns without a
// corresponding struct field are silently ignored. NULL values into non-nullable fields (non-pointer, non-sql.Null*,
// non-pgtype nullable types) produce an error. Embedded structs are expanded (subject to Go field shadowing rules);
// embedded pointers to structs are not expanded.
//
// This function closes the rows automatically on return.
func RowsToStructs[T any](rows Rows) ([]T, error) {
	defer rows.Close()

	var zero T
	typ := reflect.TypeOf(zero)
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("RowsToStructs: T must be a struct, got %s", typ.Kind())
	}

	result := make([]T, 0)

	fldDescs := rows.FieldDescriptions()
	strictFields, err := lookupStrictNamedStructFields(typ, fldDescs)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var value T
		scanTargets := setupStrictStructScanTargets(&value, strictFields)

		rawValues := rows.RawValues()
		for _, sf := range strictFields {
			if sf.columnIndex == -1 {
				continue
			}
			rawVal := rawValues[sf.columnIndex]
			if rawVal == nil && !sf.nullable {
				return result, fmt.Errorf(
					"RowsToStructs: cannot scan NULL into non-nullable field %s (type %s); column: %s",
					sf.fieldName, sf.fieldType.String(), fldDescs[sf.columnIndex].Name,
				)
			}
		}

		err := rows.Scan(scanTargets...)
		if err != nil {
			return result, err
		}
		result = append(result, value)
	}

	if err := rows.Err(); err != nil {
		return result, err
	}

	return result, nil
}

// RowToStruct scans a single row into T. T must be a struct. Matching behavior is the same as RowsToStructs.
func RowToStruct[T any](row CollectableRow) (T, error) {
	var value T
	err := (&strictNamedStructRowScanner{ptrToStruct: &value}).ScanRow(row)
	return value, err
}

type strictNamedStructRowScanner struct {
	ptrToStruct any
}

func (rs *strictNamedStructRowScanner) ScanRow(rows CollectableRow) error {
	typ := reflect.TypeOf(rs.ptrToStruct).Elem()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("RowToStruct: T must be a struct, got %s", typ.Kind())
	}

	fldDescs := rows.FieldDescriptions()
	strictFields, err := lookupStrictNamedStructFields(typ, fldDescs)
	if err != nil {
		return err
	}

	scanTargets := setupStrictStructScanTargets(rs.ptrToStruct, strictFields)

	rawValues := rows.RawValues()
	for _, sf := range strictFields {
		if sf.columnIndex == -1 {
			continue
		}
		rawVal := rawValues[sf.columnIndex]
		if rawVal == nil && !sf.nullable {
			return fmt.Errorf(
				"RowToStruct: cannot scan NULL into non-nullable field %s (type %s); column: %s",
				sf.fieldName, sf.fieldType.String(), fldDescs[sf.columnIndex].Name,
			)
		}
	}

	return rows.Scan(scanTargets...)
}

type strictStructRowField struct {
	path        []int
	columnIndex int
	nullable    bool
	fieldName   string
	fieldType   reflect.Type
}

var strictNamedStructFieldMap sync.Map

type strictNamedStructFieldsKey struct {
	t        reflect.Type
	colNames string
}

func lookupStrictNamedStructFields(
	t reflect.Type,
	fldDescs []pgconn.FieldDescription,
) ([]strictStructRowField, error) {
	key := strictNamedStructFieldsKey{
		t:        t,
		colNames: joinFieldNames(fldDescs),
	}
	if cached, ok := strictNamedStructFieldMap.Load(key); ok {
		return cached.([]strictStructRowField), nil
	}

	typeSet := make(map[reflect.Type]struct{})
	fieldStack := make([]int, 0, 1)
	result, err := computeStrictNamedStructFields(
		fldDescs,
		t,
		make(map[string]strictStructRowField),
		&fieldStack,
		typeSet,
	)
	if err != nil {
		return nil, err
	}

	fieldsIface, _ := strictNamedStructFieldMap.LoadOrStore(key, result)
	return fieldsIface.([]strictStructRowField), nil
}

func computeStrictNamedStructFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	fieldsByName map[string]strictStructRowField,
	fieldStack *[]int,
	typeSet map[reflect.Type]struct{},
) ([]strictStructRowField, error) {
	if _, ok := typeSet[t]; ok {
		return nil, fmt.Errorf("recursive struct embedding detected for type %s", t.String())
	}
	typeSet[t] = struct{}{}
	defer delete(typeSet, t)

	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)

	type embeddedInfo struct {
		fieldIndex int
		embedType  reflect.Type
	}
	var embeddeds []embeddedInfo

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.Anonymous {
			continue
		}
		sfType := sf.Type
		if sfType.Kind() == reflect.Ptr {
			sfType = sfType.Elem()
		}
		if sfType.Kind() == reflect.Struct {
			embeddeds = append(embeddeds, embeddedInfo{fieldIndex: i, embedType: sf.Type})
		}
	}

	for _, emb := range embeddeds {
		(*fieldStack)[tail] = emb.fieldIndex
		sf := t.Field(emb.fieldIndex)
		sfType := sf.Type
		isPtr := false
		if sfType.Kind() == reflect.Ptr {
			isPtr = true
			sfType = sfType.Elem()
		}
		if !isPtr {
			_, err := computeStrictNamedStructFields(
				fldDescs,
				sfType,
				fieldsByName,
				fieldStack,
				typeSet,
			)
			if err != nil {
				*fieldStack = (*fieldStack)[:tail]
				return nil, err
			}
		}
	}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i

		if sf.PkgPath != "" && !sf.Anonymous {
			continue
		}

		isAnonymousStruct := false
		if sf.Anonymous {
			sfType := sf.Type
			if sfType.Kind() == reflect.Ptr {
				sfType = sfType.Elem()
			}
			if sfType.Kind() == reflect.Struct {
				isAnonymousStruct = true
			}
		}
		if isAnonymousStruct {
			continue
		}

		dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
		if dbTagPresent {
			dbTag, _, _ = strings.Cut(dbTag, ",")
		}
		if dbTag == "-" {
			continue
		}

		colName := dbTag
		if !dbTagPresent {
			colName = sf.Name
		}

		normalize := !dbTagPresent
		fpos := fieldPosByNameCaseInsensitive(fldDescs, colName, normalize)

		pathCopy := append([]int(nil), *fieldStack...)
		nullable := isNullableType(sf.Type)

		sr := strictStructRowField{
			path:        pathCopy,
			columnIndex: fpos,
			nullable:    nullable,
			fieldName:   sf.Name,
			fieldType:   sf.Type,
		}

		key := strings.ToLower(colName)
		if !dbTagPresent {
			key = strings.ToLower(strings.ReplaceAll(colName, "_", ""))
		}
		fieldsByName[key] = sr
	}

	*fieldStack = (*fieldStack)[:tail]

	if len(*fieldStack) == 0 {
		result := make([]strictStructRowField, 0, len(fieldsByName))
		seenColumns := make(map[int]struct{})

		for _, sf := range fieldsByName {
			if sf.columnIndex == -1 {
				return nil, fmt.Errorf(
					"struct field %s has no corresponding column in result set (expected column %q)",
					sf.fieldName, fieldNameFromPath(sf, t),
				)
			}
			if _, exists := seenColumns[sf.columnIndex]; exists {
				continue
			}
			seenColumns[sf.columnIndex] = struct{}{}
			result = append(result, sf)
		}

		sort.Slice(result, func(i, j int) bool {
			return result[i].columnIndex < result[j].columnIndex
		})
		return result, nil
	}

	return nil, nil
}

func fieldNameFromPath(sf strictStructRowField, rootType reflect.Type) string {
	currentType := rootType
	var parts []string
	for _, idx := range sf.path {
		f := currentType.Field(idx)
		parts = append(parts, f.Name)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			currentType = f.Type
		} else if f.Anonymous && f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
			currentType = f.Type.Elem()
		} else {
			break
		}
	}
	return strings.Join(parts, ".")
}

func fieldPosByNameCaseInsensitive(fldDescs []pgconn.FieldDescription, field string, normalize bool) int {
	if normalize {
		field = strings.ReplaceAll(field, "_", "")
	}
	fieldLower := strings.ToLower(field)
	for i, desc := range fldDescs {
		descName := desc.Name
		if normalize {
			descName = strings.ReplaceAll(descName, "_", "")
		}
		if strings.ToLower(descName) == fieldLower {
			return i
		}
	}
	return -1
}

func isNullableType(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		return true
	}
	if t.Kind() == reflect.Interface {
		return true
	}
	if t.Kind() == reflect.Map || t.Kind() == reflect.Slice {
		return true
	}
	if hasValidBoolField(t) {
		return true
	}
	return false
}

func hasValidBoolField(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Name == "Valid" && f.Type.Kind() == reflect.Bool {
			return true
		}
	}
	return false
}

func setupStrictStructScanTargets(receiver any, fields []strictStructRowField) []any {
	scanTargets := make([]any, len(fields))
	v := reflect.ValueOf(receiver).Elem()
	for i, f := range fields {
		scanTargets[i] = v.FieldByIndex(f.path).Addr().Interface()
	}
	return scanTargets
}
