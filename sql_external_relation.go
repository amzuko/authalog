package authalog

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type SQLExternalRelationSpec struct {
	Table   string
	Columns []string
	// While labeled 'types', we actually pass example interfaces.
	// EG:
	// 		Types: []interface{}{0, "", MyEnumValue}
	// For a database relation that has a tuple of types (int, string, MyEnum).
	Types []interface{}
}

func sqlQueryForTerms(intern interner, spec SQLExternalRelationSpec, terms []Term) (string, []interface{}) {
	query := fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(spec.Columns, ", "), spec.Table)

	hasWhere := false
	for _, t := range terms {
		if t.IsConstant {
			hasWhere = true
		}
	}
	args := []interface{}{}
	whered := 0
	if hasWhere {
		query = query + " WHERE "
		for i, t := range terms {
			if t.IsConstant {
				if whered > 0 {
					query = query + " AND "
				}
				query = query + fmt.Sprintf("%s = $%d", spec.Columns[i], whered)

				str := intern.lookup(t.Value)
				switch spec.Types[i].(type) {
				case string:
					args = append(args, str)
				case int:
					// TODO should we parse it
					args = append(args, str)
				default:
					v := reflect.New(reflect.TypeOf(spec.Types[i]))
					_ = v.MethodByName("Scan").Call([]reflect.Value{reflect.ValueOf(str)})
					args = append(args, v.Interface())
				}

				whered = whered + 1
			}
		}
	}
	query = query + ";"

	trace("Query", query, "Args", args)
	return query, args
}

func makeVars(n int) []Term {
	r := make([]Term, n)
	for i := 0; i < n; i++ {
		r[i].Value = int64(i)
	}
	return r
}

func CreateSQLExternalRelation(spec SQLExternalRelationSpec, db *sql.DB) (ExternalRelation, error) {

	rt := make([]reflect.Type, len(spec.Types))
	for i, t := range spec.Types {
		rt[i] = reflect.TypeOf(t)
	}
	runner := func(in interner, terms []Term) ([][]Term, error) {
		q, args := sqlQueryForTerms(in, spec, terms)
		rows, err := db.Query(q, args...)
		if err == sql.ErrNoRows {
			return [][]Term{}, nil
		}
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var results [][]Term

		destination := make([]interface{}, len(rt))
		destinationPointers := make([]interface{}, len(destination))
		for i, v := range rt {
			destination[i] = reflect.New(v).Elem().Interface()
			destinationPointers[i] = &destination[i]
		}
		for rows.Next() {
			rows.Scan(destinationPointers...)

			r := make([]Term, len(destination))
			for i, d := range destination {
				var stringValue string
				asT := reflect.ValueOf(d).Convert(rt[i]).Interface()

				stringValue = fmt.Sprint(asT)

				v := in.intern(stringValue)

				r[i] = Term{IsConstant: true, Value: v}
			}
			results = append(results, r)
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
		return results, err
	}

	return ExternalRelation{
		head: Literal{
			Predicate: spec.Table,
			Terms:     makeVars(len(spec.Columns)),
		},
		run: runner,
	}, nil
}
