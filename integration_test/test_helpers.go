package integration

import "github.com/wclaeys/go-pq-cdc/pq/message/format"

// insertToMap converts a *format.Insert TupleData and Relation into a map keyed by column name.
func insertToMap(ins *format.Insert) map[string]any {
	m := make(map[string]any)
	if ins == nil || ins.Relation == nil {
		return m
	}
	for i, col := range ins.Relation.Columns {
		if i < len(ins.TupleData) {
			m[col.Name] = ins.TupleData[i]
		} else {
			m[col.Name] = nil
		}
	}
	return m
}
