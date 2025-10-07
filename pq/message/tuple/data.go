package tuple

import (
	"encoding/binary"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

// Tuple data types:
// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TUPLEDATA
const (
	DataTypeNull   = uint8('n')
	DataTypeToast  = uint8('u')
	DataTypeText   = uint8('t')
	DataTypeBinary = uint8('b')
)

var typeMap = pgtype.NewMap()

type RelationColumn struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Flags        uint8
}

func NewData(data []byte, tupleDataType uint8, skipByteLength int, columns []RelationColumn, toastedValuesBackup map[string]any) (map[string]any, int, error) {
	if data[skipByteLength] != tupleDataType {
		return nil, skipByteLength, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	columnNumber := binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	decoded := make(map[string]any, columnNumber)

	for _, col := range columns {
		dataType := data[skipByteLength]
		skipByteLength++

		switch dataType {
		case DataTypeNull:
			decoded[col.Name] = nil
		case DataTypeToast:
			// no payload
			if toastedValuesBackup != nil {
				if val, ok := toastedValuesBackup[col.Name]; ok {
					decoded[col.Name] = val
				}
			}
		case DataTypeText, DataTypeBinary:
			length := binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			if dataType == DataTypeText {
				val, err := decodeTextColumnData(data[skipByteLength:skipByteLength+int(length)], col.DataType)
				if err != nil {
					return nil, skipByteLength, errors.Wrap(err, "decode column")
				}
				decoded[col.Name] = val
			} else {
				decoded[col.Name] = data[skipByteLength : skipByteLength+int(length)]
			}
			skipByteLength += int(length)

		default:
			return nil, skipByteLength, errors.Newf("unknown tuple data type %q at offset %d", dataType, skipByteLength-1)
		}
	}

	return decoded, skipByteLength, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
