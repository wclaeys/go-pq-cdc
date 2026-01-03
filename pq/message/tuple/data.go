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

func NewData(data []byte, tupleDataType uint8, skipByteLength int, columns []RelationColumn, decodeData bool, toastedValuesBackup []any) ([]any, int, error) {
	if data[skipByteLength] != tupleDataType {
		return nil, skipByteLength, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	columnNumber := binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	tupleData := make([]any, columnNumber)

	for i := range columns {
		dataType := data[skipByteLength]
		skipByteLength++

		switch dataType {
		case DataTypeNull:
			tupleData[i] = nil
		case DataTypeToast:
			// no payload
			if toastedValuesBackup != nil {
				tupleData[i] = toastedValuesBackup[i]
			}
		case DataTypeText, DataTypeBinary:
			length := binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			if decodeData && dataType == DataTypeText {
				val, err := decodeColumnData(data[skipByteLength:skipByteLength+int(length)], columns[i].DataType)
				if err != nil {
					return nil, skipByteLength, errors.Wrap(err, "decode column")
				}
				tupleData[i] = val
			} else {
				tupleData[i] = data[skipByteLength : skipByteLength+int(length)]
			}
			skipByteLength += int(length)

		default:
			return nil, skipByteLength, errors.Newf("unknown tuple data type %q at offset %d", dataType, skipByteLength-1)
		}
	}

	return tupleData, skipByteLength, nil
}

func GetDecodedValue(tupleData []any, columns []RelationColumn, columnIndex int) (any, error) {
	if tupleData == nil || len(tupleData) <= columnIndex {
		return nil, nil
	}
	value := tupleData[columnIndex]
	if value == nil {
		return nil, nil
	}
	switch b := (value).(type) {
	case []byte:
		decodedValue, err := decodeColumnData(b, columns[columnIndex].DataType)
		if err != nil {
			return nil, errors.Newf("unable to decode delete message column %d: %v", columnIndex, err)
		}
		return decodedValue, nil
	default:
		return value, nil
	}
}

func decodeColumnData(data []byte, dataType uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
