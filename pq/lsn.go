package pq

import (
	"fmt"

	"github.com/go-playground/errors"
)

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%08X", (uint64(lsn)>>32)&0xFFFFFFFF, uint64(lsn)&0xFFFFFFFF)
}

func ParseLSN(s string) (LSN, error) {
	var upperHalf, lowerHalf uint64
	var nparsed int

	nparsed, err := fmt.Sscanf(s, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return 0, errors.Wrap(err, "lsn parse")
	}

	if nparsed != 2 {
		return 0, errors.Newf("lsn parse: %s", s)
	}

	return LSN((upperHalf << 32) + lowerHalf), nil
}
