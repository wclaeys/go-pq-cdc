package format

import "github.com/wclaeys/go-pq-cdc/pq"

type WALMessage interface {
	GetLSN() pq.LSN
	SetLSN(pq.LSN)
}
