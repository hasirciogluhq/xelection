package utils

import (
	"sync"
	"time"
)

const (
	sfNodeID        int64 = 1             // tek node id, istersen değiştirirsin
	sfEpoch         int64 = 1700000000000 // custom epoch (ms)
	sfTimestampBits       = 41
	sfNodeBits            = 10
	sfSeqBits             = 12

	sfMaxSeq    = -1 ^ (-1 << sfSeqBits)
	sfNodeShift = sfSeqBits
	sfTimeShift = sfSeqBits + sfNodeBits
)

var (
	sfMu       sync.Mutex
	sfLastTs   int64
	sfSequence int64
)

// hızlı timestamp (ms)
func sfNow() int64 {
	return time.Now().UnixNano() / 1e6
}

// aynı timestamp'ta seq overflow olduğunda 1 ms bekletiyor (spin-wait)
func sfWaitNextMs(ts int64) int64 {
	for {
		now := sfNow()
		if now > ts {
			return now
		}
	}
}

// dışarıdan kullanacağın tek fonksiyon
func GenerateSnowflakeID() int64 {
	sfMu.Lock()
	defer sfMu.Unlock()

	ts := sfNow()

	// clock geri gitti ise fallback
	if ts < sfLastTs {
		ts = sfLastTs
	}

	if ts == sfLastTs {
		// aynı ms içinde -> sequence artır
		sfSequence = (sfSequence + 1) & sfMaxSeq

		if sfSequence == 0 {
			// sequence tükendi -> bir sonraki ms'i bekle
			ts = sfWaitNextMs(ts)
		}
	} else {
		// yeni timestamp geldi -> sequence reset
		sfSequence = 0
	}

	sfLastTs = ts

	// bit shift ile ID oluştur
	id := ((ts - sfEpoch) << sfTimeShift) | (sfNodeID << sfNodeShift) | sfSequence
	return id
}
