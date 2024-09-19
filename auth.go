package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func HMACSHA256String(data, key string) string {
	var ret string
	hmac := hmac.New(sha256.New, []byte(key))

	hmac.Write([]byte(data))
	dataHmac := hmac.Sum(nil)

	ret = hex.EncodeToString(dataHmac)

	return ret
}
