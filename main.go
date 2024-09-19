package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog/log"
)

type Response struct {
	S bool `json:"s"`
	D []struct {
		ParentNo    string  `json:"parentNo"`
		Amount      float64 `json:"amount"`
		Lvl         int     `json:"lvl"`
		IsParent    bool    `json:"isParent"`
		AccountName string  `json:"accountName"`
		AccountNo   string  `json:"accountNo"`
		AccountType string  `json:"accountType"`
	} `json:"d"`
}

func main() {
	var token, signature string
	token = "aat.NTA.eyJ2IjoxLCJ1Ijo2OTMzNjEsImQiOjEzMDYyMzYsImFpIjo0ODY1NiwiYWsiOiI4YzZlYTdjMy1kNzUzLTQxZmYtOTU4NS05OGU4ODdmOGEyM2QiLCJhbiI6IkV4YXBvcyIsImFwIjoiOGNjYTU4ZGItNWVlOS00Yjk1LTliZWItMzI3MDhiZWQwN2NlIiwidCI6MTcyNDMxODI0OTAzM30.NPXpCZSHAsdyII+4c4dqXdQGXqs+wduJTaBqkKK0qJD6/qyMErHK9kEH88XgPQKS55BcJgI+gYDMe01Ehi2MXj+VzOQQmIAfYkkXBc30JAIqHDnMQYnX0srWEG7EFx7two8w9nI95YNaPs3eWWSRselnQ4HFqs8Dflz+Us2UnVUdZpHjQ5fdTCWFTy/as79f1jIgWUaqrrw=.jpdKG8kW0z8o8CQFJqZ8oom+MphrE3jLI7fMLTiT09M"
	signature = "M4a8mtwbvm3AJOtXizCk9lFVZEn8L330X8Z2RR8IiUdYnIBSoEmBA5zknbUOUgRV"

	var client = &http.Client{
		Timeout: time.Duration(300) * time.Second,
	}

	request, err := http.NewRequest("GET", "https://zeus.accurate.id/accurate/api/glaccount/get-pl-account-amount.do", nil)
	if err != nil {
		log.Print(err)
		return
	}

	loc, _ := time.LoadLocation("Asia/Jakarta")
	execTime := time.Now().In(loc).Format("02/01/2006 15:04:05")

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Api-Timestamp", execTime)
	request.Header.Set("X-Api-Signature", HMACSHA256String(execTime, signature))

	qParam := request.URL.Query()
	qParam.Add("fromDate", "01/09/2024")
	qParam.Add("toDate", "30/09/2024")
	request.URL.RawQuery = qParam.Encode()

	log.Debug().Msgf("invoke api")
	response, err := client.Do(request)
	if err != nil {
		log.Print(err)
		return
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Print(err)
		return
	}
	buf := bytes.NewBuffer(body)

	bufFile := bytes.NewReader(buf.Bytes())
	out, err := os.Create("response.json")
	if err != nil {
		log.Print(err)
		return
	}
	defer out.Close()
	io.Copy(out, bufFile)

	var resp Response
	bufJson := bytes.NewReader(buf.Bytes())
	err = json.NewDecoder(bufJson).Decode(&resp)
	if err != nil {
		log.Print(err)
		return
	}

	log.Print(resp)

	DB, err := sql.Open("duckdb", "")
	if err != nil {
		log.Error().Msg("cannot open duckdb connection " + err.Error())
		return
	}
	defer DB.Close()

	log.Debug().Msg("Exec: Install & Load httpfs")
	_, err = DB.Exec(`INSTALL httpfs;
		LOAD httpfs;
		CREATE SECRET my_secret (
			TYPE S3,
			KEY_ID '` + os.Getenv("KEY_ID") + `',
			SECRET '` + os.Getenv("SECRET") + `',
			URL_STYLE 'path',
			ENDPOINT '` + os.Getenv("ENDPOINT") + `');`)
	if err != nil {
		log.Error().Msg("Install httpfs, secret Failed " + err.Error())
		return
	}

	log.Debug().Msg("Exec: Create table")
	_, err = DB.Exec(`CREATE table pl (ParentNo varchar, Amount double, Lvl integer, IsParent boolean, AccountName varchar, AccountNo varchar, AccountType varchar);`)
	if err != nil {
		log.Error().Msg("Create table Failed " + err.Error())
		return
	}

	for _, v := range resp.D {
		log.Debug().Msg("Exec: Insert into duckdb")
		_, err = DB.Exec(`INSERT INTO pl VALUES ($1, $2, $3, $4, $5, $6, $7);`, v.ParentNo, v.Amount, v.Lvl, v.IsParent, v.AccountName, v.AccountNo, v.AccountType)
		if err != nil {
			log.Error().Msg("Insert into duckdb Failed " + err.Error())
			return
		}
	}

	log.Debug().Msg("Exec: Create parquet")
	_, err = DB.Exec(`COPY pl TO 's3://test/pl.parquet';`)
	if err != nil {
		log.Error().Msg("Create parquet Failed " + err.Error())
		return
	}
}
