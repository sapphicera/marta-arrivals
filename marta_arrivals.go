package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/joho/godotenv"
	"github.com/replit/database-go"
)

var out *os.File

var (
	baseUrl    = "http://developer.itsmarta.com/RealtimeTrain/RestServiceNextTrain/GetRealtimeArrivals?apikey=%s"
	reqUrl     = getEnv()
	httpClient = retryablehttp.NewClient()
)

var (
	timeStrLayout = "2006-01-02 15:04:05.999999999 -0700 MST"
	lastWriteKey  = "lastWrite"
)

type Stop struct {
	Train_Id        string `json:"train_id"`
	Station         string `json:"station"`
	Waiting_Seconds string `json:"waiting_seconds"`
}

func getEnv() string {
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("error loading .env file")
	}
	return fmt.Sprintf(baseUrl, os.Getenv("API_KEY"))
}

func main() {
	var err error
	out, err = os.OpenFile("out.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// might need to os.quit if this fails...
	if err != nil {
		fmt.Printf("out.log failure")
	}
	defer out.Close()

	// added 15-retry timeout for retryablehttp
	setTimeout()

	lastWriteFinished := make(chan bool)

	for {
		lastWrite, err := getLastWriteTime()
		if err != nil {
			lastWrite = time.Now()
			setLastWriteTime(lastWrite)
		}

		if time.Now().After(lastWrite) {
			// attempt to write stop info and check if process completed/failed
			checkWritten(lastWriteFinished)
			chanResult := <-lastWriteFinished
			if chanResult != true {
				// if failed, alerts user that the write did not complete
				err = errors.New("write not completed")
				fmt.Println(err)
			}
			time.Sleep(10 * time.Second)
		}
	}
}

// holds go routine that determines if write was completed/incomplete
// logs error that went wrong if incomplete and pushes back to main()
func checkWritten(lastWriteFinished chan bool) {
	go func() {
		result, err := writeFilteredStops(lastWriteFinished)
		if result == true {
			lastWriteFinished <- true
		} else {
			fmt.Println(err)
			lastWriteFinished <- false
		}
	}()
}

// access the stop data, filter it, and write it to out.log
func writeFilteredStops(finished chan bool) (result bool, err error) {
	stops, err := fetchStops()
	if err != nil {
		return false, err
	}
	filtered, err := filterStops(stops)
	if err != nil {
		return false, err
	}
	if len(filtered) == 0 {
		return false, errors.New("no data to log")
	}

	marshalled, err := json.Marshal(filtered)
	if err != nil {
		return false, errors.New("can't be marshalled")
	}

	out.Write(append(marshalled, '\n'))
	out.Sync()
	setLastWriteTime(time.Now())

	return true, nil
}

// creates new slice which holds stop information below 120 seconds
func filterStops(stops []Stop) ([]Stop, error) {
	var filteredSlice []Stop
	for i, v := range stops {
		intconv, err := strconv.Atoi(stops[i].Waiting_Seconds)
		if err != nil {
			return stops, errors.New("string to int failure")
		}
		if intconv < 120 {
			filteredSlice = append(filteredSlice, v)
		}
	}
	return filteredSlice, nil
}

// fetches stop information and create errors if any service fails
func fetchStops() (stops []Stop, err error) {
	resp, err := httpClient.Get(reqUrl)
	if err != nil {
		return nil, errors.New("server unreachable")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("can't read content")
	}
	if err = json.Unmarshal(body, &stops); err != nil {
		return nil, errors.New("can't unmarshal json")
	}

	return stops, nil
}

func setLastWriteTime(t time.Time) {
	database.Set(lastWriteKey, t.Format(timeStrLayout))
}

func getLastWriteTime() (time.Time, error) {
	lastWriteStr, _ := database.Get(lastWriteKey)
	return time.Parse(timeStrLayout, lastWriteStr)
}

// sets a 15 retry limit for retryablehttp before creating an error
func setTimeout() {
	httpClient.RetryMax = 15
	httpClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		ok, e := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		if !ok && resp.StatusCode == http.StatusRequestTimeout {
			return true, nil
		}
		return ok, e
	}
}
