package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

type Option struct {
	Symbol    string `json:"symbol"`
	Interval  string `json:"interval"`
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
}

func main() {
	now := time.Now().Unix() * 1000
	duration := 3600 * 1000 // 3600 seconds
	//temp
	now = 1715747119000
	startTime := now - int64(duration)
	endtime := now
	skipHeader := false
	for {

		// init data
		options := Option{
			Symbol:    "btcthb",
			Interval:  "1m",
			StartTime: startTime,
			EndTime:   endtime,
		}
		params := map[string]interface{}{}
		dataByte, _ := json.Marshal(options)
		json.Unmarshal(dataByte, &params)

		qs := constructQueryStringWithPrefix(params, "")
		url := "https://api.binance.th/api/v1/klines"

		millis := int64(startTime)
		// Convert milliseconds to seconds and nanoseconds
		seconds := millis / 1000
		nanoseconds := (millis % 1000) * int64(time.Millisecond)
		timestamp := time.Unix(seconds, nanoseconds)
		formattedTime := timestamp.Format("02/01/2006 15:04:05")
		fmt.Println("\nstart time", formattedTime, " call ", url+"?"+qs)
		req, err := http.NewRequest("GET", url+"?"+qs, nil)
		if err != nil {
			fmt.Println(err.Error())

		}

		req.Header.Add("Content-Type", "application/json")
		clientHttp := http.Client{}
		resp, _ := clientHttp.Do(req)
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println(err.Error())

		}
		if resp.StatusCode != 200 {
			fmt.Println(resp)
			break
		}
		fmt.Println(resp.Status)

		//write file
		var dataObj [][]interface{}
		json.Unmarshal(body, &dataObj)
		writeFile(dataObj, skipHeader)
		skipHeader = true
		endtime = startTime
		startTime = endtime - int64(duration)

		time.Sleep(time.Second * 2)
	}

}

func constructQueryStringWithPrefix(params map[string]interface{}, prefix string) string {
	var keys []string

	for k, _ := range params {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var qs string

	for i, k := range keys {
		v := params[k]

		if nestedParams, ok := v.(map[string]interface{}); ok {
			qs += constructQueryStringWithPrefix(nestedParams, k)
		} else if array, ok := v.([]interface{}); ok {
			logrus.Errorf("bar")
			nestedMap := map[string]interface{}{}
			for i, v := range array {
				nestedMap[fmt.Sprintf("%d", i)] = v
			}
			qs += constructQueryStringWithPrefix(nestedMap, k)
		} else {
			if prefix == "" {
				if _, ok := v.(float64); ok {
					qs += fmt.Sprintf("%s=%.f", k, v)
				} else {
					qs += fmt.Sprintf("%s=%v", k, v)
				}
			} else {
				if _, ok := v.(float64); ok {
					qs += fmt.Sprintf("%s[%s]=%.f", prefix, k, v)
				} else {
					qs += fmt.Sprintf("%s[%s]=%v", prefix, k, v)
				}
			}
		}

		if i != len(keys)-1 {
			qs += "&"
		}
	}

	return qs
}

func writeFile(dataObjs [][]interface{}, skipHeader bool) {

	file, err := os.OpenFile("klines.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening the file:", err)
		return
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()
	// Write the CSV header
	if !skipHeader {
		header := []string{"Open time", "Open", "High", "Low", "Close",
			"Volume", "Close time", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume"}

		err = writer.Write(header)
		if err != nil {
			panic(err)
		}
	}
	// Write each struct instance as a CSV row
	for _, data := range dataObjs {

		openTime := strconv.FormatFloat(data[0].(float64), 'f', 0, 64)
		closeTime := strconv.FormatFloat(data[6].(float64), 'f', 0, 64)
		NoT := strconv.FormatFloat(data[8].(float64), 'f', 0, 64)

		row := []string{openTime, data[1].(string), data[2].(string), data[3].(string), data[4].(string),
			data[5].(string), closeTime, data[7].(string), NoT, data[9].(string), data[10].(string),
		}
		err := writer.Write(row)
		if err != nil {
			panic(err)
		}
	}
}
