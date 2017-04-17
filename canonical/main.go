package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"encoding/json"
)

func main() {
	m := getPlaceIDMap()

	data, err := ioutil.ReadFile("./active_hotels_with_phone_website_placeid_06_Apr_2017.csv")
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(data))

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	var ids []string
	for i, record := range records {
		// Skip the header
		if i == 0 {
			continue
		}

		// 0 short_id,
		// 1 name,
		// 2 country_code,
		// 3 address_1,
		// 4 address_2,
		// 5 city,
		// 6 state_province,
		// 7 postal_code,
		// 8 latitude,
		// 9 longitude,
		// 10 phone,
		// 11 website
		// 12 place_id
		// 13 reason

		if record[12] == "" {
			continue
		}

		if m[record[12]] {
			continue
		}

		ids = append(ids, record[0])
		if len(ids) == 1000 {
			break
		}
	}

	data1, _ := json.Marshal(ids)

	fmt.Println(string(data1))
}

func getPlaceIDMap() map[string]bool {
	data, err := ioutil.ReadFile("../scroll/dat1.json")
	if err != nil {
		log.Fatal(err)
	}	

	var placeIDs []string
	err = json.Unmarshal(data, &placeIDs)
	if err != nil {
		log.Fatal(err)
	}

	m := make(map[string]bool)
	for _, placeID := range placeIDs {
		m[placeID] = true
	}

	return m
}