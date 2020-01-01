package main

import (
	"baldb/structure"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var nodes []structure.Node

func main() {

	db, err := sql.Open("mysql", "root:jetty$12@/myntra_mfp")

	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	rows, err := db.Query("SELECT organisation, business_unit, brand_group, brand , master_category, gender, article, gmv  FROM myntra_mfp.annual_plan_month")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Make a slice for the values
	values := make([]string, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	nodes := make([]structure.Node, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}

		// Now do something with the data.
		// Here we just print each column as a string.
		var value float64
		var key string
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if columns[i] == "gmv" {
				value, err = strconv.ParseFloat(col, 64)

			} else {
				key += ":" + col

			}

			// fmt.Println(columns[i], ": ", value)
		}
		rnode := structure.Node{Key: key, Value: value}
		rnode.Hash = rnode.ComputeHash()
		nodes = append(nodes, rnode)
		// fmt.Println("-----------------------------------")
	}
	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	var wg sync.WaitGroup
	var wgRead sync.WaitGroup
	var writeChannel = make(chan float64, 50)
	var quitChannel = make(chan bool, 2)

	workers := []*structure.Worker{}
	for j := 0; j < 100000; j++ {
		w := structure.Worker{Node: &nodes[j]}
		workers = append(workers, &w)

		w.Run(&wg, writeChannel, &wgRead)
	}
	wgRead.Add(100000)
	go func() {
		wgRead.Wait()
		fmt.Println(" Done .. summing...................")
		quitChannel <- true
	}()
	// editData := structure.EditData{Value: nodes[51].Value, Hash: nodes[51].Key}
	fmt.Println("match", nodes[100001].Key, strings.Join(strings.Split(nodes[100001].Key, ":")[1:4], ":"))
	ch := make(chan structure.ReadQuery)
	readQuery := structure.ReadQuery{Hash: strings.Join(strings.Split(nodes[100001].Key, ":")[1:4], ":")}
	go func() {
		wg.Add(1)
		for {
			select {
			case msg := <-ch:
				fmt.Println(" start .. wrtting", msg, len(workers))
				for k := 0; k < len(workers); k++ {
					workers[k].NRead <- msg
				}
				fmt.Println(" Done .. wrtting...........")
				wg.Done()
				fmt.Println(" Done .. wg.................")
				break
			}
		}
	}()
	sum := 0.0
	go func() {
		wg.Add(1)

		for {

			select {
			case wValue := <-writeChannel:
				sum += wValue
			case <-quitChannel:
				wg.Done()
				break
			}
		}

	}()
	// ch := make(chan structure.EditData)
	// fmt.Println("len", len(workers))

	// go func() {
	// 	wg.Add(1)
	// 	defer wg.Done()
	// 	for {
	// 		select {
	// 		case msg := <-ch:
	// 			for k := 0; k < len(workers); k++ {
	// 				fmt.Println(k)
	// 				workers[k].Npublsih <- msg
	// 			}
	// 			return
	// 		}
	// 	}
	// }()
	// ch <- editData
	ch <- readQuery

	wg.Wait()
	fmt.Println("Total ......sum.....................", sum)
}
