/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"database/sql"

	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"github.com/xyproto/simpleredis"
)

var (
	masterPool *simpleredis.ConnectionPool
	slavePool  *simpleredis.ConnectionPool
	db         *sql.DB
)

var separator = "###"
var table = pq.QuoteIdentifier("guestbooks")

func ListRangeHandler(rw http.ResponseWriter, req *http.Request) {
	key := mux.Vars(req)["key"]
	list := simpleredis.NewList(slavePool, key)
	members, err := list.All()
	if err != nil {
		// cache miss
		members = getFromDB()
		// update cache
		list := simpleredis.NewList(masterPool, key)
		list.Clear()
		for _, member := range members {
			list.Add(member)
		}
	}

	membersJSON := HandleError(json.MarshalIndent(members, "", "  ")).([]byte)
	rw.Write(membersJSON)
}

func ListPushHandler(rw http.ResponseWriter, req *http.Request) {
	key := mux.Vars(req)["key"]
	value := mux.Vars(req)["value"]
	values := getFromDB()
	values = append(values, value)
	writeToDB(values)
	// invalidate cache
	list := simpleredis.NewList(masterPool, key)
	list.Clear()
	ListRangeHandler(rw, req)
}

func InfoHandler(rw http.ResponseWriter, req *http.Request) {
	info := HandleError(masterPool.Get(0).Do("INFO")).([]byte)
	rw.Write(info)
}

func EnvHandler(rw http.ResponseWriter, req *http.Request) {
	environment := make(map[string]string)
	for _, item := range os.Environ() {
		splits := strings.Split(item, "=")
		key := splits[0]
		val := strings.Join(splits[1:], "=")
		environment[key] = val
	}

	envJSON := HandleError(json.MarshalIndent(environment, "", "  ")).([]byte)
	rw.Write(envJSON)
}

func HandleError(result interface{}, err error) (r interface{}) {
	if err != nil {
		panic(err)
	}
	return result
}

func writeToDB(arr []string) {
	data := strings.Join(arr, separator)
	_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (values) VALUES ($1)", table), data)
	if err != nil {
		panic("Fail to insert into database: " + err.Error())
	}
}

func getFromDB() []string {
	rows, err := db.Query("SELECT VALUES FROM guestbooks ORDER BY ID DESC LIMIT 1")
	if err != nil {
		panic("Fail to read database: " + err.Error())
	}
	for rows.Next() {
		var values string
		err = rows.Scan(&values)
		if err != nil {
			panic("Fail to read values: " + err.Error())
		}
		return strings.Split(values, separator)
	}
	return []string{}
}

func main() {
	masterPool = simpleredis.NewConnectionPoolHost("redis-master:6379")
	defer masterPool.Close()
	slavePool = simpleredis.NewConnectionPoolHost("redis-slave:6379")
	defer slavePool.Close()
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), os.Getenv("POSTGRES_DB"),
		os.Getenv("DISCOURSE_DB_HOST"), os.Getenv("DISCOURSE_DB_PORT")) // ENV
	var err error
	db, err = sql.Open("postgres", dbinfo)
	if err != nil {
		panic("Fail to connect to database: " + err.Error())
	}

	fmt.Println(db)
	r := mux.NewRouter()
	r.Path("/lrange/{key}").Methods("GET").HandlerFunc(ListRangeHandler)
	r.Path("/rpush/{key}/{value}").Methods("GET").HandlerFunc(ListPushHandler)
	r.Path("/info").Methods("GET").HandlerFunc(InfoHandler)
	r.Path("/env").Methods("GET").HandlerFunc(EnvHandler)

	n := negroni.Classic()
	n.UseHandler(r)
	n.Run(":3000")
}
