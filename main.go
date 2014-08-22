package main

import (
	"database/sql"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/300brand/logger"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

type RSS struct {
	XMLName xml.Name `xml:"rss"`
	Channel Channel  `xml:"channel"`
}

type Channel struct {
	Title string `xml:"title"` // Required. Defines the title of the channel
	Item  []Item `xml:"item"`  // Optional. Stories within the feed
}

type Item struct {
	Guid    string    `xml:"guid"`    // Optional. Defines a unique identifier for the item
	Link    string    `xml:"link"`    // Required. Defines the hyperlink to the item
	PubDate time.Time `xml:"pubDate"` // Optional. Defines the last-publication date for the item
	Source  string    `xml:"source"`  // Optional. Specifies a third-party source for the item
	Title   string    `xml:"title"`   // Required. Defines the title of the item
}

var (
	Listen   = flag.String("listen", ":8080", "HTTP Listen Addr")
	MySQLDSN = flag.String("mysql", "root:@tcp(localhost:49161)/spider?parseTime=true", "MySQL DSN")
)

var _ http.HandlerFunc = serveRSS

func serveRSS(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open("mysql", *MySQLDSN)
	if err != nil {
		logger.Error.Printf("sql.Open: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	ident := mux.Vars(r)["ident"]
	var limit int
	if limit, err = strconv.Atoi(r.FormValue("limit")); err != nil {
		limit = 50
	}

	query := fmt.Sprintf("SELECT url, title, added FROM `%s` WHERE queue = 'PROCESSED' ORDER BY id DESC LIMIT %d", ident, limit)
	rows, err := db.Query(query)
	if err != nil {
		mysqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			logger.Error.Printf("err.(MySQLError): could not cast: %s", err)
			http.Error(w, "Cast exception", http.StatusInternalServerError)
			return
		}
		if mysqlErr.Number == 1146 {
			http.NotFound(w, r)
			logger.Error.Printf("Could not find RSS data for %s", ident)
			return
		}
		logger.Error.Printf("db.Query: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rss := new(RSS)
	rss.Channel.Title = "RSS Feed for " + ident
	rss.Channel.Item = make([]Item, 0, limit)

	var firstDate time.Time
	for rows.Next() {
		i := Item{Source: "Ocular8 Spider"}
		if err := rows.Scan(&i.Link, &i.Title, &i.PubDate); err != nil {
			logger.Error.Printf("rows.Scan: %s", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if firstDate.IsZero() {
			firstDate = i.PubDate
		}
		// Knock down to whole seconds
		i.PubDate = i.PubDate.Add(time.Duration(-i.PubDate.Nanosecond()))
		i.Guid = i.Link
		rss.Channel.Item = append(rss.Channel.Item, i)
	}

	w.Header().Add("Content-Type", "application/rss+xml")
	w.Header().Add("Last-Modified", firstDate.Format(time.RFC1123))
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "\t")
	enc.Encode(rss)
}

func main() {
	flag.Parse()

	router := mux.NewRouter()
	router.HandleFunc("/{ident}.rss", serveRSS)
	logger.Error.Fatal(http.ListenAndServe(*Listen, handlers.CombinedLoggingHandler(os.Stdout, router)))
}
