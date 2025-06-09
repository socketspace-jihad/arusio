package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/socketspace-jihad/arusio/broker"
	"github.com/socketspace-jihad/arusio/handler"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		if err := http.ListenAndServe(":8080", mux); err != nil {
			panic(err)
		}
	}()

	go handler.Serve(":3030")
	broker.Serve(":3031")
}
