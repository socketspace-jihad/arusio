package main

import (
	"github.com/rs/zerolog"
	"github.com/socketspace-jihad/arusio/broker"
	"github.com/socketspace-jihad/arusio/handler"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	go handler.Serve(":3030")
	broker.Serve(":3031")
}
