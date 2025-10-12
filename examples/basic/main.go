package main

import "pkt.systems/lockd"

func main() {
	cfg := lockd.Config{Store: "mem://"}
	srv, err := lockd.NewServer(cfg)
	if err != nil {
		panic(err)
	}
	defer srv.Close()
}
