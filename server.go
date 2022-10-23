package ssdb

import (
	"github.com/tidwall/redcon"
)

type Server struct {
	opts *Options

	mux *redcon.ServeMux
}

func NewServer(opts *Options) *Server {
	s := &Server{
		opts: opts,
		mux:  redcon.NewServeMux(),
	}

	s.mux.HandleFunc("ping", s.Ping)

	return s
}

func (s *Server) ListenAndServe() error {
	return redcon.ListenAndServe(s.opts.Addr,
		s.mux.ServeRESP,
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		},
	)
}

func (h *Server) Ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}
