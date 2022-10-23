package ssdb

import (
	"context"
	"fmt"

	"github.com/tidwall/redcon"
)

type Server struct {
	opts *Options

	db *DB

	mux *redcon.ServeMux
}

func NewServer(opts *Options) (*Server, error) {
	db, err := Open(opts.Path, opts)
	if err != nil {
		return nil, err
	}

	s := &Server{
		opts: opts,
		db:   db,
		mux:  redcon.NewServeMux(),
	}

	s.mux.HandleFunc("ping", s.Ping)
	s.mux.HandleFunc("set", s.Set)
	s.mux.HandleFunc("get", s.Get)

	return s, nil
}

func (s *Server) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
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

func (s *Server) Ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (s *Server) Set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for command")
		return
	}
	ctx := context.TODO()
	if err := s.db.Set(ctx, cmd.Args[1], cmd.Args[2]); err != nil {
		conn.WriteError(fmt.Sprintf("ERR %s\n", err))
		return
	}
	conn.WriteString("OK")
}

func (s *Server) Get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for command")
		return
	}
	ctx := context.TODO()
	value, err := s.db.Get(ctx, cmd.Args[1])
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR %s\n", err))
		return
	}
	conn.WriteBulk(value)
}
