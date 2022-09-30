package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"path"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/niukuo/ragit/api"
	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/gitexec"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"github.com/soheilhy/cmux"
	serverpb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/etcdserver/etcdserverpb/gw"
	etcdraft "go.etcd.io/etcd/raft"
	"google.golang.org/grpc"
)

func main() {

	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated peer urls, joined different node by semicolon")
	peer := flag.String("peer_listen_urls", "http://127.0.0.1:9021", "list of comma separated URLs to listen on for peer traffic")
	maxFileSize := flag.Int64("max_file_size", 1*1024*1024, "limit file size")
	maxPackSize := flag.Int64("max_pack_size", 10*1024*1024, "limit write file total size")

	flag.Parse()

	var peerListenURLs []string
	peerListenURLs = append(peerListenURLs, strings.Split(*peer, ",")...)

	myid := refs.NewMemberID(peerListenURLs, nil)

	dir := strings.Replace(myid.String(), ":", "_", -1)

	listener, err := gitexec.NewListener(path.Join(dir, "repo.git"), logging.GetLogger("gitexec"))
	if err != nil {
		log.Fatalln(err)
	}

	opts := bdb.NewOptions()
	opts.Listener = listener
	opts.Logger = logging.GetLogger("bdb")
	opts.NewLocalID = func() refs.PeerID {
		return refs.NewMemberID(peerListenURLs, nil)
	}

	storage, err := bdb.Open(dir, opts)
	if err != nil {
		log.Fatalln(err)
	}

	if hardState, confState, err := storage.InitialState(); err != nil {
		log.Fatalln(err)
	} else if etcdraft.IsEmptyHardState(hardState) && len(confState.Learners)+len(confState.Voters) == 0 {
		members := make([]refs.Member, 0)

		for _, node := range strings.Split(*cluster, ";") {
			urls := strings.Split(node, ",")
			members = append(members, refs.NewMember(
				refs.NewMemberID(urls, nil),
				urls,
			))
		}

		if err := storage.Bootstrap(members); err != nil {
			log.Fatalln("bootstrap failed: ", err)
		}
	}

	// raft provides a commit stream for the proposals from the http api
	c := raft.NewConfig()
	c.Config = etcdraft.Config{
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		PreVote:                   true,
	}
	c.Storage = storage
	c.StateMachine = storage
	c.PeerListenURLs = peerListenURLs

	node, err := raft.RunNode(c)
	if err != nil {
		log.Fatalln(err)
	}

	addr := strings.TrimPrefix(peerListenURLs[0], "http://")

	mux := http.NewServeMux()
	nh := node.Handler()
	mux.Handle("/raft", nh)
	mux.Handle("/raft/", nh)
	mux.Handle("/debug/", http.DefaultServeMux)
	mux.Handle("/refs/", api.NewHandler(storage, node))
	mux.Handle("/repo.git/", http.StripPrefix("/repo.git",
		api.NewGitHandler("repo.git", storage, node, logging.GetLogger("repo.git"), api.WithLimitSize(*maxFileSize, *maxPackSize))),
	)

	gwmux := runtime.NewServeMux()
	opt := []grpc.DialOption{grpc.WithInsecure()}
	if err := gw.RegisterClusterHandlerFromEndpoint(context.Background(), gwmux, addr, opt); err != nil {
		log.Fatalln(err)
	}

	mux.Handle("/v3/cluster/member/", gwmux)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln(err)
	}

	m := cmux.New(l)
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldPrefixSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	grpcServer := grpc.NewServer()
	serverpb.RegisterClusterServer(grpcServer, node.Service())

	httpServer := http.Server{
		Handler: LogHandler(mux),
	}

	go func() {
		if err := grpcServer.Serve(grpcL); err != nil {
			log.Fatalf("grpc.Serve err: %v", err)
		}
	}()

	go func() {
		if err := httpServer.Serve(httpL); err != nil {
			log.Fatalf("http.Serve err: %v", err)
		}
	}()

	m.Serve()
}

func LogHandler(def http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.Method, r.URL)
		def.ServeHTTP(w, r)
	})
}
