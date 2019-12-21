package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"path"
	"strings"

	"github.com/niukuo/ragit/api"
	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/gitexec"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/raft"
	etcdraft "go.etcd.io/etcd/raft"
)

func main() {
	cluster := flag.String("cluster", "127.0.0.1:9021", "comma separated cluster peers")
	id := flag.String("id", "127.0.0.1:9021", "node ID")
	flag.Parse()

	myid, err := raft.ParsePeerID(*id)
	if err != nil {
		log.Fatalln(err)
	}

	peers := make([]raft.PeerID, 0)
	for _, peer := range strings.Split(*cluster, ",") {
		id, err := raft.ParsePeerID(peer)
		if err != nil {
			log.Fatalln(err)
		}
		peers = append(peers, id)
	}

	dir := strings.Replace(myid.String(), ":", "_", -1)

	listener, err := gitexec.NewListener(path.Join(dir, "repo.git"), logging.GetLogger("gitexec"))
	if err != nil {
		log.Fatalln(err)
	}

	opts := bdb.NewOptions()
	opts.Listener = listener
	opts.Logger = logging.GetLogger("")

	storage, err := bdb.Open(dir, opts)
	if err != nil {
		log.Fatalln(err)
	}

	if hardState, confState, err := storage.InitialState(); err != nil {
		log.Fatalln(err)
	} else if etcdraft.IsEmptyHardState(hardState) && confState.Size() == 0 {
		if err := storage.Bootstrap(peers); err != nil {
			log.Fatalln("bootstrap failed: ", err)
		}
	}

	// raft provides a commit stream for the proposals from the http api
	c := raft.NewConfig()
	c.Config = etcdraft.Config{
		ID:                        uint64(myid),
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

	node, err := raft.RunNode(c)
	if err != nil {
		log.Fatalln(err)
	}

	mux := http.NewServeMux()
	nh := node.Handler()
	mux.Handle("/raft", nh)
	mux.Handle("/raft/", nh)
	mux.Handle("/debug/", http.DefaultServeMux)
	mux.Handle("/refs/", api.NewHandler(storage, node))
	mux.Handle("/repo.git/", http.StripPrefix("/repo.git", api.NewGitHandler("repo.git", storage, node, logging.GetLogger("repo.git"))))

	log.Fatalln(http.ListenAndServe(myid.Addr(), LogHandler(mux)))
}

func LogHandler(def http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.Method, r.URL)
		def.ServeHTTP(w, r)
	})
}
