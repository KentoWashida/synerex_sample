package main

import (
	"flag"

	"github.com/golang/protobuf/proto"
	storage "github.com/synerex/proto_storage"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"log"
	"sync"
)

var (
	nodesrv                              = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local                                = flag.String("local", "", "Local Synerex Server")
	flagData                             = flag.String("d", "simpleData", "simpleData or another")
	supplyClient *sxutil.SXServiceClient = nil
	ssMu         *sync.Mutex
	ssLoop       *bool
)

func init() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("simpleReceiver(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	channelTypes := []uint32{pbase.STORAGE_SERVICE}
	sxServerAddress, rerr := sxutil.RegisterNode(*nodesrv, "simpleR", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)
	client := sxutil.GrpcConnectServer(sxServerAddress)
	if client == nil {
		log.Fatal("Can't connect Synerex Server")
		return
	}
	supplyClient = sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, "{Client:simpleR}")
}

func main() {

	// Make sendData
	sendRecord := storage.Record{
		Record: []byte("write record"),
		Option: []byte(*flagData),
	}

	out, err := proto.Marshal(&sendRecord)
	if err == nil {
		cont := &api.Content{Entity: out}
		spo := sxutil.SupplyOpts{
			Name:  "Record",
			Cdata: cont,
		}
		supplyId, serr := supplyClient.NotifySupply(&spo)
		log.Printf("Supply simpleData %d", supplyId)
		if serr != nil {
			log.Printf("send Error %s", serr)
		}
	}

}
