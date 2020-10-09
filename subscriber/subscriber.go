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
	nodesrv                                 = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local                                   = flag.String("local", "", "Local Synerex Server")
	subscribeClient *sxutil.SXServiceClient = nil
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
	subscribeClient = sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, "{Client:simpleR}")
}

func getSimpleData() {
	log.Print("I get simpleData!")
}

func supplyStorageCB(clt *sxutil.SXServiceClient, sp *api.Supply) {
	if sp.SupplyName == "Record" {
		sRecord := &storage.Record{}
		err := proto.Unmarshal(sp.Cdata.Entity, sRecord)
		if err == nil {
			if string(sRecord.Option) == "simpleData" {
				getSimpleData()
			} else {
				log.Print("This is not simpleData")
			}
		}
	}
}

func main() {

	log.Print("Subscribe Storage Supply")
	_, _ = sxutil.SimpleSubscribeSupply(subscribeClient, supplyStorageCB)
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
