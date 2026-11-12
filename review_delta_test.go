package whatsmeow

import (
 "context"
 "errors"
 "sync"
 "sync/atomic"
 "testing"
 "time"

 "go.mau.fi/whatsmeow/appstate"
 "go.mau.fi/whatsmeow/appstate/lthash"
 "go.mau.fi/whatsmeow/proto/waE2E"
 "go.mau.fi/whatsmeow/proto/waServerSync"
 "go.mau.fi/whatsmeow/proto/waSyncAction"
 "go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
 "go.mau.fi/whatsmeow/store"
 "go.mau.fi/whatsmeow/types/events"
 "google.golang.org/protobuf/proto"
)

type reviewCappedRestoreStore struct {
 store.AppStateStore
 cancel context.CancelFunc
 resetting bool
 capObserved bool
}
func(s *reviewCappedRestoreStore) GetAllAppStateMutationMACs(ctx context.Context,name string)([]store.AppStateMutationMAC,error){
 return s.AppStateStore.(appStateMutationMACDumper).GetAllAppStateMutationMACs(ctx,name)
}
func(s *reviewCappedRestoreStore) DeleteAppStateVersion(ctx context.Context,name string)error{
 err:=s.AppStateStore.DeleteAppStateVersion(ctx,name)
 if err==nil {s.resetting=true;s.cancel()};return err
}
func(s *reviewCappedRestoreStore) PutAppStateMutationMACs(ctx context.Context,name string,v uint64,macs []store.AppStateMutationMAC)error{
 if s.resetting {
  deadline,ok:=ctx.Deadline();s.capObserved=ok&&time.Until(deadline)<=10*time.Second
  <-ctx.Done() // Slow restore reaches the real ten-second cleanup cap.
 }
 return s.AppStateStore.PutAppStateMutationMACs(ctx,name,v,macs)
}
func TestReviewDeltaRestoreCapDoesNotLeaveVersionWithoutMACs(t *testing.T){
 device:=newSQLiteAppStateDevice(t)
 env:=newAppState409RaceEnv(t,device)
 original:=device.AppState
 ctx,cancel:=context.WithCancel(t.Context());defer cancel()
 wrapped:=&reviewCappedRestoreStore{AppStateStore:original,cancel:cancel}
 device.AppState=wrapped
 started:=time.Now()
 err:=env.cli.FetchAppState(ctx,env.name,true,false)
 v,h,readErr:=original.GetAppStateVersion(t.Context(),string(env.name))
 macs,macErr:=original.(appStateMutationMACDumper).GetAllAppStateMutationMACs(t.Context(),string(env.name))
 consistent:=h==sumStoredMACs(t,original,env.name)
 t.Logf("elapsed=%v cap_observed=%v returned_cancel=%v version=%d mac_count=%d hash_consistent=%v",time.Since(started),wrapped.capObserved,errors.Is(err,context.Canceled),v,len(macs),consistent)
 if readErr!=nil||macErr!=nil||v!=100||!consistent {t.Fatal("cleanup cap left a committed version/hash without its MAC rows")}
}

func reviewLabelClient(t *testing.T)(*Client,*fakeSyncdServer){
 device:=newSQLiteAppStateDevice(t)
 keyID:=[]byte{1,2,3}
 if err:=device.AppStateKeys.PutAppStateSyncKey(t.Context(),keyID,store.AppStateSyncKey{Data:make([]byte,32),Timestamp:1,Fingerprint:[]byte{1}});err!=nil{t.Fatal(err)}
 cli:=NewClient(device,nil);cli.EmitAppStateEventsOnFullSync=true
 seed,err:=cli.appStateProc.EncodePatch(t.Context(),keyID,appstate.HashState{},appstate.BuildLabelEdit("7","initial",1,false));if err!=nil{t.Fatal(err)}
 var parsed waServerSync.SyncdPatch
 if err:=proto.Unmarshal(seed,&parsed);err!=nil{t.Fatal(err)}
 record:=parsed.Mutations[0].Record;blob:=record.Value.Blob
 mac:=store.AppStateMutationMAC{IndexMAC:record.Index.Blob,ValueMAC:blob[len(blob)-32:]}
 state:=appstate.HashState{Version:100}
 lthash.WAPatchIntegrity.SubtractThenAddInPlace(state.Hash[:],nil,[][]byte{mac.ValueMAC})
 if err:=device.AppState.PutAppStateVersion(t.Context(),string(appstate.WAPatchRegular),100,state.Hash);err!=nil{t.Fatal(err)}
 if err:=device.AppState.PutAppStateMutationMACs(t.Context(),string(appstate.WAPatchRegular),100,[]store.AppStateMutationMAC{mac});err!=nil{t.Fatal(err)}
 update,err:=cli.appStateProc.EncodePatch(t.Context(),keyID,state,appstate.BuildLabelEdit("7","old",1,false));if err!=nil{t.Fatal(err)}
 server:=attachFakeServer(t,cli,appstate.WAPatchRegular);server.version=100;server.appendPatch(update)
 return cli,server
}

func reviewAdoptLabelRecovery(t *testing.T,cli *Client){
 t.Helper()
 keyID,err:=cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(t.Context());if err!=nil{t.Fatal(err)}
 mac:=make([]byte,32);mac[0]=42
 var hash [128]byte;lthash.WAPatchIntegrity.SubtractThenAddInPlace(hash[:],nil,[][]byte{mac})
 raw,err:=proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
  CollectionName:proto.String(string(appstate.WAPatchRegular)),CollectionLthash:hash[:],Version:&waSyncdSnapshotRecovery.SyncdVersion{Version:proto.Uint64(110)},
  MutationRecords:[]*waSyncdSnapshotRecovery.SyncdPlainTextRecord{{KeyID:keyID,Mac:mac,Value:&waSyncAction.SyncActionData{
   Index:[]byte(`["label_edit","7"]`),Value:&waSyncAction.SyncActionValue{LabelEditAction:&waSyncAction.LabelEditAction{Name:proto.String("new")}},
  }}},
 });if err!=nil{t.Fatal(err)}
 result:=[]*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{SyncdSnapshotFatalRecoveryResponse:&waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot:raw}}}
 if !cli.handleAppStateRecovery(t.Context(),"synthetic-label-recovery",result){t.Fatal("recovery rejected")}
}

func TestReviewDeltaLabelEventsCannotRegressAfterRecovery(t *testing.T){
 cli,_:=reviewLabelClient(t)
 ctx,cancel:=context.WithTimeout(t.Context(),5*time.Second);defer cancel()
 paused,resume:=make(chan struct{}),make(chan struct{})
 var gated atomic.Bool
 cli.AddEventHandler(func(evt any){
  if label,ok:=evt.(*events.LabelEdit);ok&&label.Action.GetName()=="old"&&!gated.Swap(true){
   close(paused);select{case<-resume:case<-ctx.Done():}
  }
 })
 var mu sync.Mutex
 var projected string
 cli.AddEventHandler(func(evt any){if label,ok:=evt.(*events.LabelEdit);ok{mu.Lock();projected=label.Action.GetName();mu.Unlock()}})
 done:=make(chan error,1)
 go func(){done<-cli.FetchAppState(ctx,appstate.WAPatchRegular,false,false)}()
 select{case<-paused:case<-ctx.Done():t.Fatal("fetch did not reach its label event")}
 reviewAdoptLabelRecovery(t,cli)
 close(resume)
 select{case err:=<-done:if err!=nil{t.Fatal(err)};case<-ctx.Done():t.Fatal("fetch wedged")}
 v,h,err:=cli.Store.AppState.GetAppStateVersion(t.Context(),string(appstate.WAPatchRegular));if err!=nil{t.Fatal(err)}
 consistent:=h==sumStoredMACs(t,cli.Store.AppState,appstate.WAPatchRegular)
 mu.Lock();stale:=projected=="old";mu.Unlock()
 t.Logf("sdk_version=%d hash_consistent=%v stale_label_delivered_last=%v",v,consistent,stale)
 if v!=110||!consistent||stale{t.Fatal("the hash is protected but old fetch events overwrite the newer recovery projection")}
}

type reviewRecoveryBeforeMACStore struct{
 store.AppStateStore
 once atomic.Bool
 before func()
}
func(s *reviewRecoveryBeforeMACStore)GetAppStateMutationMAC(ctx context.Context,name string,index []byte)([]byte,error){
 if !s.once.Swap(true){s.before()}
 return s.AppStateStore.GetAppStateMutationMAC(ctx,name,index)
}
func TestReviewDeltaRecoveryDuringMACReadIsStaleNotBroken(t *testing.T){
 cli,_:=reviewLabelClient(t)
 original:=cli.Store.AppState
 wrapped:=&reviewRecoveryBeforeMACStore{AppStateStore:original}
 wrapped.before=func(){reviewAdoptLabelRecovery(t,cli)}
 cli.Store.AppState=wrapped
 ctx,cancel:=context.WithTimeout(t.Context(),5*time.Second);defer cancel()
 var mismatchEvents atomic.Int32
 cli.AddEventHandler(func(evt any){if e,ok:=evt.(*events.AppStateSyncError);ok&&errors.Is(e.Error,appstate.ErrMismatchingLTHash){mismatchEvents.Add(1)}})
 err:=cli.FetchAppState(ctx,appstate.WAPatchRegular,false,false)
 changed:=errors.Is(err,appstate.ErrStateChanged)
 mismatch:=errors.Is(err,appstate.ErrMismatchingLTHash)
 t.Logf("state_changed=%v hash_mismatch=%v mismatch_events=%d",changed,mismatch,mismatchEvents.Load())
 if !changed||mismatchEvents.Load()!=0{t.Fatal("a superseded fetch is misclassified as a broken collection before reaching the persistence guard")}
}
