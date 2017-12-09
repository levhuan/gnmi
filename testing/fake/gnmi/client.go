/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gnmi

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/kylelemons/godebug/pretty"
	"github.com/openconfig/gnmi/testing/fake/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	swsscommon "github.com/Azure/sonic-swss-common"
	gpb "github.com/openconfig/gnmi/proto/gnmi"
	fpb "github.com/openconfig/gnmi/testing/fake/proto"
)

// Client contains information about a client that has connected to the fake.
type Client struct {
	sendMsg   int64
	recvMsg   int64
	errors    int64
	cTime     time.Time
	cCount    int64
	config    *fpb.Config
	polled    chan struct{}
	mu        sync.RWMutex
	canceled  bool
	q         queue.Queue
	synced    bool
	subscribe *gpb.SubscriptionList
}

// NewClient returns a new initialized client.
func NewClient(config *fpb.Config) *Client {
	return &Client{
		config: config,
		polled: make(chan struct{}, 2),
		synced: false,
	}
}

// String returns the target the client is querying.
func (c *Client) String() string {
	return c.config.Target
}

func getDbpath(c *Client) ([]string, error) {

	var buffer bytes.Buffer
	var dbpath []string

	sublist := c.subscribe
	log.V(6).Infof("SubscribRequest : %#v", sublist)
	// TODO: Prefix parsing parsing
	prefix := sublist.GetPrefix()
	log.V(6).Infof("prefix : %#v", prefix)

	subscriptions := sublist.GetSubscription()
	if subscriptions != nil {
		for _, subscription := range subscriptions {
			path := subscription.GetPath()
			//log.V(2).Infof("path : %#v", path)
			elements := path.GetElement()
			if elements != nil {
				// log.V(2).Infof("path.Element : %#v", elements)
			}

			buffer.Reset()
			elems := path.GetElem()
			if elems != nil {
				//log.V(2).Infof("path.Elem : %#v", elems)
				for i, elem := range elems {
					log.V(6).Infof("index %d elem : %#v %#v", i, elem.GetName(), elem.GetKey())
					if i != 0 {
						buffer.WriteString("|")
					}
					buffer.WriteString(elem.GetName())
				}
				dbpath = append(dbpath, buffer.String())
			}
		}
	}
	log.V(6).Infof("dbpath : %#v", dbpath)
	return dbpath, nil
}

// Support fixed CONFIG_DB for now
func subscribeDb(c *Client, stop chan struct{}) {
	var buffer bytes.Buffer
	var dbn int
	var sstables []*swsscommon.SubscriberStateTable
	// TODO: get db number from prefix or path
	dbn = swsscommon.CONFIG_DB

	dbpath, err := getDbpath(c)
	if err != nil {
		log.V(1).Infof("Failed to get dbpath for %v", c)
	}
	db := swsscommon.NewDBConnector(dbn, swsscommon.DBConnectorDEFAULT_UNIXSOCKET, uint(0))
	defer swsscommon.DeleteDBConnector(db)

	sel := swsscommon.NewSelect()
	defer swsscommon.DeleteSelect(sel)

	for _, table := range dbpath {
		sstable := swsscommon.NewSubscriberStateTable(db, table)
		defer swsscommon.DeleteSubscriberStateTable(sstable)
		sel.AddSelectable(sstable.SwigGetSelectable())
		sstables = append(sstables, &sstable)
	}

	for {
		fd := []int{0}
		var timeout uint = 1000

		c.mu.RLock()
		canceled := c.canceled
		c.mu.RUnlock()
		if canceled {
			log.V(1).Infof("Client %s canceled, exiting subscribeDb routine", c)
			return
		}

		var ret int
		select {
		default:
			ret = sel.Xselect(fd, timeout)
		case <-stop:
			log.V(1).Infof("Stoping subscribeDb routine for Client %s ", c)
			return
		}

		if ret == swsscommon.SelectTIMEOUT {
			log.V(6).Infof("SelectTIMEOUT")
			if c.synced == true {
				continue
			}
			c.mu.RLock()
			q := c.q
			c.mu.RUnlock()
			if q == nil {
				log.V(1).Infof("Client %s has nil client queue nothing to do", c)
				return
			}
			// Inject sync message after first timeout.
			if !c.config.DisableSync {
				q.Add(&fpb.Value{
					Timestamp: &fpb.Timestamp{Timestamp: time.Now().UnixNano()},
					Repeat:    1,
					Value:     &fpb.Value_Sync{uint64(1)},
				})
			}
			log.V(1).Infof("Client %s synced", c)
			c.synced = true
			continue
		}
		if ret != swsscommon.SelectOBJECT {
			log.V(1).Infof("Error: Client %s Expecting : %v", c, swsscommon.SelectOBJECT)
			continue
		}

		for _, sstable := range sstables {
			if sel.IsSelected((*sstable).SwigGetSelectable()) {
				vpsr := swsscommon.NewFieldValuePairs()
				defer swsscommon.DeleteFieldValuePairs(vpsr)

				ko := swsscommon.NewStringPair()
				defer swsscommon.DeleteStringPair(ko)

				(*sstable).Pop(ko, vpsr)
				key := ko.GetFirst()
				strlist := []string{}
				strlist = append(strlist, ko.GetSecond())

				for n := vpsr.Size() - 1; n >= 0; n-- {
					strlist = append(strlist, "|")
					fieldpair := vpsr.Get(int(n))
					strlist = append(strlist, fieldpair.GetFirst(), "=", fieldpair.GetSecond())

				}
				log.V(6).Infof(" key %v, strlist %v", key, strlist)

				c.mu.RLock()
				q := c.q
				c.mu.RUnlock()
				if q == nil {
					log.V(1).Infof("Client %s has nil client queue nothing to do", c)
					return
				}

				path := []string{(*sstable).GetTableName()}
				path = append(path, key)

				buffer.Reset()
				buffer.WriteString(ko.GetSecond())
				for n := vpsr.Size() - 1; n >= 0; n-- {
					fieldpair := vpsr.Get(int(n))
					buffer.WriteString("|")
					buffer.WriteString(fieldpair.GetFirst())
					buffer.WriteString("=")
					buffer.WriteString(fieldpair.GetSecond())
				}

				fpbv := &fpb.Value{
					// Should use path elements in original subscription list
					Path:      path,
					Timestamp: &fpb.Timestamp{Timestamp: time.Now().UnixNano()},
					Repeat:    1,
					Seed:      0,
					Value: &fpb.Value_StringValue{&fpb.StringValue{
						Value: buffer.String(),
						/*
							Distribution: &fpb.StringValue_List{
								List: &fpb.StringList{
									Random:  false,
									Options: strlist}}*/}},
				}
				log.V(6).Infof("configDB path %#v, strlist %#v", path, strlist)
				q.Add(fpbv)
				log.V(5).Infof("Added fpbv #%v", fpbv)
			}
		}
	}
}

// Support fixed COUNTERS_DB for now
func pollDb(c *Client, stop chan struct{}) {
	var buffer bytes.Buffer
	var dbn int
	var dbpath []string

	// TODO: get db number from prefix or path
	dbn = swsscommon.COUNTERS_DB

	dbpath, err := getDbpath(c)
	if err != nil {
		log.V(1).Infof("Failed to get dbpath for %v", c)
	}

	db := swsscommon.NewDBConnector(dbn, swsscommon.DBConnectorDEFAULT_UNIXSOCKET, uint(0))
	defer swsscommon.DeleteDBConnector(db)

	// Use first path, testing only!
	tbl := swsscommon.NewTable(db, dbpath[0])
	defer swsscommon.DeleteTable(tbl)

	// get all keys in DB first
	keys := swsscommon.NewVectorString()
	//defer swsscommon.DeleteVectorString(keys)
	tbl.GetKeys(keys)

	dbkeys := []string{""}
	//var dbkeys []string
	for i := 0; i < int(keys.Size()); i++ {
		dbkeys = append(dbkeys, keys.Get(i))
	}
	swsscommon.DeleteVectorString(keys)

	//TODO: get all keys in DB, then find all matches for each dbpath.
	/*
		// get all keys in DB first
		keys := swsscommon.NewVectorString()
		defer swsscommon.DeleteVectorString(keys)
		tbl.GetKeys(keys)
		for i :=0; i < keys.Size(); i++ {

		}
		for _, table := range dbpath {
			//populate data for each path with the key info
		}
	*/
	q := c.q
	for {
		_, more := <-c.polled
		if !more {
			log.V(1).Infof("%v polled channel closed, exiting pollDb routine", c)
			return
		}
		log.V(2).Infof("dbkeys len: %v", len(dbkeys))
		for idx, dbkey := range dbkeys {
			vpsr := swsscommon.NewFieldValuePairs()
			defer swsscommon.DeleteFieldValuePairs(vpsr)

			ret := tbl.Get(dbkey, vpsr)
			if ret != true {
				// TODO: Key might gets deleted
				log.V(1).Infof("%v table get failed", dbkey)
				continue
			}

			path := []string{tbl.GetTableName()}
			if strings.Compare(dbkey, "") != 0 {
				path = append(path, dbkey)
			}

			buffer.Reset()
			for n := vpsr.Size() - 1; n >= 0; n-- {
				buffer.WriteString("  ")
				fieldpair := vpsr.Get(int(n))
				buffer.WriteString(fieldpair.GetFirst())
				buffer.WriteString("=")
				buffer.WriteString(fieldpair.GetSecond())
			}
			fpbv := &fpb.Value{
				//Path:      dbpath,
				Path:      path,
				Timestamp: &fpb.Timestamp{Timestamp: time.Now().UnixNano()},
				Repeat:    1,
				Seed:      0,
				Value: &fpb.Value_StringValue{&fpb.StringValue{
					Value: buffer.String()}},
			}
			q.Add(fpbv)
			log.V(5).Infof("Added idex %v fpbv #%v ", idx, fpbv)
		}

		if !c.config.DisableSync {
			q.Add(&fpb.Value{
				Timestamp: &fpb.Timestamp{Timestamp: time.Now().UnixNano()},
				Repeat:    1,
				Value:     &fpb.Value_Sync{uint64(1)},
			})
			log.V(2).Infof("Sync done!")
		}

	}
}

// Run starts the client. The first message received must be a
// SubscriptionList. Once the client is started, it will run until the stream
// is closed or the schedule completes. For Poll queries the Run will block
// internally after sync until a Poll request is made to the server. This is
// important as the test may look like a deadlock since it can cause a timeout.
// Also if you Reset the client the change will not take effect until after the
// previous queue has been drained of notifications.
func (c *Client) Run(stream gpb.GNMI_SubscribeServer) (err error) {
	if c.config == nil {
		return grpc.Errorf(codes.FailedPrecondition, "cannot start client: config is nil")
	}
	if stream == nil {
		return grpc.Errorf(codes.FailedPrecondition, "cannot start client: stream is nil")
	}

	defer func() {
		if err != nil {
			c.errors++
		}
	}()

	query, err := stream.Recv()
	c.cTime = time.Now()
	c.cCount++
	c.recvMsg++
	if err != nil {
		if err == io.EOF {
			return grpc.Errorf(codes.Aborted, "stream EOF received before init")
		}
		return grpc.Errorf(grpc.Code(err), "received error from client")
	}

	log.V(1).Infof("Client %s recieved initial query with go struct : %#v %v", c, query, query)

	c.subscribe = query.GetSubscribe()
	if c.subscribe == nil {
		return grpc.Errorf(codes.InvalidArgument, "first message must be SubscriptionList: %q", query)
	}
	// Initialize the queue used between send and recv.
	if err = c.reset(); err != nil {
		return grpc.Errorf(codes.Aborted, "failed to initialize the queue: %v", err)
	}

	stop := make(chan struct{})
	defer close(stop)

	if c.subscribe.GetMode() == gpb.SubscriptionList_STREAM {
		go subscribeDb(c, stop)
	}
	if c.subscribe.GetMode() == gpb.SubscriptionList_POLL {
		c.polled <- struct{}{}
		go pollDb(c, stop)
	}
	/*
		if sublist.Mode == gpb.SubscriptionList_ONCE {
			log.V(1).Infof("gpb.SubscriptionList_ONCE mode not supported : %#v", sublist)
			return
		}
		if sublist.Mode == gpb.SubscriptionList_POLL {
			log.V(1).Infof("gpb.SubscriptionList_POLL mode not supported : %#v", sublist)
			return
		}
	*/
	log.V(1).Infof("Client %s running", c)
	go c.recv(stream)
	c.send(stream)
	log.V(1).Infof("Client %s shutdown", c)

	return nil
}

// Close will cancel the client context and will cause the send and recv goroutines to exit.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.canceled = true
	if c.q != nil {
		// inform queue consumer event happened
		c.q.Signal()
	}
}

// Config returns the current config of the client.
func (c *Client) Config() *fpb.Config {
	return c.config
}

var syncResp = &gpb.SubscribeResponse{
	Response: &gpb.SubscribeResponse_SyncResponse{
		SyncResponse: true,
	},
}

func (c *Client) recv(stream gpb.GNMI_SubscribeServer) {
	for {
		log.V(5).Infof("Client %s blocking on stream.Recv()", c)
		event, err := stream.Recv()
		c.recvMsg++
		switch err {
		default:
			log.V(1).Infof("Client %s received error: %v", c, err)
			c.Close()
			return
		case io.EOF:
			log.V(1).Infof("Client %s received io.EOF", c)
			return
		case nil:
		}
		if c.subscribe.Mode == gpb.SubscriptionList_POLL {
			log.V(1).Infof("Client %s received Poll event: %v", c, event)
			if _, ok := event.Request.(*gpb.SubscribeRequest_Poll); !ok {
				log.V(1).Infof("Client %s received invalid Poll event: %v", c, event)
				c.Close()
				return
			}
			/*
				if err = c.reset(); err != nil {
					c.Close()
					return
				}
			*/
			c.polled <- struct{}{}
			continue
		}
		log.V(1).Infof("Client %s received invalid event: %s", c, event)
	}
	log.V(1).Infof("Client %s exit from recv()", c)
}

// processQueue makes a copy of q then will process values in the queue until
// the queue is complete or an error.  Each value is converted into a gNMI
// notification and sent on stream.
func (c *Client) processQueue(stream gpb.GNMI_SubscribeServer) error {
	c.mu.RLock()
	q := c.q
	c.mu.RUnlock()
	if q == nil {
		return fmt.Errorf("nil client queue nothing to do")
	}
	for {
		c.mu.RLock()
		canceled := c.canceled
		c.mu.RUnlock()
		if canceled {
			return fmt.Errorf("client canceled")
		}
		event, err := q.Next()
		c.sendMsg++
		if err != nil {
			c.errors++
			return fmt.Errorf("unexpected queue Next(): %v", err)
		}
		if event == nil {
			switch {
			case c.config.DisableEof:
				return fmt.Errorf("send exiting due to disabled EOF")
			}
			log.V(6).Infof("queue exhaused, pending on q.event")
			c.sendMsg--

			more := q.Wait()
			if more {
				log.V(6).Infof("queue event received")
				continue
			} else {
				return fmt.Errorf("Queue closed, end of updates")
			}
			//return fmt.Errorf("end of updates")
		}
		var resp *gpb.SubscribeResponse
		switch v := event.(type) {
		case *fpb.Value:
			if resp, err = valToResp(v); err != nil {
				c.errors++
				return err
			}
		case *gpb.SubscribeResponse:
			resp = v
		}

		err = stream.Send(resp)
		if err != nil {
			log.V(1).Infof("Client %s sending error:%v", c, resp)
			c.errors++
			return err
		}
		log.V(5).Infof("Client %s done sending: %v", c, resp)
		log.V(2).Infof("Client %s done sending, msg count %d", c, c.sendMsg)
	}
}

// send runs until process Queue returns an error. Each loop is meant to allow
// for a reset of the sending queue based on query type.
func (c *Client) send(stream gpb.GNMI_SubscribeServer) {
	for {
		if err := c.processQueue(stream); err != nil {
			log.Errorf("Client %s error: %v", c, err)
			return
		}
	}
}

// SetConfig will replace the current configuration of the Client. If the client
// is running then the change will not take effect until the queue is drained
// of notifications.
func (c *Client) SetConfig(config *fpb.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
}

func (c *Client) reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.V(1).Infof("Client %s using config:\n%s", c, pretty.Sprint(c.config))
	switch {
	default:
		log.V(1).Infof("Client %s --- c.config.Values: \n%s", c, pretty.Sprint(c.config.Values))
		q := queue.New(c.config.GetEnableDelay(), c.config.Seed, c.config.Values)
		/*
			// Inject sync message after latest provided update in the config.
			if !c.config.DisableSync {
				q.Add(&fpb.Value{
					Timestamp: &fpb.Timestamp{Timestamp: q.Latest()},
					Repeat:    1,
					Value:     &fpb.Value_Sync{uint64(1)},
				})
			}
		*/
		c.q = q
	case c.config.GetFixed() != nil:
		log.V(1).Infof("Client %s  queue.NewFixed", c)
		q := queue.NewFixed(c.config.GetFixed().Responses, c.config.EnableDelay)
		// Inject sync message after latest provided update in the config.
		if !c.config.DisableSync {
			q.Add(syncResp)
		}
		c.q = q
	}
	return nil
}

// valToResp converts a fake_proto Value to its corresponding gNMI proto stream
// response type.
// fake_proto sync values are converted to gNMI subscribe responses containing
// SyncResponses.
// All other fake_proto values are assumed to be gNMI subscribe responses
// containing Updates.
func valToResp(val *fpb.Value) (*gpb.SubscribeResponse, error) {
	switch val.GetValue().(type) {
	case *fpb.Value_Delete:
		return &gpb.SubscribeResponse{
			Response: &gpb.SubscribeResponse_Update{
				Update: &gpb.Notification{
					Timestamp: val.Timestamp.Timestamp,
					Delete:    []*gpb.Path{{Element: val.Path}},
				},
			},
		}, nil
	case *fpb.Value_Sync:
		var sync bool
		if queue.ValueOf(val).(uint64) > 0 {
			sync = true
		}
		return &gpb.SubscribeResponse{
			Response: &gpb.SubscribeResponse_SyncResponse{
				SyncResponse: sync,
			},
		}, nil
	default:
		tv := queue.TypedValueOf(val)
		if tv == nil {
			return nil, fmt.Errorf("failed to get TypedValue of %s", val)
		}
		return &gpb.SubscribeResponse{
			Response: &gpb.SubscribeResponse_Update{
				Update: &gpb.Notification{
					Timestamp: val.Timestamp.Timestamp,
					Update: []*gpb.Update{
						{
							Path: &gpb.Path{Element: val.Path},
							Val:  tv,
						},
					},
				},
			},
		}, nil
	}
}
