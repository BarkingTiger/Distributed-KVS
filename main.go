package main

import (
	"encoding/json"
	"hash/fnv"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"git.tu-berlin.de/mcc-fred/vclock"
	"github.com/gorilla/mux"
	"golang.org/x/exp/slices"
)

var inView bool
var ticker time.Ticker

// This NodeShards shows which nodes are in which INDIVIDUAL shard.
// Mostly used in the getView function
type NodeShards struct {
	Shard int      `json:"shard_id"`
	Node  []string `json:"nodes"`
}

// A slice that contains all of the shards
// and the nodes in each shard.
type ShardsDisplay struct {
	NodesList []NodeShards `json:"view"`
}

// tracks how many shards there are, and the nodes in the view.
// This will basically be our "view" struct from the last assignment
type Shards struct {
	Shard int       `json:"num_shards"`
	Nodes []string  `json:"nodes"`
	Time  time.Time `json:"time"`
}

// probably dont need this anymore
type Views struct {
	View []string `json:"view"`
}

type KVS struct {
	Key     string        `json:"key"`
	Value   string        `json:"val"`
	Vector  vclock.VClock `json:"causal-metadata"`
	Version uint64        `json:"version"`
	Time    time.Time     `json:"time"`
}

var keys []KVS
var display ShardsDisplay
var current Shards
var numShards int
var selfID int
var getView []NodeShards

// check if a version vclock is greater/equal to request
func check_version(r vclock.VClock) bool {
	//for all the keys, check if a local key is equal or a descendant of the request key
	//idk if this works
	for _, k := range keys {
		//look if the key exists
		version, found := r.FindTicks(k.Key)
		if !found {
			continue
		}
		if version < k.Version {
			return false
		}
	}
	//else false
	return true
}

// gets the kvs
func get_kvs(w http.ResponseWriter, r *http.Request) {
	if !inView {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		json.NewEncoder(w).Encode(map[string]string{"error": "uninitialized"})
		return
	}
	time.Sleep(time.Second)

	w.Header().Set("Content-Type", "application/json")

	//gets the JSON body
	vars := mux.Vars(r)
	k := vars["key"]
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	//if there's a vclock in find if the KVS has updated to it so far
	//this ideally should have a 500 error if 20 seconds have passed

	//checks if it's in memory
	//do we have to tick the vector for gets as well?
	for _, item := range keys {
		if item.Key == k && item.Value != "" {
			value := item.Value
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Value   string        `json:"val"`
				Version vclock.VClock `json:"causal-metadata"`
			}{value, item.Vector})
			return
		}
	}
	hashed_key := hash(k) //% uint64(current.Shard)
	hash_key_64 := int64(hashed_key)
	bucketNumber := jump_hash(hash_key_64, len(current.Nodes))
	//the bucket number will choose one of the addresses,
	//and we can get the shard of this address.
	targetShard := bucketNumber % current.Shard
	//fmt.Printf("target shard: %#v\n myShard: %#v\n", targetShard, selfID)
	set_shardView()

	designatedIndex := 0

	for j := 0; j < len(getView); j++ {
		if getView[j].Shard == targetShard {
			designatedIndex = j
		}
	}
	if targetShard != selfID {
		vector := make(chan vclock.VClock)
		value := make(chan string)
		for _, eachAddress := range getView[designatedIndex].Node {
			go func(address string, vector chan vclock.VClock) {
				client := http.Client{
					Timeout: time.Second * 20,
				}
				view_marshalled, _ := json.Marshal(key)
				r, _ := http.NewRequest("GET", "http://"+address+"/kvs/data/"+k, strings.NewReader(string(view_marshalled)))
				r.Header.Add("Content-Type", "application/json")
				response, err := client.Do(r)
				if err != nil {
					return
				}
				res := KVS{}
				json.NewDecoder(response.Body).Decode(&res)
				response.Body.Close()
				vector <- res.Vector
				value <- res.Value

			}(eachAddress, vector)

		}

		select {
		case res := <-vector:
			value := <-value
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Value   string        `json:"val"`
				Version vclock.VClock `json:"causal-metadata"`
			}{value, res})
		case <-time.After(20 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(struct {
				Error    string     `json:"error"`
				Upstream NodeShards `json:"upstream"`
			}{"upstream down", getView[designatedIndex]})
		}
		return
	}

	w.WriteHeader(404)
	json.NewEncoder(w).Encode(struct {
		Version vclock.VClock `json:"causal-metadata"`
	}{key.Vector})
}

// deletes the kvs
func delete_kvs(w http.ResponseWriter, r *http.Request) {
	if !inView {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		json.NewEncoder(w).Encode(map[string]string{"error": "uninitialized"})
		return
	}
	w.Header().Set("Content-Type", "application/json")

	//gets the JSON body
	vars := mux.Vars(r)
	k := vars["key"]
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	//checks if it's in memory, if so delete it
	for index, item := range keys {
		if item.Key == k && item.Value != "" {
			//ticker.Stop()
			item.Value = ""
			item.Version += 1
			//increment clock
			item.Vector.Tick(item.Key)
			key.Time = time.Now()
			keys = append(keys[:index], keys[index+1:]...)
			keys = append(keys, item)
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{item.Vector})
			return
		}
	}

	hashed_key := hash(k) //% uint64(current.Shard)
	hash_key_64 := int64(hashed_key)
	bucketNumber := jump_hash(hash_key_64, len(current.Nodes))
	if current.Shard == 0 {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "divide by 0"})
		return
	}
	targetShard := bucketNumber % current.Shard
	//f("target shard: %#v\n myShard: %#v\n", targetShard, selfID)
	designatedIndex := 0

	for j := 0; j < len(getView); j++ {
		if getView[j].Shard == targetShard {
			designatedIndex = j
		}
	}
	if targetShard != selfID {
		vector := make(chan vclock.VClock)
		for _, eachAddress := range getView[designatedIndex].Node {
			go func(address string, vector chan vclock.VClock) {
				client := http.Client{
					Timeout: time.Second * 20,
				}
				view_marshalled, _ := json.Marshal(key)
				r, _ := http.NewRequest("DELETE", "http://"+address+"/kvs/data/"+k, strings.NewReader(string(view_marshalled)))
				r.Header.Add("Content-Type", "application/json")
				response, err := client.Do(r)
				if err != nil {
					return
				}
				res := KVS{}
				json.NewDecoder(response.Body).Decode(&res)
				response.Body.Close()
				vector <- res.Vector
			}(eachAddress, vector)

		}

		select {
		case res := <-vector:
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{res})
		case <-time.After(20 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(struct {
				Error    string     `json:"error"`
				Upstream NodeShards `json:"upstream"`
			}{"upstream down", getView[designatedIndex]})
		}
		return
	}

	//not found
	w.WriteHeader(404)
	json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
}

// handles the request based on the method
func handle_kvs(w http.ResponseWriter, r *http.Request) {

	switch r.Method {

	//GET request
	case http.MethodGet:
		get_kvs(w, r)

	//PUT request
	case http.MethodPut:
		create_kvs(w, r)

	//DELETE request
	case http.MethodDelete:
		delete_kvs(w, r)

	//catch all for other requests
	default:
		w.WriteHeader(405)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	}
}

// handler for view functions
func handle_kvs_view(w http.ResponseWriter, r *http.Request) {

	switch r.Method {

	//GET request
	case http.MethodGet:
		get_kvs_view(w, r)

	//PUT request
	case http.MethodPut:
		create_kvs_view(w, r)

	//DELETE request
	case http.MethodDelete:
		delete_kvs_view(w, r)

	//catch all for other requests
	default:
		w.WriteHeader(405)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	}
}

func set_shardView() {

	getView = getView[:0]
	//fmt.Printf("AFTER DELETE: %v\n", getView)
	for i := 0; i < current.Shard; i++ {
		var singularShard NodeShards
		singularShard.Shard = i
		nodeList := []string{}
		getView = append(getView, (NodeShards{
			Shard: i,
			Node:  nodeList,
		}))
	}
	//this for loop actually appends onto the [empty node list] that we
	//made in the previous for loop
	for j := 0; j < len(current.Nodes); j++ {
		shardID := j % current.Shard
		getView[shardID].Node = append(getView[shardID].Node, current.Nodes[j])
		//fmt.Printf("VIEW SHARDS: %v\n", getView[shardID].Node)
	}
	//fmt.Printf("POST RESHARD: %v\n", getView)
}

// returns the current view
func get_kvs_view(w http.ResponseWriter, r *http.Request) {
	//loop based on number of shards
	//This whole for loop basically makes a struct of the form:
	//{shard_id: i, nodes = [empty node list]}

	//set_shardView()
	json.NewEncoder(w).Encode(struct {
		NodesList []NodeShards `json:"view"`
	}{getView})

	w.WriteHeader(200)

}

// used later for determining which shard the address is in
func indexOf(e string, list []string) int {
	for i, j := range list {
		if e == j {
			return i
		}
	}
	return -1 //not found.
}

// sets the node view
func create_kvs_view(w http.ResponseWriter, r *http.Request) {
	var shardList Shards
	oldList := current.Nodes
	_ = json.NewDecoder(r.Body).Decode(&shardList)
	current.Nodes = shardList.Nodes
	current.Shard = shardList.Shard
	current.Time = time.Now()
	if shardList.Time.Before(current.Time) && !shardList.Time.IsZero() {
		return
	}
	inView = false

	for i := 0; i < len(current.Nodes); i++ {
		if current.Nodes[i] == os.Getenv("ADDRESS") {
			inView = true
			//start_gossip()
		}
	}
	selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
	set_shardView()
	var delete []string
	for _, item := range oldList {

		if !slices.Contains(current.Nodes, item) {
			delete = append(delete, item)
		}
	}
	for _, item := range delete {
		url := "http://" + item + "/kvs/admin/view"
		r, _ := http.NewRequest("DELETE", url, nil)
		http.DefaultClient.Do(r)
	}
	for index, item := range keys {
		if item.Value != "" {
			hashed_key := hash(item.Key)
			hash_key_64 := int64(hashed_key)
			bucketNumber := jump_hash(hash_key_64, len(current.Nodes))
			targetShard := bucketNumber % current.Shard
			if targetShard != selfID {
				keys = append(keys[:index], keys[index+1:]...)
			}
		}
	}
	w.WriteHeader(200)
}

// checks if views are the same, else set it
func compare_view(w http.ResponseWriter, r *http.Request) {
	var v Shards
	_ = json.NewDecoder(r.Body).Decode(&v)
	inView = true
	//maybe add || number shards == 0
	if len(current.Nodes) == 0 {
		current.Nodes = v.Nodes
		current.Shard = v.Shard
		set_shardView()
		selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
		return
	}
	//if the arrays are equal return
	/*
		if testEq(v.Nodes, current.Nodes) && (v.Shard == current.Shard) {
			//selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
			return
		}
	*/
	if v.Time.After(current.Time) {
		//fmt.Println("ITS TRUE")
		current.Nodes = v.Nodes
		current.Shard = v.Shard
		set_shardView()
		selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
		for index, item := range keys {
			if item.Value != "" {
				hashed_key := hash(item.Key)
				hash_key_64 := int64(hashed_key)
				bucketNumber := jump_hash(hash_key_64, len(current.Nodes))
				targetShard := bucketNumber % current.Shard
				if targetShard != selfID {
					//fmt.Println("GET OUT")
					keys = append(keys[:index], keys[index+1:]...)
				}
			}
		}
		return
	}

	//set it
	/*
		current.Nodes = v.Nodes
		current.Shard = v.Shard
		//determine what shard this address is in
		set_shardView()
		selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
		//delete the key off the shard if it's not there after the REsharding


	*/
}

// deletes the node view
func delete_kvs_view(w http.ResponseWriter, r *http.Request) {
	inView = false
	keys = nil
	current.Nodes = current.Nodes[:0]
	current.Shard = 0
	ticker.Stop()
	//fmt.Println("STOPPED")
	w.WriteHeader(200)
}

// for testing purposes this prints out the KVS
func test(v Shards) {
	if !inView {
		return
	}
	//fmt.Println("CURRENT KVS")
	//fmt.Println(v)
	//fmt.Println(inView)
}

// custom function for checking if arrays are equal
func testEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

// gossips to other nodes about what KVS it has
func gossip_kvs(v []KVS) {
	if !inView {
		return
	}
	client := http.Client{
		Timeout: time.Second * 1,
	}

	//should change this to just target a random node for less traffic

	//gossip the kvs
	for i := 0; i < len(current.Nodes); i++ {
		if current.Nodes[i] == os.Getenv("ADDRESS") {
			continue
		}
		//only gossip to other nodes in the same shard as you
		if i%current.Shard == selfID {
			url := "http://" + current.Nodes[i] + "/gossip"
			view_marshalled, _ := json.Marshal(keys)
			r, err := http.NewRequest("PUT", url, strings.NewReader(string(view_marshalled)))
			if err != nil {
				//fmt.Println("ERROR IN GOSSIP KVS")
				continue
			}
			r.Header.Add("Content-Type", "application/json")
			client.Do(r)
		}
	}
}

// creates the kvs
func create_kvs(w http.ResponseWriter, r *http.Request) {
	if !inView {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		json.NewEncoder(w).Encode(map[string]string{"error": "uninitialized"})
	}
	//fmt.Println("we've entered create_kvs")

	w.Header().Set("Content-Type", "application/json")

	//gets the JSON body
	vars := mux.Vars(r)
	k := vars["key"]
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	if key.Vector == nil {
		key.Vector = vclock.New()
	}

	//checks if JSON is malformed
	if k == "" {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "bad request"})
		return
	}

	//check if key or value is longer than 200 characters
	if len(k) > 2048 || len(key.Value) > 8000000 {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "key/val too large"})
		return
	}

	hashed_key := hash(k) //% uint64(current.Shard)
	hash_key_64 := int64(hashed_key)
	bucketNumber := jump_hash(hash_key_64, len(current.Nodes))
	if current.Shard == 0 {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "divide by 0"})
		return
	}
	targetShard := bucketNumber % current.Shard
	//f("target shard: %#v\n myShard: %#v\n", targetShard, selfID)
	//set_shardView()
	//if this current node is not in the same shard as the designated bucket,
	//then proxy the request to a node that is in the same shard as the bucket.

	//designatedIndex refers to the index within our getView that has the shard.
	designatedIndex := 0

	for j := 0; j < len(getView); j++ {
		if getView[j].Shard == targetShard {
			designatedIndex = j
		}
	}
	//fmt.Printf("GETVIEW: %v\n", (getView))
	//fmt.Printf("INDEX: %v\n", (designatedIndex))
	//fmt.Printf("VIEW: %v\n", getView[designatedIndex].Node)
	if targetShard != selfID {
		vector := make(chan vclock.VClock)
		for _, eachAddress := range getView[designatedIndex].Node {
			go func(address string, vector chan vclock.VClock) {
				client := http.Client{
					Timeout: time.Second * 20,
				}
				view_marshalled, _ := json.Marshal(key)
				r, _ := http.NewRequest("PUT", "http://"+address+"/kvs/data/"+k, strings.NewReader(string(view_marshalled)))
				r.Header.Add("Content-Type", "application/json")
				response, err := client.Do(r)
				if err != nil {
					return
				}
				res := KVS{}
				json.NewDecoder(response.Body).Decode(&res)
				response.Body.Close()
				vector <- res.Vector
			}(eachAddress, vector)

		}

		select {
		case res := <-vector:
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{res})
		case <-time.After(20 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(struct {
				Error    string     `json:"error"`
				Upstream NodeShards `json:"upstream"`
			}{"upstream down", getView[designatedIndex]})
		}
		return
	}

	//right now this is just sending to a specific bucket number (aka index on the address list),
	//but we need to consider the case when this bucket is down.
	//In that case, we should send it to another node that is in the same shard as the bucket!

	//checks if it's in memory, if so replace it
	for index, item := range keys {
		if item.Key == k && item.Value != "" {
			item.Value = key.Value
			item.Vector.Merge(key.Vector)
			item.Vector.Tick(item.Key)
			item.Version, _ = item.Vector.FindTicks(item.Key)
			item.Time = time.Now()
			keys = append(keys[:index], keys[index+1:]...)
			keys = append(keys, item)
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{item.Vector})
			return
		}
	}
	//fmt.Println("we've passed flag2")

	//if it's a new key
	//make new clock and tick it
	key.Key = k
	key.Vector.Tick(key.Key)
	key.Version, _ = key.Vector.FindTicks(key.Key)
	key.Time = time.Now()
	keys = append(keys, key)
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(struct {
		Version vclock.VClock `json:"causal-metadata"`
	}{key.Vector})
}

// compares the KVS and updates it accordingly
func compare_kvs(w http.ResponseWriter, r *http.Request) {
	var k []KVS
	_ = json.NewDecoder(r.Body).Decode(&k)
	//if current node KVS is empty
	if len(keys) == 0 {
		keys = k
		return
	}

	//compare both and update KVS
	mergedMap := make(map[string]KVS)
	mergedKeys := []KVS{}

	//slot in recieved key
	for _, key := range k {
		mergedMap[key.Key] = key
	}

	//if key is not in the local kvs || local key is concurrent to sent key and sent key was written later || sent key is descendent of local key
	for _, key := range keys {
		if p, ok := mergedMap[key.Key]; !ok || (p.Version == key.Version && key.Vector.Compare(p.Vector, vclock.Concurrent) && key.Time.After(p.Time)) || key.Version > p.Version {
			mergedMap[key.Key] = key
		}
	}

	//reform the array
	for _, key := range mergedMap {
		mergedKeys = append(mergedKeys, key)
	}

	//set the array
	keys = mergedKeys
}

// gossips about the view to other nodes
func gossip_view(v Shards) {
	if !inView {
		return
	}

	client := http.Client{
		Timeout: time.Second * 1,
	}

	//should change this to just target a random node for less traffic
	for i := 0; i < len(current.Nodes); i += 1 {
		if current.Nodes[i] == os.Getenv("ADDRESS") {
			continue
		}
		url := "http://" + current.Nodes[i] + "/gossip/view"
		view_marshalled, _ := json.Marshal(Shards{Shard: current.Shard, Nodes: current.Nodes, Time: current.Time})
		//r, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData)) ------------
		r, err := http.NewRequest("PUT", url, strings.NewReader(string(view_marshalled)))
		if err != nil {
			continue
		}
		r.Header.Add("Content-Type", "application/json")
		client.Do(r)
	}
}

func hash(s string) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(s))
	return hasher.Sum64()
}

func jump_hash(key int64, buckets int) int {
	rand.Seed(key)
	var tracker1 float64
	var tracker2 float64
	tracker1 = 1.0
	tracker2 = 0.0
	for tracker2 < float64(buckets) {
		tracker1 = tracker2
		tracker2 = ((tracker1 + 1) / rand.Float64())
	}
	return int(tracker1)
}

// currently doesn't really look at causal consistency yet;
// but it can be used to make sure all the keys are in the right shards!
func get_all_keys(w http.ResponseWriter, r *http.Request) {

	var keyList []string
	count := 0
	vectorCombined := vclock.New()
	for _, item := range keys {
		if item.Value != "" {
			keyList = append(keyList, item.Key)
			count = count + 1
		}
		//are we including the causal metadata of deleted keys?
		vectorCombined.Merge(item.Vector)
		//increment vector here for reads if needed

	}

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(struct {
		Shard   int           `json:"shard_id"`
		Count   int           `json:"count"`
		Keys    []string      `json:"keys"`
		Version vclock.VClock `json:"causal-metadata"`
	}{selfID, count, keyList, vectorCombined})

}

// starts gossiping
func start_gossip() {
	//do this every second
	interal := 1
	ticker = *time.NewTicker(time.Duration(interal) * time.Second)

	//sends info about view/KVS to the other nodes in the background
	go func() {
		for {
			<-ticker.C
			go gossip_view(current)
			go gossip_kvs(keys)
			//go test(current)
		}
	}()
}

func main() {
	router := mux.NewRouter()
	inView = false
	start_gossip()
	router.HandleFunc("/kvs/data", get_all_keys).Methods("GET")
	router.HandleFunc("/gossip/view", compare_view).Methods("PUT")
	router.HandleFunc("/gossip", compare_kvs).Methods("PUT")
	router.HandleFunc("/kvs/admin/view", handle_kvs_view).Methods("GET", "PUT", "DELETE")
	router.HandleFunc("/kvs/data/{key}", handle_kvs).Methods("GET", "PUT", "DELETE")

	http.ListenAndServe(":8080", router)

}
