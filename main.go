package main

import (
	"encoding/json"
	"fmt"
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

type NodeShards struct {
	Shard int      `json:"shard_id"`
	Node  []string `json:"nodes"`
}

type ShardsDisplay struct {
	NodesList []NodeShards `json:"view"`
}

type Shards struct {
	Shard int      `json:"num_shards"`
	Nodes []string `json:"nodes"`
}

type Views struct {
	View []string `json:"view"`
}

type KVS struct {
	Key     string        `json:"key"`
	Value   string        `json:"val"`
	Version vclock.VClock `json:"causal-metadata"`
	Time    time.Time     `json:"time"`
}

var keys []KVS
var display ShardsDisplay
var current Shards
var numShards int
var selfID int

// check if a version vclock is greater/equal to request
func check_version(r vclock.VClock) bool {
	//for all the keys, check if a local key is equal or a descendant of the request key
	//idk if this works
	for _, k := range keys {
		//if so return true
		if k.Version.Compare(r, vclock.Descendant|vclock.Equal) {
			return true
		}
	}
	//else false
	return false
}

// gets the kvs
func get_kvs(w http.ResponseWriter, r *http.Request) {
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

	//if there's a vclock in find if the KVS has updated to it so far
	//this ideally should have a 500 error if 20 seconds have passed
	if key.Version != nil {
		for {
			if check_version(key.Version) {
				break
			}
		}
	}

	//checks if it's in memory
	for _, item := range keys {
		if item.Key == k && item.Value != "" {
			value := item.Value
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Value   string        `json:"val"`
				Version vclock.VClock `json:"causal-metadata"`
			}{value, item.Version})
			return
		}
	}

	//not found
	w.WriteHeader(404)
	json.NewEncoder(w).Encode(struct {
		Version vclock.VClock `json:"causal-metadata"`
	}{key.Version})
}

// deletes the kvs
func delete_kvs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	//gets the JSON body
	vars := mux.Vars(r)
	k := vars["key"]
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	//checks if it's in memory, if so delete it
	for index, item := range keys {
		if item.Key == k && item.Value != "" {
			item.Value = ""
			//increment clock
			key.Version.Tick(os.Getenv("ADDRESS"))
			key.Time = time.Now()
			keys = append(keys[:index], keys[index+1:]...)
			keys = append(keys, item)
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{key.Version})
			return
		}
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

//gonna need this for finding right shard

/*
// passes the request upstream
func pass_kvs(w http.ResponseWriter, r *http.Request) {
	//makes sure it's done in 10 seconds or it will timeout
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	r = r.WithContext(ctx)

	//get url from environment variable FORWARDING_ADDRESS
	//add http:// into it or else it won't work
	url, _ := url.Parse("http://" + os.Getenv("FORWARDING_ADDRESS"))
	proxy := httputil.NewSingleHostReverseProxy(url)

	//error message on timeout
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		json.NewEncoder(w).Encode(map[string]string{"error": "upstream down", "upstream": os.Getenv("FORWARDING_ADDRESS")})
	}

	//send upstream
	proxy.ServeHTTP(w, r)
}
*/

// returns the current view
func get_kvs_view(w http.ResponseWriter, r *http.Request) {
	var getView []NodeShards
	//loop based on number of shards
	for i := 0; i < current.Shard; i++ {
		var singularShard NodeShards
		singularShard.Shard = i
		nodeList := []string{}
		getView = append(getView, (NodeShards{
			Shard: i,
			Node:  nodeList,
		}))
	}
	for j := 0; j < len(current.Nodes); j++ {
		shardID := j % current.Shard
		getView[shardID].Node = append(getView[shardID].Node, current.Nodes[j])
	}

	json.NewEncoder(w).Encode(struct {
		NodesList []NodeShards `json:"view"`
	}{getView})

	w.WriteHeader(200)

}

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

	for i := 0; i < len(current.Nodes); i++ {
		if current.Nodes[i] == os.Getenv("ADDRESS") {
			inView = true
			start_gossip()
		}
	}
	selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
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
	w.WriteHeader(200)
}

// checks if views are the same, else set it
func compare_view(w http.ResponseWriter, r *http.Request) {
	var v Shards
	_ = json.NewDecoder(r.Body).Decode(&v)
	inView = true
	//if the arrays are equal return
	if testEq(v.Nodes, current.Nodes) {
		return
	}

	//set it
	current.Nodes = v.Nodes
	current.Shard = v.Shard
	selfID = indexOf(os.Getenv("ADDRESS"), current.Nodes) % current.Shard
}

// deletes the node view
func delete_kvs_view(w http.ResponseWriter, r *http.Request) {
	inView = false
	keys = nil
	current.Nodes = current.Nodes[:0]
	current.Shard = 0
	ticker.Stop()
	fmt.Println("STOPPED")
	w.WriteHeader(200)
}

// for testing purposes this prints out the KVS
func test(v []KVS) {
	if !inView {
		return
	}
	fmt.Println("CURRENT KVS")
	fmt.Println(v)
	fmt.Println(inView)
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
		url := "http://" + current.Nodes[i] + "/gossip"
		view_marshalled, _ := json.Marshal(keys)
		r, err := http.NewRequest("PUT", url, strings.NewReader(string(view_marshalled)))
		if err != nil {
			continue
		}
		r.Header.Add("Content-Type", "application/json")
		client.Do(r)
	}
}

// creates the kvs
func create_kvs(w http.ResponseWriter, r *http.Request) {
	if !inView {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		json.NewEncoder(w).Encode(map[string]string{"error": "uninitialized"})
	}

	w.Header().Set("Content-Type", "application/json")

	//gets the JSON body
	vars := mux.Vars(r)
	k := vars["key"]
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	if key.Version == nil {
		key.Version = vclock.New()
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
	targetShard := bucketNumber % current.Shard
	//if the selfID (which is the shard this process is in) is not
	//equal to the target shard, we should just return.
	if targetShard != selfID {
		return
	}

	//checks if it's in memory, if so replace it
	for index, item := range keys {
		if item.Key == k && item.Value != "" {
			item.Value = key.Value
			//increment clock
			key.Version.Tick(os.Getenv("ADDRESS"))
			key.Time = time.Now()
			keys = append(keys[:index], keys[index+1:]...)
			keys = append(keys, item)
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{key.Version})
			return
		}
	}

	//if it's a new key
	//make new clock and tick it
	key.Key = k
	key.Version.Tick(os.Getenv("ADDRESS"))
	key.Time = time.Now()
	keys = append(keys, key)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(struct {
		Version vclock.VClock `json:"causal-metadata"`
	}{key.Version})
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
		if p, ok := mergedMap[key.Key]; !ok || (key.Version.Compare(p.Version, vclock.Concurrent) && key.Time.After(p.Time)) || key.Version.Compare(p.Version, vclock.Descendant) {
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
		view_marshalled, _ := json.Marshal(Shards{Shard: current.Shard, Nodes: current.Nodes})
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
		fmt.Println("looping jump hash")
		fmt.Println(tracker2)
	}
	return int(tracker1)
}

// currently doesn't really look at causal consistency yet;
// but it can be used to make sure all the keys are in the right shards!
func get_all_keys(w http.ResponseWriter, r *http.Request) {

	var keyList []string
	count := 0
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	//metaData is the clock of the request
	metaData := key.Version
	if metaData == nil {
		metaData = vclock.New()
	}
	metaData.Tick(os.Getenv("ADDRESS"))
	for _, item := range keys {
		keyList = append(keyList, item.Key)
		count = count + 1
	}

	w.WriteHeader(200)

	json.NewEncoder(w).Encode(struct {
		Shard   int           `json:"shard_id"`
		Count   int           `json:"count"`
		Keys    []string      `json:"keys"`
		Version vclock.VClock `json:"causal-metadata"`
	}{selfID, count, keyList, metaData})

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
			time.Sleep(time.Second * 1)
		}
	}()
}

func main() {
	router := mux.NewRouter()
	inView = false
	router.HandleFunc("/kvs/data", get_all_keys).Methods("GET")
	router.HandleFunc("/gossip/view", compare_view).Methods("PUT")
	router.HandleFunc("/gossip", compare_kvs).Methods("PUT")
	router.HandleFunc("/kvs/admin/view", handle_kvs_view).Methods("GET", "PUT", "DELETE")
	router.HandleFunc("/kvs/data/{key}", handle_kvs).Methods("GET", "PUT", "DELETE")

	http.ListenAndServe(":8080", router)

}
