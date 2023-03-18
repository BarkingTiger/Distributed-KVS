package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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

type Views struct {
	View []string `json:"view"`
}

type KVS struct {
	Key     string        `json:"key"`
	Value   string        `json:"val"`
	Vector  vclock.VClock `json:"causal-metadata"`
	Time    time.Time     `json:"time"`
	Version uint64        `json:"version"`
}

var keys []KVS

var current Views

// checks if the local KVS holds the right key versions that are being requested
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

func get_all_kvs(w http.ResponseWriter, r *http.Request) {
	if !inView {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		json.NewEncoder(w).Encode(map[string]string{"error": "uninitialized"})
		return
	}
	time.Sleep(time.Second)

	w.Header().Set("Content-Type", "application/json")

	//gets the JSON body
	var key KVS
	_ = json.NewDecoder(r.Body).Decode(&key)

	//if there's a vclock in find if the KVS has updated to it so far
	//this ideally should have a 500 error if 20 seconds have passed

	/*
		if key.Vector != nil {
			for {
				if check_version(key.Vector) {
					break
				}
			}
		}
	*/

	count := 0
	list := make([]string, 0)
	//checks if it's in memory
	for _, item := range keys {
		count += 1
		list = append(list, item.Key)
	}

	//not found
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(struct {
		Count int           `json:"count"`
		Keys  []string      `json:"keys"`
		List  vclock.VClock `json:"causal-metadata"`
	}{count, list, key.Vector})
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

	/*
		if key.Vector != nil {
			for {
				if check_version(key.Vector) {
					break
				}
			}
		}
	*/

	//checks if it's in memory
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

	//not found
	w.WriteHeader(404)
	json.NewEncoder(w).Encode(struct {
		Version vclock.VClock `json:"causal-metadata"`
	}{key.Vector})
}

func max(x, y uint64) uint64 {
	if x < y {
		return y + 1
	}
	return x + 1
}

// creates the kvs
func create_kvs(w http.ResponseWriter, r *http.Request) {
	if !inView {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(418)
		json.NewEncoder(w).Encode(map[string]string{"error": "uninitialized"})
		return
	}
	//time.Sleep(time.Second)

	w.Header().Set("Content-Type", "application/json")
	fmt.Println("TEST")

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

	//checks if it's in memory, if so replace it
	for index, item := range keys {
		if item.Key == k && item.Value != "" {
			item.Value = key.Value
			//find higher version
			r_version, _ := key.Vector.FindTicks(item.Key)
			item.Version = max(r_version, item.Version)
			//merge clock in case of extra deps
			item.Vector.Merge(key.Vector)
			//set to higher version
			item.Vector.Set(item.Key, item.Version)
			item.Time = time.Now()
			keys = append(keys[:index], keys[index+1:]...)
			keys = append(keys, item)
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Vector vclock.VClock `json:"causal-metadata"`
			}{item.Vector})
			return
		}
	}

	//if it's a new key
	//make new clock and tick it
	key.Key = k
	key.Vector.Tick(key.Key)
	key.Version = 1
	key.Time = time.Now()
	keys = append(keys, key)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(struct {
		Vector vclock.VClock `json:"causal-metadata"`
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
			item.Value = key.Value
			//find higher version
			r_version, _ := key.Vector.FindTicks(item.Key)
			item.Version = max(r_version, item.Version)
			//merge clock in case of extra deps
			item.Vector.Merge(key.Vector)
			//set to higher version
			item.Vector.Set(item.Key, item.Version)
			item.Time = time.Now()
			keys = append(keys[:index], keys[index+1:]...)
			keys = append(keys, item)
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(struct {
				Version vclock.VClock `json:"causal-metadata"`
			}{key.Vector})
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
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(struct {
		View []string `json:"view"`
	}{current.View})
}

// sets the node view
func create_kvs_view(w http.ResponseWriter, r *http.Request) {
	var viewList Views
	oldList := current.View

	_ = json.NewDecoder(r.Body).Decode(&viewList)
	current.View = viewList.View

	//if the view is the same don't do anything
	if testEq(oldList, current.View) {
		return
	}

	//find if you are in the view
	for i := 0; i < len(current.View); i++ {
		if current.View[i] == os.Getenv("ADDRESS") {
			inView = true
			start_gossip()
		}
	}

	//find nodes that aren't in the view anymore
	var delete []string
	for _, item := range oldList {

		if !slices.Contains(current.View, item) {
			delete = append(delete, item)
		}
	}

	//delete nodes not in view
	for _, item := range delete {
		url := "http://" + item + "/kvs/admin/view"
		r, _ := http.NewRequest("DELETE", url, nil)
		http.DefaultClient.Do(r)
	}

	//pass_view()
}

// deletes the node view
func delete_kvs_view(w http.ResponseWriter, r *http.Request) {
	inView = false
	keys = nil
	current.View = current.View[:0]
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

// gossips about the view to other nodes
func gossip_view(v Views) {
	if !inView {
		return
	}

	client := http.Client{
		Timeout: time.Second * 2,
	}

	//fmt.Println(v)
	//should change this to just target a random node for less traffic
	for i := 0; i < len(current.View); i += 1 {
		if current.View[i] == os.Getenv("ADDRESS") {
			continue
		}
		url := "http://" + current.View[i] + "/kvs/admin/view"
		jsonData, _ := json.Marshal(v)
		r, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
		if err != nil {
			continue
		}
		r.Header.Add("Content-Type", "application/json")
		client.Do(r)
	}
}

// checks if views are the same, else set it
func compare_view(w http.ResponseWriter, r *http.Request) {
	var v []string
	_ = json.NewDecoder(r.Body).Decode(&v)

	fmt.Println(v)
	inView = true
	if len(current.View) == 0 {
		current.View = v
	}

	//if the arrays are equal return
	if testEq(v, current.View) {
		return
	}

	//set it
	current.View = v
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
	for i := 0; i < len(current.View); i++ {
		if current.View[i] == os.Getenv("ADDRESS") {
			continue
		}
		url := "http://" + current.View[i] + "/gossip"
		view_marshalled, _ := json.Marshal(keys)
		r, err := http.NewRequest("PUT", url, strings.NewReader(string(view_marshalled)))
		if err != nil {
			continue
		}
		r.Header.Add("Content-Type", "application/json")
		client.Do(r)
	}
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

	//if key is not in the local kvs || key version == p.version && concurrent && key timestamp is after p timestamp || sent key version is greater than p version
	for _, key := range keys {
		if p, ok := mergedMap[key.Key]; !ok || (p.Vector.Compare(key.Vector, vclock.Equal|vclock.Concurrent) && key.Time.After(p.Time)) || key.Version > p.Version {
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

// starts gossiping
func start_gossip() {
	//do this every second
	interal := 1
	ticker = *time.NewTicker(time.Duration(interal) * time.Second)

	//sends info about view/KVS to the other nodes in the background
	go func() {
		for {
			<-ticker.C
			gossip_view(current)
			gossip_kvs(keys)
		}
	}()
}

func main() {
	router := mux.NewRouter()
	inView = false
	//if the program gets a FORWARDING_ADDRESS then it's a follower, else it's the main
	router.HandleFunc("/kvs/data", get_all_kvs).Methods("GET")
	router.HandleFunc("/gossip", compare_kvs).Methods("PUT")
	router.HandleFunc("/kvs/admin/view", handle_kvs_view).Methods("GET", "PUT", "DELETE")
	router.HandleFunc("/kvs/data/{key}", handle_kvs).Methods("GET", "PUT", "DELETE")

	http.ListenAndServe(":8080", router)

}
