package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var SOCKET_ADDRESS = ""
var SHARD_COUNT int
var VIEWS []string
var store = make(map[string]interface{})
var vector_clock = make(map[string]int)
var shards = make(map[int][]string)
var shard int

func handleKVS(w http.ResponseWriter, r *http.Request) {
  p := strings.Split(r.URL.Path, "/")
  key := p[2]
  if key == "" {
    w.WriteHeader(http.StatusBadRequest)
    w.Write([]byte(`{"error": "No key in request"}`))
    return
  }
  // Check for downed replicas
  for _, view := range VIEWS {
    if view != SOCKET_ADDRESS {
      req, err := http.NewRequest("GET", "http://"+view+"/store", nil)
      if err == nil {
        client := &http.Client{
          Timeout: 1 * time.Second,
        }
        resp, err := client.Do(req)
        if err == nil {
          defer resp.Body.Close()
        } else {
          broadcastReplicaDelete(view)
        }
      } else {
        return
      }
    }
  }

  key_hash := hash(key)
  key_replica := key_hash % SHARD_COUNT
  switch r.Method {
  case "PUT":
    if len(key) > 50 {
      w.WriteHeader(http.StatusBadRequest)
      w.Write([]byte(`{"error": "Key is too long"}`))
      return
    }
    body, _ := ioutil.ReadAll(r.Body)
    var target interface{}
    json.Unmarshal(body, &target)
    m := target.(map[string]interface{})
    r.Body = ioutil.NopCloser(bytes.NewBuffer(body))

    causal_satisfied := 1

    if _, ok := m["value"]; ok {
      if contains(shards[key_replica], SOCKET_ADDRESS) {
        // LOCAL KEY
        // get and convert metadata to a map[string]int
        metadata, received_metadata := m["causal-metadata"]
        var raw_metadata = map[string]int{}
        if metadata != nil {
          raw_metadata = stringToMap(fmt.Sprintf("%v", metadata))
        } else {
          received_metadata = false
        }
        if _, k := store[key]; k {
          // key in map
          if received_metadata {
            // if we receive metadata, make sure we can deliver
            // only check metadata within local shard
            if !checkVCDeliverable(raw_metadata, vector_clock, SOCKET_ADDRESS, shards[key_replica]) {
              causal_satisfied = 0
              w.WriteHeader(http.StatusServiceUnavailable)
              w.Write([]byte(`{"error": "Causal dependencies not satisfied; try again later"}`))
            } else {
              vector_clock[SOCKET_ADDRESS] += 1
              vector_clock = mergeVC(raw_metadata, vector_clock)
              store[key] = m["value"]
              w.WriteHeader(http.StatusOK)
              w.Write([]byte(`{"result": "updated", "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
            }
          } else {
            vector_clock[SOCKET_ADDRESS] += 1
            store[key] = m["value"]
            w.WriteHeader(http.StatusOK)
            w.Write([]byte(`{"result": "updated", "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
          }
        } else {
          // key not in map
          if received_metadata {
            if !checkVCDeliverable(raw_metadata, vector_clock, SOCKET_ADDRESS, shards[key_replica]) {
              causal_satisfied = 0
              w.WriteHeader(http.StatusServiceUnavailable)
              w.Write([]byte(`{"error": "Causal dependencies not satisfied; try again later"}`))
            } else {
              vector_clock[SOCKET_ADDRESS] += 1
              vector_clock = mergeVC(raw_metadata, vector_clock)
              store[key] = m["value"]
              w.WriteHeader(http.StatusCreated)
              w.Write([]byte(`{"result": "created", "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
            }
          } else {
            vector_clock[SOCKET_ADDRESS] += 1
            store[key] = m["value"]
            w.WriteHeader(http.StatusCreated)
            w.Write([]byte(`{"result": "created", "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
          }
        }

        // if we delivered the PUT request, send it out to the other replicas
        if causal_satisfied == 1 {
          str := fmt.Sprintf("%v", m["value"])
          _, err := strconv.Atoi(str) //check if value is an int or string
          value := ""
          if err == nil {
            value = str
          } else {
            value = strconv.Quote(str)
          }
          var request_string = []byte(`{"value": ` + value + `, "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "sender": "` + SOCKET_ADDRESS + `"}`)
          for _, view := range shards[key_replica] {
            if view != SOCKET_ADDRESS {
              req, err := http.NewRequest("PUT", "http://" + view + "/replica/" + key, bytes.NewBuffer(request_string))
              if err != nil {
                panic(err)
              }
              req.Header.Set("Content-Type", "application/json")

              client := &http.Client{}
              resp, erro := client.Do(req)
              if erro != nil {
                panic(err)
              }

              // if dependencies not met, retry twice more for a total of three tries
              if resp.StatusCode != 200 {
                retryCounter := 2
                for retryCounter != 0 && resp.StatusCode != 200 {
                  reqq, errr := http.NewRequest("PUT", "http://" + view + "/replica/" + key, bytes.NewBuffer(request_string))
                  if errr != nil {
                    panic(err)
                  }
                  reqq.Header.Set("Content-Type", "application/json")
                  resp, erro = client.Do(reqq)
                  if erro != nil {
                    panic(erro)
                  }
                  time.Sleep(1 * time.Second)
                  retryCounter -= 1
                }
              }
              defer resp.Body.Close()
            }
          }
        }
      } else {
        // REMOTE KEY - forwards the same request to a different shard
        body, err := ioutil.ReadAll(r.Body)
        if err != nil {
          panic(err)
        }
        url := fmt.Sprintf("%s://%s%s", "http", shards[key_replica][0], r.RequestURI)
        proxyReq, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
        proxyReq.Header = make(http.Header)
        for h, val := range r.Header {
            proxyReq.Header[h] = val
        }
        client := &http.Client{}
        resp, err := client.Do(proxyReq)
        if err != nil {
            panic(err)
        }
        defer resp.Body.Close()
        w.WriteHeader(resp.StatusCode)
        respBody, err := ioutil.ReadAll(resp.Body)
        if err != nil {
          panic(err)
        }
        w.Write(respBody)
      }
    } else {
      // json doesn't have 'value' key
      w.WriteHeader(http.StatusBadRequest)
      w.Write([]byte(`{"error": "PUT request does not specify a value"}`))
    }

  case "GET":
    body, _ := ioutil.ReadAll(r.Body)
    var target interface{}
    json.Unmarshal(body, &target)
    m := target.(map[string]interface{})
    r.Body = ioutil.NopCloser(bytes.NewBuffer(body))

    if contains(shards[key_replica], SOCKET_ADDRESS) {
      metadata, received_metadata := m["causal-metadata"]
      var raw_metadata = map[string]int{}
      if metadata != nil {
        raw_metadata = stringToMap(fmt.Sprintf("%v", metadata))
      } else {
        received_metadata = false
      }

      if received_metadata {
        if checkVCDeliverable(raw_metadata, vector_clock, SOCKET_ADDRESS, shards[key_replica]) {
          if _, ok := store[key]; ok {
            vector_clock = mergeVC(raw_metadata, vector_clock) //update VC
            str := fmt.Sprintf("%v", store[key])
            _, err := strconv.Atoi(str) //check if value is an int or string
            value := ""
            if err == nil {
              value = str
            } else {
              value = strconv.Quote(str)
            }
            w.WriteHeader(http.StatusOK)
            w.Write([]byte(`{"result": "found", "value": ` + value + `, "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
          } else {
            w.WriteHeader(http.StatusNotFound)
            w.Write([]byte(`{"error": "Key does not exist"}`))
          }
        } else {
          w.WriteHeader(http.StatusServiceUnavailable)
          w.Write([]byte(`{"error": "Causal dependencies not satisfied; try again later"}`))
        }
      } else {
        // if no causal metadata, don't need to update vector clock
        if _, ok := store[key]; ok {
          str := fmt.Sprintf("%v", store[key])
          _, err := strconv.Atoi(str) //check if value is an int or string
          value := ""
          if err == nil {
            value = str
          } else {
            value = strconv.Quote(str)
          }

          w.WriteHeader(http.StatusOK)
          w.Write([]byte(`{"result": "found", "value": ` + value + `, "causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
        } else {
          w.WriteHeader(http.StatusNotFound)
          w.Write([]byte(`{"error": "Key does not exist"}`))
        }
      }
    } else {
      // REMOTE KEY - forwards the same request to a different shard
      body, err := ioutil.ReadAll(r.Body)
      if err != nil {
        panic(err)
      }
      url := fmt.Sprintf("%s://%s%s", "http", shards[key_replica][0], r.RequestURI)
      proxyReq, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
      proxyReq.Header = make(http.Header)
      for h, val := range r.Header {
          proxyReq.Header[h] = val
      }
      client := &http.Client{}
      resp, err := client.Do(proxyReq)
      if err != nil {
          panic(err)
      }
      defer resp.Body.Close()
      w.WriteHeader(resp.StatusCode)
      respBody, err := ioutil.ReadAll(resp.Body)
      if err != nil {
        panic(err)
      }
      w.Write(respBody)
    }

  case "DELETE":
    body, _ := ioutil.ReadAll(r.Body)
    var target interface{}
    json.Unmarshal(body, &target)
    m := target.(map[string]interface{})

    if contains(shards[key_replica], SOCKET_ADDRESS) {
      metadata, received_metadata := m["causal-metadata"]
      causal_satisfied := 1
      var raw_metadata = map[string]int{}
      if received_metadata && fmt.Sprintf("%v", metadata) != "" {
        raw_metadata = stringToMap(fmt.Sprintf("%v", metadata))
      } else {
        received_metadata = false
      }
      if received_metadata {
        if checkVCDeliverable(raw_metadata, vector_clock, SOCKET_ADDRESS, shards[key_replica]) {
          if _, ok := store[key]; ok {
            vector_clock[SOCKET_ADDRESS] += 1
            vector_clock = mergeVC(raw_metadata, vector_clock)
            w.WriteHeader(http.StatusOK)
            delete(store, key)
            w.Write([]byte(`{"result": "deleted, "` + `"causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
          } else {
            w.WriteHeader(http.StatusNotFound)
            w.Write([]byte(`{"error": "Key does not exist"}`))
          }
        } else {
          w.WriteHeader(http.StatusServiceUnavailable)
          causal_satisfied = 0
          w.Write([]byte(`{"error": "Causal dependencies not satisfied; try again later"}`))
        }
      } else {
        // shouldn't happen
        if _, ok := store[key]; ok {
          vector_clock[SOCKET_ADDRESS] += 1
          w.WriteHeader(http.StatusOK)
          delete(store, key)
          w.Write([]byte(`{"result": "deleted, "` + `"causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "shard-id": ` + strconv.Itoa(key_replica) + `}`))
        } else {
          w.WriteHeader(http.StatusNotFound)
          w.Write([]byte(`{"error": "Key does not exist"}`))
        }
      }

      // only broadcast DELETE if we actually delivered the original request
      if causal_satisfied == 1 {
        var request_string = []byte(`{"causal-metadata": "` + strings.TrimSuffix(mapToString(vector_clock), ",") + `", "sender": "` + SOCKET_ADDRESS + `"}`)
        for _, view := range VIEWS {
          if view != SOCKET_ADDRESS {
            req, err := http.NewRequest("DELETE", "http://" + view + "/replica/" + key, bytes.NewBuffer(request_string))
            if err != nil {
              panic(err)
            }
            req.Header.Set("Content-Type", "application/json")

            client := &http.Client{}
            resp, erro := client.Do(req)
            if erro != nil {
              panic(err)
            }
            defer resp.Body.Close()
            if resp.StatusCode != 200 {
              retryCounter := 2
              for retryCounter != 0 && resp.StatusCode != 200 {
                reqq, errr := http.NewRequest("DELETE", "http://" + view + "/replica/" + key, bytes.NewBuffer(request_string))
                if errr != nil {
                  panic(err)
                }
                reqq.Header.Set("Content-Type", "application/json")
                resp, erro = client.Do(reqq)
                if erro != nil {
                  panic(erro)
                }
                time.Sleep(1 * time.Second)
                retryCounter -= 1
              }
            }
          }
        }
      }
    } else {
      // forwarding request
      body, err := ioutil.ReadAll(r.Body)
      if err != nil {
        panic(err)
      }
      url := fmt.Sprintf("%s://%s%s", "http", shards[key_replica][0], r.RequestURI)
      proxyReq, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
      proxyReq.Header = make(http.Header)
      for h, val := range r.Header {
          proxyReq.Header[h] = val
      }
      client := &http.Client{}
      resp, err := client.Do(proxyReq)
      if err != nil {
          panic(err)
      }
      defer resp.Body.Close()
      w.WriteHeader(resp.StatusCode)
      respBody, err := ioutil.ReadAll(resp.Body)
      if err != nil {
        panic(err)
      }
      w.Write(respBody)
    }
  }
}

func handleView(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "PUT":
		body, _ := ioutil.ReadAll(r.Body)
		var target interface{}
		json.Unmarshal(body, &target)
		m := target.(map[string]interface{})
		if _, ok := m["socket-address"]; ok {
			converted, _ := json.Marshal(m["socket-address"])
			p := string(converted)
			address_string := strings.Trim(p, "\"")
			if contains(VIEWS, address_string) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"result": "already present"}`))
			} else {
				VIEWS = append(VIEWS, address_string)
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{"result": "added"}`))
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "Request body has no field socket-address"}`))
		}

	case "GET":
		views_combined := combineViewSlice(VIEWS)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"view": ` + views_combined + `}`))

	case "DELETE":
		body, _ := ioutil.ReadAll(r.Body)
		var target interface{}
		json.Unmarshal(body, &target)
		m := target.(map[string]interface{})
		if _, ok := m["socket-address"]; ok {
			converted, _ := json.Marshal(m["socket-address"])
			p := string(converted)
			address_string := strings.Trim(p, "\"")
			if contains(VIEWS, address_string) {
				VIEWS = deleteElement(VIEWS, address_string)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"result": "deleted"}`))
			} else {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{"error": "View has no such replica"}`))
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": "Request body has no field socket-address"}`))
		}
	}
}

func handleShard(w http.ResponseWriter, r *http.Request) {
  p := strings.Split(r.URL.Path, "/")
  shardOp := p[2]

  switch r.Method {
  case "GET":
    switch shardOp {
    case "ids":
      ids := make([]string, 0, len(shards))
      for i := range shards {
          ids = append(ids, strconv.Itoa(i))
      }
      w.WriteHeader(http.StatusOK)
      w.Write([]byte(`{"shard-ids": [` + strings.Join(ids, ", ") + "]}"))
    case "node-shard-id":
      w.WriteHeader(http.StatusOK)
      w.Write([]byte(`{"node-shard-id": ` + strconv.Itoa(shard) + "}"))
    case "members":
      shardId, err := strconv.Atoi(p[3])
      if err !=nil {
        w.WriteHeader(http.StatusNotFound)
        return
      }
      if _, ok := shards[shardId]; ok {
        members := make([]string, len(shards[shardId]))
        for i := 0; i < len(members); i++ {
          members[i] = `"` + shards[shardId][i] + `"`
        }
        formattedMembers := strings.Join(members, ", ")
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"shard-members": [` + formattedMembers + "]}"))
      } else {
        w.WriteHeader(http.StatusNotFound)
      }
    case "key-count":
      shardId, err := strconv.Atoi(p[3])
      if err !=nil {
        w.WriteHeader(http.StatusNotFound)
        return
      }
      if _, ok := shards[shardId]; ok {
        targetShard := shards[shardId]
        sum := 0
        for _, addr := range targetShard {
          req, err := http.NewRequest("GET", "http://"+addr+"/store", nil)
          if err == nil {
            client := &http.Client{}
            resp, err := client.Do(req)
            if err == nil {
              defer resp.Body.Close()
              body, _ := ioutil.ReadAll(resp.Body)
              sum += sumStore(string(body))
              break
            }
          }
        }
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"shard-key-count": ` + strconv.Itoa(sum) + "}"))
      } else {
        w.WriteHeader(http.StatusNotFound)
      }
    }
  case "PUT":
    switch shardOp {
    case "add-member":
      body, _ := ioutil.ReadAll(r.Body)
      var target interface{}
      json.Unmarshal(body, &target)
      m := target.(map[string]interface{})

      // Error Checking
      if len(p) != 4 {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(`{"error": "Shard ID missing"}`))
        return
      }
      shardId, err := strconv.Atoi(p[3])
      if err !=nil {
        w.WriteHeader(http.StatusNotFound)
        w.Write([]byte(`{"error": "Non-existent shard ID"}`))
        return
      }
      _, okShards := shards[shardId]
      addr, ok := m["socket-address"]
      if !ok {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(`{"error": "Socket address not found in request body"}`))
        return
      }
      okViews := false
      for i := 0; i < len(VIEWS); i++ {
        if VIEWS[i] == addr {
          okViews = true
          break
        }
      }

      // Add node to specified shard, provided both exist
      if okShards && okViews {
        for _, view := range VIEWS {
          request_string := []byte(`{"newMember":"` + addr.(string) + `", "shardId":"` + strconv.Itoa(shardId) + `"}`)
          req, err := http.NewRequest("PUT", "http://" + view + "/member", bytes.NewBuffer(request_string))
          if err != nil {
            panic(err)
          }
          req.Header.Set("Content-Type", "application/json")
          client := &http.Client{}
          resp, err := client.Do(req)
          if err == nil {
            defer resp.Body.Close()
          }
        }
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"result": "node added to shard"}`))
      } else {
        w.WriteHeader(http.StatusNotFound)
        w.Write([]byte(`{"error": "Invalid shard ID or socket address"}`))
        return
      }
    case "reshard":
      body, _ := ioutil.ReadAll(r.Body)
      var target interface{}
      json.Unmarshal(body, &target)
      m := target.(map[string]interface{})

      countObj, ok := m["shard-count"]
      if !ok {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(`{"error": "Shard count not found in request body"}`))
        return
      }

      count := int(countObj.(float64))
      if len(VIEWS) / count < 2 {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(`{"error": "Not enough nodes to provide fault tolerance with requested shard count"}`))
        return
      }
      
      // reshard nodes
      for _, view := range VIEWS {
        request_string := []byte(`{"count":"` + strconv.Itoa(count) + `"}`)
        req, err := http.NewRequest("PUT", "http://" + view + "/rebalance1", bytes.NewBuffer(request_string))
        if err != nil {
          panic(err)
        }
        req.Header.Set("Content-Type", "application/json")
        client := &http.Client{}
        resp, err := client.Do(req)
        if err == nil {
          defer resp.Body.Close()
        }
      }

      // rebalance keys
      for _, view := range VIEWS {
        req, err := http.NewRequest("GET", "http://" + view + "/rebalance2", nil)
        if err != nil {
          panic(err)
        }
        client := &http.Client{}
        resp, err := client.Do(req)
        if err == nil {
          defer resp.Body.Close()
        }
      }

      w.WriteHeader(http.StatusOK)
      w.Write([]byte(`{"result": "resharded"}`))
    }
  }
}

func handleStore(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		allPairs := ""
		for key, value := range store {
			val := fmt.Sprint(value)
      if (strings.Contains(val, "map")) {
        allPairs += key + ":" + val[10:len(val)-1] + ","
      } else {
        allPairs += key + ":" + val + ","
      }
		}
    if len(allPairs) > 0 {
      allPairs = allPairs[0:len(allPairs)-1]
    }
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(allPairs))
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func handleClock(w http.ResponseWriter, r *http.Request) {
  if r.Method == "GET" {
    states := ""
		for key, value := range vector_clock {
			val := fmt.Sprint(value)
      states += key + "|" + val + ","
		}
    if len(states) > 0 {
      states = states[0:len(states)-1]
    }
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(states))
  } else {
    w.WriteHeader(http.StatusNotFound)
  }
}

func handleMember(w http.ResponseWriter, r *http.Request) {
  if r.Method == "PUT" {
    body, _ := ioutil.ReadAll(r.Body)
    var target interface{}
    json.Unmarshal(body, &target)
    m := target.(map[string]interface{})

    shardId, err := strconv.Atoi(m["shardId"].(string))
    if err != nil {
      panic(err)
    }
    newMember := m["newMember"].(string)
    // if updating shard members for already existing nodes
    if newMember != SOCKET_ADDRESS {
      // get list of members in shard that is to be updated
      shardMembers := shards[shardId]
      var newShardMembers [] string
      for _, member := range shardMembers {
        newShardMembers = append(newShardMembers, member)
        if member == newMember {
          w.WriteHeader(http.StatusBadRequest)
          w.Write([]byte(`{"status": "error, member already exists"}`))
          return
        }
      }
      newShardMembers = append(newShardMembers, newMember)
      // add new member into shard members list
      shards[shardId] = newShardMembers
    } else {
      // if shard Id not equal to default value given to node on startup:
      if shard != shardId {
        // get current shard members
        shardMembers := shards[shard]
        var newShardMembers []string
        // place all but current node in list of new members
        for _, member := range shardMembers {
          if member != SOCKET_ADDRESS {
            newShardMembers = append(newShardMembers, member)
          }
        }
        // replace current shard members list with new members list
        shards[shard] = newShardMembers
        // update node's shard
        shard = shardId
      }
      // get list of current shard members, add new member onto shard members list
      toAppend := shards[shard]
      toAppend = append(toAppend, newMember)
      shards[shard] = toAppend
    }
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(`{"status":"updated shard members"}`))
  } else {
    w.WriteHeader(http.StatusNotFound)
  }
}

func handleRebalance1(w http.ResponseWriter, r *http.Request) {
  if r.Method == "PUT" {
    body, _ := ioutil.ReadAll(r.Body)
    var target interface{}
    json.Unmarshal(body, &target)
    m := target.(map[string]interface{})

    count, err := strconv.Atoi(m["count"].(string))
    if err != nil {
      panic(err)
    }

    SHARD_COUNT = count
    shardNodes()

    w.WriteHeader(http.StatusOK)
    w.Write([]byte(`{"status": "Keys rebalanced 1"}`))
  } else {
    w.WriteHeader(http.StatusNotFound)
  }
}

func handleRebalance2(w http.ResponseWriter, r *http.Request) {
  if r.Method == "GET" {
    for k, v := range store {
      val := fmt.Sprintf("%v", v)
      fmt.Print("For key:", k, ", value:", v, ", ")
      if hash(k) % SHARD_COUNT == shard {
        fmt.Println("Key", k, " belongs in current shard", shard)
        // Broadcast kv pair to each node in current shard
        nodes := shards[shard]
        for _, node := range nodes {
          if node != SOCKET_ADDRESS {
            request_string := []byte(`{"value": ` + val + "}")
            req, err := http.NewRequest("PUT", "http://" + node + "/simplekvs/" + k, bytes.NewBuffer(request_string))
            if err != nil {
              panic(err)
            }
            req.Header.Set("Content-Type", "application/json")
            client := &http.Client{}
            resp, err := client.Do(req)
            if err == nil {
              defer resp.Body.Close()
            }
          }
        }
      } else {
        fmt.Println("Key", k, " must be moved to shard", hash(k) % SHARD_COUNT, " from current shard", shard)
        // Get the target shard
        nodes := shards[hash(k) % SHARD_COUNT]
        for _, node := range nodes {
          // For every node in target shard, put kv pair in each node's store
          request_string := []byte(`{"value": ` + val + "}")
          req, err := http.NewRequest("PUT", "http://" + node + "/simplekvs/" + k, bytes.NewBuffer(request_string))
          if err != nil {
            panic(err)
          }
          req.Header.Set("Content-Type", "application/json")
          client := &http.Client{}
          resp, err := client.Do(req)
          if err == nil {
            defer resp.Body.Close()
          }
        }
        // Delete kv pair from current shard
        delete(store, k)
      }
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte(`{"status": "Keys rebalanced 2"}`))
  } else {
    w.WriteHeader(http.StatusNotFound)
  }
}

func handleSimpleKVS(w http.ResponseWriter, r *http.Request) {
  if r.Method == "PUT" {
    p := strings.Split(r.URL.Path, "/")
    key := p[2]
    body, _ := ioutil.ReadAll(r.Body)
    var target interface{}
    json.Unmarshal(body, &target)
    m := target.(map[string]interface{})
    value := m["value"]

    store[key] = value
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(`{"status": "added simply"}`))
  } else {
    w.WriteHeader(http.StatusNotFound)
  }
}

// When a replica is newly inserted into an existing network
func broadcastReplicaPut() {
	pairsRetrieved := false
	for _, view := range VIEWS {
		if view != SOCKET_ADDRESS {
			request_string := []byte(`{"socket-address":"` + SOCKET_ADDRESS + `"}`)
			req, err := http.NewRequest("PUT", "http://"+view+"/view", bytes.NewBuffer(request_string))
			if err != nil {
				panic(err)
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(req)
			// accounting for race condition on initial start up
			if err == nil {
				defer resp.Body.Close()
				// Must get KV pairs from existing replica
				if !pairsRetrieved {
          req, err := http.NewRequest("GET", "http://"+view+"/store", nil)
          if err == nil {
            client := &http.Client{}
            resp, err := client.Do(req)
            if err == nil {
              defer resp.Body.Close()
              body, _ := ioutil.ReadAll(resp.Body)
              buildStore(string(body))

              req, err := http.NewRequest("GET", "http://"+view+"/clock", nil)
              if err == nil {
                client := &http.Client{}
                resp, err := client.Do(req)
                if err == nil {
                  defer resp.Body.Close()
                  body, _ := ioutil.ReadAll(resp.Body)
                  buildClock(string(body))
                  pairsRetrieved = true
                }
              }
            }
          }
				}
			}
		}
	}
}

func broadcastReplicaDelete(toDelete string) {
	for _, view := range VIEWS {
		if view != toDelete {
			request_string := []byte(`{"socket-address":"` + toDelete + `"}`)
			req, err := http.NewRequest("DELETE", "http://"+view+"/view", bytes.NewBuffer(request_string))
			if err != nil {
				panic(err)
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
      time.Sleep(1 * time.Second)
			defer resp.Body.Close()
		}
	}
}

func handleReplica(w http.ResponseWriter, r *http.Request) {
  p := strings.Split(r.URL.Path, "/")
  key := p[2]

  body, _ := ioutil.ReadAll(r.Body)
  var target interface{}
  json.Unmarshal(body, &target)
  m := target.(map[string]interface{})

  metadata, _ := m["causal-metadata"]
  var raw_metadata = stringToMap(fmt.Sprintf("%v", metadata))
  var sender = fmt.Sprintf("%v", m["sender"])

  key_hash := hash(key)
  key_replica := key_hash % SHARD_COUNT

  switch r.Method {
  case "PUT":
    if checkReplicaVCDeliverable(raw_metadata, vector_clock, sender, shards[key_replica]) {
      vector_clock = mergeVC(raw_metadata, vector_clock)
      str := fmt.Sprintf("%v", m["value"])
      store[key] = str
      w.WriteHeader(http.StatusOK)
      w.Write([]byte(`{"result": "confirmed"}`))
    } else {
      w.WriteHeader(http.StatusServiceUnavailable)
      w.Write([]byte(`{"error": "Causal put replica dependencies not satisfied; try again later"}`))
    }

  case "DELETE":
    if checkReplicaVCDeliverable(raw_metadata, vector_clock, sender, shards[key_replica]) {
      vector_clock = mergeVC(raw_metadata, vector_clock)
      delete(store, key)
      w.WriteHeader(http.StatusOK)
      w.Write([]byte(`{"result": "deleted"}`))
    } else {
      w.WriteHeader(http.StatusServiceUnavailable)
      w.Write([]byte(`{"error": "Causal delete replica dependencies not satisfied; try again later"}`))
    }
  }
}

func main() {
	// Check for FORWARDING_ADDRESS environment variable

  // os.Setenv("SOCKET_ADDRESS", "127.0.0.1:8090") // just to test
  // os.Setenv("SHARD_COUNT", "2")
  // os.Setenv("VIEW", "127.0.0.1:8090,127.0.0.1:8070,127.0.0.1:8080") // just to test

	SOCKET_ADDRESS = os.Getenv("SOCKET_ADDRESS")
	VIEWS = strings.Split(os.Getenv("VIEW"), ",")
  SHARD_COUNT, _ = strconv.Atoi(os.Getenv("SHARD_COUNT"))

  for _, v := range VIEWS {
    vector_clock[v] = 0
  }

  shardNodes()

  http.HandleFunc("/view", handleView)
	http.HandleFunc("/kvs/", handleKVS)
  http.HandleFunc("/shard/", handleShard)
	http.HandleFunc("/store", handleStore)
  http.HandleFunc("/clock", handleClock)
  http.HandleFunc("/replica/", handleReplica)
  http.HandleFunc("/member", handleMember)
  http.HandleFunc("/rebalance1", handleRebalance1)
  http.HandleFunc("/rebalance2", handleRebalance2)
  http.HandleFunc("/simplekvs/", handleSimpleKVS)
	// on startup, broadcast PUT to all other replicas
	broadcastReplicaPut()
  http.ListenAndServe(":8090", nil)
}

func shardNodes() {
  for i := 0; i < SHARD_COUNT; i++ {
    leftBound := i * (len(VIEWS) / SHARD_COUNT)
    rightBound := (i + 1) * (len(VIEWS) / SHARD_COUNT)
    if i == SHARD_COUNT - 1 && len(VIEWS) % 2 == 1 {
      rightBound = rightBound + 1
    }
    shards[i] = VIEWS[leftBound : rightBound]
    for j := leftBound; j < rightBound; j++ {
      if SOCKET_ADDRESS == VIEWS[j] {
        shard = i
        break
      }
    }
  }
}
