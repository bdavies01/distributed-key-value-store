package main

import (
	"strings"
  	"fmt"
  	"strconv"
  	"bytes"
	"hash/fnv"
)

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func combineViewSlice(s []string) string {
	str := "["
	for idx, v := range s {
		if idx != len(s)-1 {
			str += "\"" + v + "\"" + ", "
		} else {
			str += "\"" + v + "\"" + "]"
		}
	}
	return str
}

func deleteElement(s []string, str string) []string {
	j := 0
	for _, v := range s {
		if v != str {
			s[j] = v
			j++
		}
	}
	s = s[:j]
	return s
}

func buildStore(body string) {
  if body == "" {
    return
  }
  pairs := strings.Split(body, ",")
	for _, pair := range pairs {
		key := strings.Split(pair, ":")[0]
		value := strings.Split(pair, ":")[1]
    store[key] = value
	}
}

func sumStore(body string) int {
  if body == "" {
    return 0
  }
  pairs := strings.Split(body, ",")
	return len(pairs)
}

func buildClock(body string) {
  if body == "" {
    return
  }
  pairs := strings.Split(body, ",")
	for _, pair := range pairs {
		addr := strings.Split(pair, "|")[0]
		vc := strings.Split(pair, "|")[1]
    vector_clock[addr], _ = strconv.Atoi(vc)
	}
}

func mapToString(m map[string]int) string {
    b := new(bytes.Buffer)
    for key, value := range m {
        fmt.Fprintf(b, "%s=%d,", key, value)
    }
    return b.String()
}

func stringToMap(s string) map[string]int {
  ss := strings.Split(s, ",")
  m := make(map[string]int)
  for _, pair := range ss {
    z := strings.Split(pair, "=")
    m[z[0]], _ = strconv.Atoi(z[1])
  }
  return m
}

func checkVCDeliverable(sender map[string]int, receiver map[string]int, key string, shard_members[] string) bool {
  if len(sender) != len(receiver) {
    return false
  }
  if sender[key] != receiver[key] {
    return false
  }
  for k, v := range sender {
    if k != key && receiver[k] < v && contains(shard_members, k) {
      return false
    }
  }
  return true
}

func checkReplicaVCDeliverable(sender map[string]int, receiver map[string]int, key string, shard_members[] string) bool {
  if len(sender) != len(receiver) {
    return false
  }
  if sender[key] != receiver[key] + 1{
    return false
  }
  for k, v := range sender {
    if k != key && receiver[k] < v && contains(shard_members, k) {
      return false
    }
  }
  return true
}

func mergeVC(sender map[string]int, receiver map[string]int) map[string]int {
  if len(sender) != len(receiver) {
  }
  ret := make(map[string]int)
  for k, v := range receiver {
    if v >= sender[k] {
      ret[k] = v
    } else {
      ret[k] = sender[k]
    }
  }
  return ret
}

func hash(s string) int {
  h := fnv.New32a()
  h.Write([]byte(s))
  return int(h.Sum32())
}