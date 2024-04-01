package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"runtime/pprof"
	"sort"
)

type measurement struct {
	name                 string
	min, max, sum, count int64
}

func main() {
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	if len(os.Args) != 2 {
		panic("missing measurements filename")
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data := collectData(file)
	printMeasurements(data)
}

func collectData(file io.Reader) map[uint64]*measurement {
	data := make(map[uint64]*measurement)

	h := fnv.New64a()

	// TODO: see if this can be further optimised, reads don't show up in the trace though
	b := make([]byte, 1024*1024*1024)

	var offset int
	for {
		n, err := file.Read(b[offset:])
		if err != nil {
			if !errors.Is(err, io.EOF) {
				panic(err)
			}
			break
		}

		var ns, ne int
		for i := 0; i < len(b[:offset+n]); i++ {
			switch b[i] {
			case ';':
				ne = i
			case '\n':
				name := b[ns:ne]
				temperature := int64(parseTemperature(b[ne+1 : i]))

				h.Reset()
				h.Write(name)
				id := h.Sum64()

				station, ok := data[id]
				if !ok {
					data[id] = &measurement{string(name), temperature, temperature, temperature, 1}
				} else {
					if temperature < station.min {
						station.min = temperature
					}
					if temperature > station.max {
						station.max = temperature
					}
					station.sum += temperature
					station.count++
				}
				ns = i + 1
			}
		}
		copy(b[0:offset+n-ns], b[ns:offset+n])
		offset = offset + n - ns
	}
	return data
}

func printMeasurements(data map[uint64]*measurement) {
	// Not worth optimizing at this point
	indexes := make(map[string]uint64)
	keys := make([]string, 0, len(data))
	for id, m := range data {
		keys = append(keys, m.name)
		indexes[m.name] = id
	}
	sort.Strings(keys)

	print("{")
	for _, k := range keys {
		v := data[indexes[k]]
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, float64(v.min)/10., math.Round(float64(v.sum)/float64(v.count))/10., float64(v.max)/10.)
	}
	print("}\n")
}

func parseTemperature(temp []byte) int64 {
	var n int64
	n += int64(temp[len(temp)-1] - '0')
	// -2 is the .
	n += int64(temp[len(temp)-3]-'0') * 10
	if len(temp) > 3 {
		if temp[len(temp)-4] == '-' {
			return -n
		}
		n += int64(temp[len(temp)-4]-'0') * 100
	}
	if len(temp) > 4 {
		return -n
	}
	return n
}
