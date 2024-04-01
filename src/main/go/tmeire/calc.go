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

func (m *measurement) Print() {
	fmt.Printf("%s=%.1f/%.1f/%.1f, ",
		m.name,
		float64(m.min)/10.,
		math.Round(float64(m.sum)/float64(m.count))/10.,
		float64(m.max)/10.,
	)
}

func (m *measurement) Add(temperature int64) {
	if temperature < m.min {
		m.min = temperature
	}
	if temperature > m.max {
		m.max = temperature
	}
	m.sum += temperature
	m.count++
}

func (m *measurement) Merge(m1 *measurement) {
	if m1.min < m.min {
		m.min = m1.min
	}
	if m1.max > m.max {
		m.max = m1.max
	}
	m.sum += m1.sum
	m.count += m1.count
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

// TODO: see if this can be further optimised, reads don't show up in the trace though
const blockSize = 1024 * 1024 * 1024

func collectData(file io.Reader) map[uint64]*measurement {
	data := make(map[uint64]*measurement)

	var offset int
	var b1 = make([]byte, blockSize)
	var b2 []byte
	blockCount := 0
	for {
		// Read the next block of the file
		n, err := file.Read(b1[offset:])
		if err != nil {
			if !errors.Is(err, io.EOF) {
				panic(err)
			}
			break
		}

		// Find the end of the last full measurement
		ns := offset + n - 1
		for i := ns; i >= 0; i-- {
			if b1[i] == '\n' {
				ns = i
				break
			}
		}

		// Parse the block until the last full measurement & merge it into the main dataset
		res := process(b1[:ns+1])
		for id, m := range res {
			station, ok := data[id]
			if !ok {
				data[id] = m
			} else {
				station.Merge(m)
			}
		}

		// Create a new block for the next goroutine
		b2, b1 = b1, make([]byte, 1024*1024*1024)
		copy(b1[0:(offset+n)-(ns+1)], b2[ns+1:offset+n])
		offset = (offset + n) - (ns + 1)
		blockCount++
	}
	return data
}

func process(b []byte) map[uint64]*measurement {
	data := make(map[uint64]*measurement)
	if len(b) == 0 || (len(b) == 1 && b[0] == '\n') {
		return nil
	}

	// if ok, err := regexp.Match(`([^;]+;[0-9]{1,2}.[0-9]\n)+`, b); err != nil || !ok {
	// 	println(err)
	// 	panic(string(b))
	// }

	var h = fnv.New64a()

	var ns, ne int
	for i := 0; i < len(b); i++ {
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
				station.Add(temperature)
			}
			ns = i + 1
		}
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
		data[indexes[k]].Print()
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
