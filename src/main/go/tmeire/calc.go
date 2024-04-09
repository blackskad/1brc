package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
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

type measurements map[uint64]*measurement

func (ms measurements) Merge(res measurements) {
	for id, m := range res {
		station, ok := ms[id]
		if !ok {
			ms[id] = m
		} else {
			station.Merge(m)
		}
	}
}

func main() {
	if os.Getenv("ENABLE_PROFILING") != "" {
		f, err := os.Create("cpu_profile.prof")
		if err != nil {
			panic(err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()

		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	if len(os.Args) != 2 {
		panic("missing measurements filename")
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data := collectData(file, blockSize, runtime.NumCPU()-1)
	printMeasurements(data)
}

// TODO: see if this can be further optimised, reads don't show up in the trace though
const blockSize = 1024 * 1024 * 1024

func collectData(file io.Reader, blockSize int, parallellism int) map[uint64]*measurement {
	var wg sync.WaitGroup
	results := make(chan map[uint64]*measurement)

	// Spin up a limited number of goroutines to limit scheduling issues between them
	inputs := make(chan []byte)
	for i := 0; i < parallellism; i++ {
		wg.Add(1)
		go func() {
			for input := range inputs {
				results <- process(input)
			}
			wg.Done()
		}()
	}

	// One goroutine to collect all the result sets into one
	done := make(chan struct{})
	data := measurements(make(map[uint64]*measurement))
	go func() {
		for res := range results {
			data.Merge(res)
		}
		close(done)
	}()

	var offset int
	var b1 = make([]byte, blockSize)
	var b2 []byte
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
		inputs <- b1[:ns+1]

		// Create a new block for the next goroutine
		b2, b1 = b1, make([]byte, blockSize)
		copy(b1[0:(offset+n)-(ns+1)], b2[ns+1:offset+n])
		offset = (offset + n) - (ns + 1)
	}
	close(inputs)

	// Wait until all processing goroutines have finished, then close the results channel to make sure the collection goroutine can quit as well
	wg.Wait()
	close(results)

	<-done
	return data
}

func process(b []byte) map[uint64]*measurement {
	data := measurements(make(map[uint64]*measurement))

	var h = fnv.New64a()

	if len(b) > 0 && b[0] == '\n' {
		b = b[1:]
	}

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
