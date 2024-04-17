package main

import (
	"errors"
	"fmt"
	"hash"
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
	id uint64

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

func (ms measurements) Merge(res *Map) {
	for _, m := range res.data {
		if m.id == 0 {
			continue
		}
		station, ok := ms[m.id]
		if !ok {
			ms[m.id] = &m
			continue
		}
		station.Merge(&m)
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
	results := make(chan *Map)

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

func process(b []byte) *Map {
	data := New()

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

			data.Add(name, temperature)
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

type Map struct {
	data []measurement
	h    hash.Hash64
	keys int64

	asc, desc bool
	prev      uint64
}

func New() *Map {
	return &Map{
		data: make([]measurement, 1_000_000),
		h:    fnv.New64a(),
		asc:  true,
		desc: true,
	}
}

func (m *Map) Add(name []byte, temperature int64) {
	m.h.Reset()
	m.h.Write(name)
	id := m.h.Sum64()

	if m.keys == 0 {
		//
	}

	var idx, last int
	switch {
	case m.keys == 0:
		idx = len(m.data) / 2.
	case m.asc && m.prev < id:
		idx = (len(m.data) / 2.) + int(m.keys) + 1
	case m.desc && m.prev > id:
		idx = (len(m.data) / 2.) - int(m.keys) - 1
	default:
		idx = len(m.data) / 2.
		last = len(m.data) - 1

		m.asc, m.desc = false, false
	}

	for idx != last {
		switch {
		case m.data[idx].id == id:
			if temperature < m.data[idx].min {
				m.data[idx].min = temperature
			}
			if temperature > m.data[idx].max {
				m.data[idx].max = temperature
			}
			m.data[idx].sum += temperature
			m.data[idx].count++
			return
		case m.data[idx].id == 0:
			m.data[idx].id = id
			m.data[idx].name = string(name)
			m.data[idx].min = temperature
			m.data[idx].max = temperature
			m.data[idx].sum += temperature
			m.data[idx].count++
			m.keys++
			m.prev = id
			return
		case m.data[idx].id < id:
			last, idx = idx, idx/2.
		case m.data[idx].id > id:
			last, idx = idx, (last+idx)/2.
		}
	}
	// TODO: we would have to resize the slice and reorganize all the data.
	// Let's see if we can trade this memory over a rebalancing algorithm.
	panic(fmt.Sprintf("slice not big enough! (keys: %d, size: %d)", m.keys, len(m.data)))
}
