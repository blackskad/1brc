package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
)

type measurement struct {
	name                 []byte
	min, max, sum, count int64
}

func (m *measurement) Print() {
	fmt.Printf("%s=%.1f/%.1f/%.1f, ",
		string(m.name),
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

func collectData(file io.Reader, blockSize int, parallellism int) measurements {
	var wg sync.WaitGroup
	results := make(chan measurements)

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
	data := New()
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

func process(b []byte) measurements {
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

func printMeasurements(data measurements) {
	results := data.Flatten()
	slices.SortFunc(results, func(m1 *measurement, m2 *measurement) int {
		if string(m1.name) < string(m2.name) {
			return -1
		}
		return 1
	})

	print("{")
	for _, k := range results {
		k.Print()
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

func hash(name []byte) uint16 {
	l := min(len(name), 8)

	var id uint16
	for idx, b := range name[len(name)-l:] {
		val := uint16(b)
		if idx%2 == 0 {
			val = val << 8
		}
		id = id & val
	}
	return id
}

type measurements []bucket

func New() measurements {
	return make([]bucket, math.MaxUint16)
}

func (mm measurements) Merge(res measurements) {
	for _, vals := range res {
		for _, m := range vals {
			h := hash(m.name)

			mm[h] = mm[h].Add(m)
		}
	}
}

func (m measurements) Flatten() []*measurement {
	var res []*measurement
	for _, b := range m {
		for _, mm := range b {
			res = append(res, mm)
		}
	}
	return res
}

func (m measurements) Add(name []byte, temperature int64) {
	id := hash(name)

	m[id] = m[id].AddNew(name, temperature)
}

type bucket []*measurement

func (b bucket) Add(m *measurement) bucket {
	for _, d := range b {
		if bytes.Equal(d.name, m.name) {
			d.Merge(m)
			return b
		}
	}
	return append(b, m)
}

func (b bucket) AddNew(name []byte, temperature int64) bucket {
	for _, d := range b {
		if bytes.Equal(name, d.name) {
			if temperature < d.min {
				d.min = temperature
			}
			if temperature > d.max {
				d.max = temperature
			}
			d.sum += temperature
			d.count++
			return b
		}
	}

	return append(b, &measurement{
		name:  name,
		min:   temperature,
		max:   temperature,
		sum:   temperature,
		count: 1,
	})
}
