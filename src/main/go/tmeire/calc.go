package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
)

type measurement struct {
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

	data := make(map[string]*measurement)

	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		name := parts[0]
		tempStr := strings.Trim(parts[1], "\n")

		temperature := int64(parseTemperature(tempStr))

		station, ok := data[name]
		if !ok {
			data[name] = &measurement{temperature, temperature, temperature, 1}
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
	}

	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	print("{")
	for _, k := range keys {
		v := data[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, float64(v.min)/10., math.Round(float64(v.sum)/float64(v.count))/10., float64(v.max)/10.)
	}
	print("}\n")
}

func parseTemperature(temp string) int64 {
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
