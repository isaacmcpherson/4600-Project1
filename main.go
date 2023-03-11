package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	//SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	//SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	//RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		burstTime int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) 
{
	var (
		serviceTime     int64
		waitingTime       float64
		turnAroundTime float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes 
	{
		if processes[i].ArrivalTime > 0 
		{
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		waitingTime += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].burstTime + waitingTime
		turnAroundTime += float64(turnaround)

		completion := processes[i].burstTime + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string
		{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].burstTime),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].burstTime

		gantt = append(gantt, TimeSlice
		{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := waitingTime / count
	aveTurnaround := turnAroundTime / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//func SJFPrioritySchedule(w io.Writer, title string, processes []Process)
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) 
{
	var (
		waitingTime       float64
		turnAroundTime float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	temp := make([]Process, len(processes))
	copy(temp, processes)

	tProcess := make([]process, len(temp))
	for i := range tProcess 
	{
		tProcess[i] = process{waitingTime: 0, TAround: 0, exit: 0}
	}

	var time, start int64 = 0, 0
	inc := 0                 

	for !processDone(tProcess)
	{
		swapped := false
		for index, proc := range tProcess 
		{
			if temp[index].ArrivalTime < time 
			{ 
				if index != inc && proc.exit == 0 
				{ 
					tProcess[index].waitingTime += 1 
				} else if index == inc 
				{ 
					temp[index].burstTime--
					if temp[index].burstTime == 0 
					{
						swapped = true
						tProcess[index].exit = time
					}
				}
			}
		}
		new := 0
		for index, proc := range processes
		{
			if tProcess[index].exit == 0 && proc.ArrivalTime <= time 
			{
				if 	temp[index].burstTime < temp[inc].burstTime || 
					temp[inc].burstTime < 1  || 
					(temp[index].burstTime == temp[inc].burstTime && temp[index].Priority > temp[inc].Priority)
				{ 
					new = index
					swapped = true
				}
			}
		}
		if swapped 
		{
			gantt = append(gantt, TimeSlice
			{
				PID:   int64(inc + 1),
				Start: start,
				Stop:  time,
			})
			inc = new 
			start = time  
		}

		time++
	}

	for i, proc := range tProcess 
	{
		schedule[i] = []string
		{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].burstTime),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(proc.waitingTime),
			fmt.Sprint(proc.waitingTime + processes[i].burstTime),
			fmt.Sprint(proc.exit),
		}

		turnAroundTime += float64(proc.waitingTime) + float64(processes[i].burstTime) 
		waitingTime += float64(proc.waitingTime)
	}
	count := float64(len(processes))
	aveWait := waitingTime / count
	aveTurnaround := turnAroundTime / count
	aveThroughput := count / float64(time-1) 

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//func SJFSchedule(w io.Writer, title string, processes []Process)
func SJFSchedule(w io.Writer, title string, processes []Process) 
{
	var (
		waitingTime       float64
		turnAroundTime float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	temp := make([]Process, len(processes)) 
	copy(temp, processes)

	pd := make([]ProcessData, len(temp)) 
	for i := range pd 
	{
		pd[i] = ProcessData{waitingTime: 0, TAround: 0, exit: 0}
	}

	var time, start int64 = 0, 0 
	current := 0                

	for !processDone(pd) 
	{
		swapped := false
		for index, proc := range pd 
		{
			if temp[index].ArrivalTime < time 
			{
				if index != current && proc.exit == 0 
				{
					pd[index].waitingTime += 1
				} else if index == current 
				{
					temp[index].burstTime--
					if temp[index].burstTime == 0 
					{
						swapped = true
						pd[index].exit = time
					}
				}
			}
		}
		new := 0
		for index, proc := range processes 
		{
			if pd[index].exit == 0 && proc.ArrivalTime <= time 
			{
				if temp[index].burstTime < temp[current].burstTime || temp[current].burstTime < 1 
				{ 
					new = index
					swapped = true
				}
			}
		}
		if swapped {
			gantt = append(gantt, TimeSlice
			{
				PID:   int64(current + 1),
				Start: start,
				Stop:  time,
			})
			current = new 
			start = time  
		}

		time++
	}
	for i, proc := range pd 
	{
		schedule[i] = []string
		{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].burstTime),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(proc.waitingTime),
			fmt.Sprint(proc.waitingTime + processes[i].burstTime),
			fmt.Sprint(proc.exit),
		}

		turnAroundTime += float64(proc.waitingTime) + float64(processes[i].burstTime)
		waitingTime += float64(proc.waitingTime)
	}
	count := float64(len(processes))
	aveWait := waitingTime / count
	aveTurnaround := turnAroundTime / count
	aveThroughput := count / float64(time-1) 

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

//func RRSchedule(w io.Writer, title string, processes []Process)
func RRSchedule(w io.Writer, title string, processes []Process) 
{
	var (
		n = len(processes)
		timeLeft = make([]int64, n)
		ArrivalTime = make([]int64, n)
		waitingTime = make([]int64, n)
		turnAroundTIme = make([]int64, n)
		schedule = make([][]string, len(processes))
		currentTime int64 = 0
		turnAroundTime int64 = 0
		waitingTimeing int64 = 0
		val int64 = 2
	)

	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})
	for i := 0; i < n; i++ {
		timeLeft[i] = processes[i].burstTime
		ArrivalTime[i] = processes[i].ArrivalTime
	}

	for 
	{
		Done := true

		for i := 0; i < n; i++ 
		{
			if timeLeft[i] > 0 
			{
				Done = false
				if timeLeft[i] > val
				{

					currentTime += val
					timeLeft[i] -= val
				} 
				else 
				{
					currentTime += timeLeft[i]
					turnAroundTIme[i] = currentTime - ArrivalTime[i]
					timeLeft[i] = 0
				}
			}
		}
		if Done 
		{
			break
		}
	}

	for i := 0; i < n; i++ 
	{
		waitingTime[i] = turnAroundTIme[i] - processes[i].burstTime
		turnAroundTime += turnAroundTIme[i]
		waitingTimeing += waitingTime[i]

		schedule[i] = []string
		{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].burstTime),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime[i]),
			fmt.Sprint(turnAroundTIme[i]),
		}
	}

	avgTurnaround := float64(turnAroundTime) / float64(n)
	avgWaiting := float64(waitingTimeing) / float64(n)
	throughput := float64(n) / float64(currentTime)

	outputTitle(w, title)
	outputSchedule(w, schedule, avgWaiting, avgTurnaround, throughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].burstTime = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
