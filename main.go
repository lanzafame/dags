package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type SizedSlice struct {
	CIDs    []string
	CumSize int
}

type SizedCID struct {
	CID  string
	Size int
}

var dagLimit = 1610610000

func main() {
	// read in file of newline-delimited CIDs, store in []SizedCID
	cidFile := os.Args[1]
	data, err := os.ReadFile(cidFile)
	if err != nil {
		log.Fatal(err)
	}
	os.Stdout.Write(data)

	sizedCids := []SizedCID{}

	// iterate through slice calling getCidSize for each CID and
	// storing the size alongside the CID
	for i := 0; i < len(sizedCids); i++ {
		size, err := getCidSize(sizedCids[i].CID)
		if err != nil {
			fmt.Println("cid: ", sizedCids[i].CID, " error: ", err)
		}
		sizedCids[i].Size = size
	}

	// create top level slice of SizedSlices that will be handed to
	// ipfs files commands
	mfsCids := []SizedSlice{}

LOOP:
	cids := []string{}
	ss := SizedSlice{CIDs: cids}

	for _, sc := range sizedCids {
		if ss.CumSize+sc.Size >= dagLimit {
			mfsCids = append(mfsCids, ss)
			goto LOOP
		}
		ss.CIDs = append(ss.CIDs, sc.CID)
		ss.CumSize += sc.Size
	}

	output := []byte{}
	lines := []string{}
	for _, b := range mfsCids {
		line := strings.Join(b.CIDs, ", ")
		lines = append(lines, line)
	}
	output = []byte(strings.Join(lines, "\n"))

	// decide what to do with mfs slice
	// either write it to file in a way that makes calling ipfs files cp
	// or do the ipfs files cp calls here

	err = os.WriteFile("mfs.output", output, 0666)
	if err != nil {
		fmt.Printf("failed to write output: %w", err)
	}
}

func getCidSize(cid string) (size int, err error) {
	cmd := exec.Command("ipfs", "files", "stat", "--size", fmt.Sprintf("/ipfs/%s", cid))
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Start()
	if err != nil {
		return -1, err
	}
	err = cmd.Wait()
	if err != nil {
		return -1, err
	}

	size, err = strconv.Atoi(out.String())
	if err != nil {
		fmt.Errorf("strconv: size: %w", err)
		return -1, err
	}
	return size, nil
}
