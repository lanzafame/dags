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

	str := string(data)
	cidsstr := strings.Split(str, "\n")

	sizedCids := []SizedCID{}

	for _, c := range cidsstr {
		sc := SizedCID{CID: c}
		sizedCids = append(sizedCids, sc)
	}

	// iterate through slice calling getCidSize for each CID and
	// storing the size alongside the CID
	for i := 0; i < len(sizedCids)-1; i++ {
		size, err := getCidSize(sizedCids[i].CID)
		if err != nil {
			fmt.Errorf("cid: %s\terror: %s", sizedCids[i].CID, err)
			return
		}
		sizedCids[i].Size = size
		fmt.Printf("%s\t%d\n", sizedCids[i].CID, sizedCids[i].Size)
	}

	// create top level slice of SizedSlices that will be handed to
	// ipfs files commands
	mfsCids := []SizedSlice{}

	ss := SizedSlice{}
	for i := 0; i < len(sizedCids)-1; i++ {
		ss.CIDs = append(ss.CIDs, sizedCids[i].CID)
		ss.CumSize += sizedCids[i].Size
		if ss.CumSize+sizedCids[i].Size >= dagLimit {
			fmt.Printf("cid set added to mfs set: %+v\n", ss)
			mfsCids = append(mfsCids, ss)
			ss = SizedSlice{}
			continue
		}
	}

	if len(ss.CIDs) > 0 {
		mfsCids = append(mfsCids, ss)
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

		return -2, err
	}

	size, err = strconv.Atoi(strings.TrimRight(out.String(), "\n"))
	if err != nil {
		fmt.Errorf("strconv: size: %w", err)
		return -3, err
	}
	return size, nil
}
