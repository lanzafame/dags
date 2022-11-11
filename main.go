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
			// fmt.Printf("cid set added to mfs set: %+v\n", ss)
			mfsCids = append(mfsCids, ss)
			ss = SizedSlice{}
			continue
		}
	}

	if len(ss.CIDs) > 0 {
		mfsCids = append(mfsCids, ss)
		// fmt.Printf("cid set added to mfs set: %+v\n", ss)
	}

	fmt.Println(len(mfsCids))
	for i, b := range mfsCids {
		dirname := fmt.Sprintf("/tw-%d", i)
		fmt.Printf("gathering %s\n", dirname)
		err := gatherCIDs(b, dirname)
		if err != nil {
			fmt.Printf("failed to gatherCIDs: %v\n", err)
			return
		}
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
		fmt.Printf("strconv: size: %v\n", err)
		return -3, err
	}
	return size, nil
}

func ipfsMkdir(name string) error {
	cmd := exec.Command("ipfs", "files", "mkdir", name)
	err := cmd.Start()
	if err != nil {
		fmt.Printf("mkdir: %v\n", err)
		return err
	}
	err = cmd.Wait()
	if err != nil {
		fmt.Printf("mkdir: %v\n", err)
		return err
	}
	return nil
}

func ipfsCopy(src, dest string) error {
	cmd := exec.Command("ipfs", "files", "cp", "--flush=false", src, dest)
	err := cmd.Start()
	if err != nil {
		fmt.Printf("copy: %v\n", err)
		return err
	}
	err = cmd.Wait()
	if err != nil {
		fmt.Printf("copy: %v\n", err)
		return err
	}
	return nil
}

func ipfsFlush(dir string) error {
	cmd := exec.Command("ipfs", "files", "flush", dir)
	err := cmd.Start()
	if err != nil {
		fmt.Printf("flush: %v\n", err)
		return err
	}
	err = cmd.Wait()
	if err != nil {
		fmt.Printf("flush: %v\n", err)
		return err
	}
	return nil
}

func gatherCIDs(cids SizedSlice, dirname string) error {
	// mkdir
	err := ipfsMkdir(dirname)
	if err != nil {
		fmt.Printf("mkdir: %v\n", err)
		return err
	}

	// copy cids to dir
	for _, cid := range cids.CIDs {
		err = ipfsCopy(fmt.Sprintf("/ipfs/%s", cid), fmt.Sprintf("%s/%s", dirname, cid))
		if err != nil {
			fmt.Printf("copy: cid: %s\terr:%v\n", cid, err)
			// return err
		}
	}

	// flush dir
	err = ipfsFlush(dirname)
	if err != nil {
		fmt.Printf("flush: %v\n", err)
		return err
	}
	return nil
}

// function internalloop
// 	set m $argv[1]
// 	for i in (seq 1 3907)
// 	    set pins (tail -n +(math (echo $i)"*256") ~/upload.pins.(echo $m)m | head -n 256)

// 	    set dir /(echo $m)-(echo $i)-256
// 	    ipfs files mkdir $dir

// 	    echo $dir

// 	    echo "copying pins to mfs dir: $dir"
// 	    set start (date +%s)
// 	    for p in $pins
// 		 ipfs files cp --flush=false /ipfs/$p $dir/$p
// 	    end
// 	    set finish (date +%s)
// 	    set cmd_dur_secs (math $finish - $start)
// 	    echo "ipfs mfs copy op took $cmd_dur_secs"

// 	    echo "flushing $dir"
// 	    time ipfs files flush $dir
// 	end
// end

// function gather-cids-into-mfs
//     argparse h/help -- $argv
//     or return

//     if set -q _flag_help
//         echo "gather-cids-into-mfs [-h|--help] <start> <finish> [ARGUMENT ...]"
// 	echo "start -- start of range to process (millions)"
// 	echo "finish -- finish of range to process (millions)"
// 	echo
// 	echo "gather-cids-into-mfs 1 4"
// 	echo
// 	echo "the above command will process from the first million to the 4th million"
//         return 0
//     end

//     for m in (seq $argv[1] $argv[2])
// 	set exportdir (echo $m)m
// 	mkdir -p $exportdir

// 	set start (date +%s)
// 	internalloop $m
// 	set finish (date +%s)
// 	set cmd_dur_secs (math $finish - $start)
// 	set cmd_dur_mins (math $cmd_dur_secs / 60)
// 	set cmd_dur_hrs (math $cmd_dur_mins / 60)
// 	echo "entire mfs operation for $m took $cmd_dur_secs seconds/$cmd_dur_mins minutes/$cmd_dur_hrs hours"
//     end
// end

// gather-cids-into-mfs $argv
