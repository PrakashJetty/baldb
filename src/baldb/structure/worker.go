package structure

import (
	"fmt"
	"sync"
)

type Worker struct {
	Node     *Node
	Npublsih chan EditData
	NRead    chan ReadQuery
}

func (w *Worker) Run(wg *sync.WaitGroup, writeChannel chan float64, wgRead *sync.WaitGroup) {
	w.Npublsih = make(chan EditData)
	w.NRead = make(chan ReadQuery)
	go func() {
		fmt.Println(" start", w.Node.Key)
		for {
			select {
			case editData := <-w.Npublsih:
				if w.Node.IsPartOfHashKey(editData.Hash) {
					fmt.Println("match", w.Node.Key)
					w.Node.Value = editData.Value
					wg.Done()
				} else {
					fmt.Println(" NO match")
					wg.Done()
				}
			case readData := <-w.NRead:
				fmt.Println(" read .. sum")
				if w.Node.IsSomePartOfHashKey(readData.Hash) {
					fmt.Println("Read sum found", w.Node.Value)
					writeChannel <- w.Node.Value
				}
				wgRead.Done()
				fmt.Println(" Done .. sum")

			}
		}
	}()
}
