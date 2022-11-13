package main

import (
	"errors"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/google/uuid"
)

func main() {
	basePath := os.Getenv("BASE_DIR")
	if basePath == "" {
		log.Fatalln("BASE_DIR env variable must be specified")
	}

	if basePath[len(basePath)-1] == '/' {
		basePath = basePath[:len(basePath)-1]
	}

	fIn, err := os.Open(basePath + "/uniqueUsers.csv")
	if err != nil {
		log.Fatalln(err)
	}

	activeUsers, err := ReadActiveUsers(fIn)
	if err != nil {
		log.Fatalln(err)
	}
	func() { _ = fIn.Close() }()

	if len(activeUsers) == 0 {
		log.Println("no uniqueUsers found")
		os.Exit(0)
	}

	fIn, err = os.Open(basePath + "/transactions.csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer func() { _ = fIn.Close() }()

	joinResult, err := ReadNJoinNonBlockedTransactionsWithActiveUsers(fIn, activeUsers)

	joinResult.SortByTransactionAmount(true)

	if err := joinResult.Output(os.Stdout); err != nil {
		log.Fatalln(err)
	}
}

type JoinResult struct {
	CategoryID []uint8
	NumUsers   arrayUInt32
	SumAmount  arrayUInt32
}

// SortByTransactionAmount sorts by transaction_amount.
func (v *JoinResult) SortByTransactionAmount(desc bool) {
	v.SumAmount.Sort(desc)
}

// Output outputs the result.
func (v *JoinResult) Output(writer io.Writer) error {
	const header = "transaction_category_id,sum_amount,num_users"

	if _, err := writer.Write([]byte(header + "\n")); err != nil {
		return errors.New("cannot write output: " + err.Error())
	}

	var row string
	for i, transactionCategoryID := range v.CategoryID {
		row = strconv.FormatInt(int64(transactionCategoryID), 10)
		row += "," + strconv.FormatInt(int64(v.SumAmount[i]), 10)
		row += "," + strconv.FormatInt(int64(v.NumUsers[i]), 10)
		row += "\n"

		if _, err := writer.Write(*(*[]byte)(unsafe.Pointer(&row))); err != nil {
			return errors.New("cannot write output: " + err.Error())
		}
	}

	return nil
}

type arrayUInt32 []uint32

func (x arrayUInt32) Len() int {
	return len(x)
}

func isNaN(f uint32) bool {
	return f != f
}

func (x arrayUInt32) Less(i, j int) bool { return x[i] < x[j] || (isNaN(x[i]) && !isNaN(x[j])) }

func (x arrayUInt32) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x arrayUInt32) Sort(desc bool) {
	if desc {
		sort.Sort(sort.Reverse(x))
	} else {
		sort.Sort(x)
	}
}

type transactionAggregate struct {
	SumAmount   uint32
	uniqueUsers map[uuid.UUID]struct{}
}

type joinInterim struct {
	v  map[uint8]transactionAggregate
	mu *sync.RWMutex
}

func (v *joinInterim) AddTransaction(transactionCategoryID uint8, userID uuid.UUID, amount uint32) {
	v.mu.RLock()
	o, ok := v.v[transactionCategoryID]
	if !ok {
		v.mu.Lock()
		v.v[transactionCategoryID] = transactionAggregate{
			SumAmount:   amount,
			uniqueUsers: UniqueUsers{userID: struct{}{}},
		}
		v.mu.Unlock()
	}
	v.mu.RUnlock()

	v.mu.Lock()
	o.uniqueUsers[userID] = struct{}{}
	o.SumAmount += amount
	v.mu.Unlock()
}

// ReadNJoinNonBlockedTransactionsWithActiveUsers reads transactions.csv and filters blocked transactions out.
// Every non blocked transaction is further filtered depending on the user status (join with active users).
// JoinResult is grouped by the transaction category.
func ReadNJoinNonBlockedTransactionsWithActiveUsers(in *os.File, activeUsers UniqueUsers) (
	JoinResult, error,
) {
	panic("todo")
}

func mustParseUUIDBytes(v []byte) uuid.UUID {
	o, err := uuid.ParseBytes(v)
	if err != nil {
		panic(err)
	}
	return o
}

// UniqueUsers defines the list of unique users.
type UniqueUsers map[uuid.UUID]struct{}

// ReadActiveUsers reads users.csv and filters not active UniqueUsers out.
func ReadActiveUsers(f io.ReaderAt) (UniqueUsers, error) {
	panic("todo")
	//const maxWorkers = 10
	//buf := make([]byte, 40)
	//f.ReadAt(buf, 0)
}
