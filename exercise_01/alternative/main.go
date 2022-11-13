package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
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

	t0 := time.Now()
	fIn, err := os.Open(basePath + "/users.csv")
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

	joinResult.Sort(true)

	if err := joinResult.Output(os.Stdout); err != nil {
		log.Fatalln(err)
	}
	log.Printf("elapsed time: %d microseconds\n", time.Now().Sub(t0).Microseconds())
}

// JoinResult results stored pre-sorted in desc order.
type JoinResult struct {
	CategoryID []uint8
	NumUsers   arrayUInt32
	SumAmount  arrayUInt32
}

func (x JoinResult) Len() int {
	return len(x.CategoryID)
}

func (x JoinResult) Less(i, j int) bool {
	return x.SumAmount[i] < x.SumAmount[j] || (isNaNUint32(x.SumAmount[i]) && !isNaNUint32(x.SumAmount[j]))
}

func (x JoinResult) Swap(i, j int) {
	x.CategoryID[i], x.CategoryID[j] = x.CategoryID[j], x.CategoryID[i]
	x.NumUsers[i], x.NumUsers[j] = x.NumUsers[j], x.NumUsers[i]
	x.SumAmount[i], x.SumAmount[j] = x.SumAmount[j], x.SumAmount[i]
}

// Sort sorts by the total amount.
// It uses the default Go sort which is based on pdqsort, see the paper: https://arxiv.org/pdf/2106.05123.pdf.
func (x JoinResult) Sort(desc bool) {
	if desc {
		sort.Sort(sort.Reverse(x))
	} else {
		sort.Sort(x)
	}
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

func isNaNUint32(f uint32) bool {
	return f != f
}

type transactionAggregate struct {
	SumAmount   uint32
	uniqueUsers UniqueUsers
}

func (v transactionAggregate) NumberUniqueUsers() uint32 {
	return uint32(len(v.uniqueUsers))
}

type joinInterim struct {
	Data map[uint8]transactionAggregate
	mu   *sync.RWMutex
}

func (v *joinInterim) Len() int {
	return len((*v).Data)
}

func (v *joinInterim) AddTransaction(transactionCategoryID uint8, userID uuid.UUID, amount uint32) {
	v.mu.RLock()
	o, ok := v.Data[transactionCategoryID]
	v.mu.RUnlock()

	v.mu.Lock()

	if !ok {
		o = transactionAggregate{
			SumAmount:   0,
			uniqueUsers: UniqueUsers{},
		}
	}

	o.SumAmount += amount
	o.uniqueUsers[userID] = struct{}{}
	v.Data[transactionCategoryID] = o

	v.mu.Unlock()
}

// ReadNJoinNonBlockedTransactionsWithActiveUsers reads transactions.csv and filters blocked transactions out.
// Every non blocked transaction is further filtered depending on the user status (join with active users).
// JoinResult is grouped by the transaction category.
func ReadNJoinNonBlockedTransactionsWithActiveUsers(f io.Reader, activeUsers UniqueUsers) (
	JoinResult, error,
) {
	sc := bufio.NewScanner(f)
	sc.Split(bufio.ScanLines)

	joinBuf := joinInterim{
		Data: map[uint8]transactionAggregate{},
		mu:   &sync.RWMutex{},
	}

	rowCounter := 0
	for sc.Scan() {
		if rowCounter == 0 {
			rowCounter++
			continue
		}

		v := sc.Bytes()

		// filter blocked transactions out
		if byteArrayContainTrue(v[85:90]) {
			continue
		}

		userID, err := uuid.ParseBytes(v[48:84])
		if err != nil {
			return JoinResult{}, errors.New("cannot parse user_id: " + err.Error())

		}

		// join condition
		if _, ok := activeUsers[userID]; !ok {
			continue
		}

		// parse two last columns
		var (
			transactionCategoryID uint8
			transactionAmount     uint32
		)

		i := len(v) - 1
		r := len(v) - 1
		parsed := 0
		for parsed < 2 {
			if v[i] == ',' {
				if r == len(v)-1 {
					v, err := strconv.ParseUint(string(v[i+1:]), 10, 8)
					if err != nil {
						return JoinResult{}, errors.New("cannot parse transaction_category_id: " + err.Error())
					}
					transactionCategoryID = uint8(v)
				} else {
					v, err := strconv.ParseUint(string(v[i+1:r]), 10, 32)
					if err != nil {
						return JoinResult{}, errors.New("cannot parse transaction_amount: " + err.Error())
					}
					transactionAmount = uint32(v)
				}
				r = i
				parsed++
			}
			i--
		}

		// validation
		_, err = uuid.ParseBytes(v[:36])
		if err != nil {
			return JoinResult{}, errors.New("cannot parse transaction_id: " + err.Error())
		}

		_, err = time.Parse("2006-01-02", string(v[37:47]))
		if err != nil {
			return JoinResult{}, errors.New("cannot parse date: " + err.Error())
		}

		joinBuf.AddTransaction(transactionCategoryID, userID, transactionAmount)

		rowCounter++

	}

	o := JoinResult{
		CategoryID: make([]uint8, joinBuf.Len()),
		NumUsers:   make(arrayUInt32, joinBuf.Len()),
		SumAmount:  make(arrayUInt32, joinBuf.Len()),
	}

	i := 0
	for categoryID, d := range joinBuf.Data {
		o.CategoryID[i] = categoryID
		o.NumUsers[i] = d.NumberUniqueUsers()
		o.SumAmount[i] = d.SumAmount
		i++
	}

	return o, nil
}

func byteArrayContainTrue(v []byte) bool {
	return v[0] == 't' || v[0] == 'T' || v[0] == '1'
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
func ReadActiveUsers(f io.Reader) (UniqueUsers, error) {
	sc := bufio.NewScanner(f)
	sc.Split(bufio.ScanLines)

	o := UniqueUsers{}

	rowCounter := 0
	for sc.Scan() {
		if rowCounter == 0 {
			rowCounter++
			continue
		}

		userID, err := uuid.ParseBytes(sc.Bytes()[:36])
		if err != nil {
			return nil, errors.New("[" + strconv.Itoa(rowCounter) + "] " + err.Error())
		}

		// filter not active users
		if !byteArrayContainTrue(sc.Bytes()[37:40]) {
			continue
		}
		o[userID] = struct{}{}

	}

	return o, nil
}
