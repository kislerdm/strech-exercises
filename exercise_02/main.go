// app to calculate the reference
package main

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	basePath := os.Getenv("BASE_DIR")
	if basePath == "" {
		log.Fatalln("BASE_DIR env variable must be specified")
	}

	if err := Generate(basePath); err != nil {
		log.Fatalln(err)
	}
}

type transaction struct {
	TransactionID         string
	Date                  time.Time
	UserID                string
	IsBlocked             bool
	TransactionAmount     int
	TransactionCategoryID int
}

type transactions []transaction

func (tr *transactions) Add(t transaction) {
	for _, el := range *tr {
		if t.TransactionID == el.TransactionID && t.UserID == el.UserID {
			return
		}
	}
	*tr = append(*tr, t)
}

type dailyUniqueTransactionsPerUser []string

func (v *dailyUniqueTransactionsPerUser) Add(s string) {
	for _, el := range *v {
		if s == el {
			return
		}
	}
	*v = append(*v, s)
}

type userDailyUniqueTransactions struct {
	ID    *dailyUniqueTransactionsPerUser
	Count int
}

func (v *userDailyUniqueTransactions) Add(transactionID string) {
	v.ID.Add(transactionID)
	v.Count = len(*v.ID)
}

type users map[string]map[time.Time]*userDailyUniqueTransactions

func (u users) Add(userID string, transactionID string, date time.Time) {
	if _, ok := u[userID]; !ok {
		u[userID] = map[time.Time]*userDailyUniqueTransactions{
			date: {
				ID:    &dailyUniqueTransactionsPerUser{transactionID},
				Count: 1,
			},
		}
		return
	}

	if _, ok := u[userID][date]; !ok {
		u[userID][date] = &userDailyUniqueTransactions{
			ID:    &dailyUniqueTransactionsPerUser{transactionID},
			Count: 1,
		}
	}

	u[userID][date].Add(transactionID)
}

type dailyTransactions7days map[string]map[time.Time]int

func (u users) CalculateTotalTransactionsPrev7Days() dailyTransactions7days {
	o := dailyTransactions7days{}
	for userID, dailyTransactions := range u {
		o[userID] = map[time.Time]int{}
		for date, _ := range dailyTransactions {
			o[userID][date] = countCumulativePrevDays(dailyTransactions, date, 7)
		}
	}
	return o
}

func countCumulativePrevDays(
	dailyTransactions map[time.Time]*userDailyUniqueTransactions, date time.Time, lookBackDays int,
) int {
	var counter int
	for d, t := range dailyTransactions {
		if date.Sub(d) > 0 && int(date.Sub(d).Hours()) <= lookBackDays*24 {
			counter += t.Count
		}
	}
	return counter
}

func toBool(s string) bool {
	return s[0] == 't' || s[0] == 'T' || s[0] == '1'
}

func reader(r io.Reader) (transactions, users, error) {
	csvReader := csv.NewReader(r)

	rowInd := 0

	transactions := transactions{}
	users := users{}

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, errors.New("error parsing the row <" + strconv.Itoa(rowInd) + ">: " + err.Error())
		}

		if rowInd == 0 {
			rowInd++
			continue
		}

		t := transaction{
			TransactionID: row[0],
			UserID:        row[2],
			IsBlocked:     toBool(row[3]),
		}

		t.Date, err = time.Parse("2006-01-02", row[1])
		if err != nil {
			return nil, nil, errors.New("error parsing the row <" + strconv.Itoa(rowInd) + ">[column:date]: " + err.Error())
		}

		t.TransactionAmount, err = strconv.Atoi(row[4])
		if err != nil {
			return nil, nil, errors.New("error parsing the row <" + strconv.Itoa(rowInd) + ">[column:transaction_amount]: " + err.Error())
		}

		t.TransactionCategoryID, err = strconv.Atoi(row[5])
		if err != nil {
			return nil, nil, errors.New("error parsing the row <" + strconv.Itoa(rowInd) + ">[column:transaction_category_id]: " + err.Error())
		}

		users.Add(t.UserID, t.TransactionID, t.Date)

		transactions.Add(t)
	}

	return transactions, users, nil
}

func Generate(baseDir string) error {
	if baseDir == "" {
		return errors.New("base dir must be specified")
	}

	pathIn := baseDir + "/transactions.csv"

	fIn, err := os.Open(pathIn)
	if err != nil {
		return err
	}
	defer func() { _ = fIn.Close() }()

	transactions, users, err := reader(fIn)
	if err != nil {
		return err
	}

	log.Printf("generate aggregates for %d users\n", len(users))
	results, err := join(transactions, users.CalculateTotalTransactionsPrev7Days())
	if err != nil {
		return err
	}

	pathOut := baseDir + "/want.csv"

	fOut, err := os.Create(pathOut)
	if err != nil {
		return err
	}
	defer func() { _ = fOut.Close() }()

	return writer(results, []string{"transaction_id", "user_id", "date", "total_lookup_7days"}, fOut)
}

func join(t transactions, dailyTransactions dailyTransactions7days) ([]resultRow, error) {
	var o []resultRow
	for _, tr := range t {
		o = append(
			o, resultRow{
				Date:          tr.Date,
				TransactionID: tr.TransactionID,
				UserID:        tr.UserID,
				Cnt7Days:      dailyTransactions[tr.UserID][tr.Date],
			},
		)
	}

	return o, nil
}

type resultRow struct {
	Date          time.Time
	TransactionID string
	UserID        string
	Cnt7Days      int
}

func (r resultRow) ToCSVRow() []string {
	return []string{r.TransactionID, r.UserID, r.Date.Format("2006-01-02"), strconv.Itoa(r.Cnt7Days)}
}

func writer(results []resultRow, header []string, fout *os.File) error {
	w := csv.NewWriter(fout)
	if err := w.Write(header); err != nil {
		return err
	}
	for _, row := range results {
		if err := w.Write(row.ToCSVRow()); err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}
