package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_dailyUniqueTransactionsPerUser_Add(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		v    DailyUniqueTransactionsPerUser
		args args
		want DailyUniqueTransactionsPerUser
	}{
		{
			name: "add to empty",
			v:    DailyUniqueTransactionsPerUser{},
			args: args{
				"foo",
			},
			want: DailyUniqueTransactionsPerUser{"foo"},
		},
		{
			name: "add to existing, not unique",
			v:    DailyUniqueTransactionsPerUser{"foo"},
			args: args{
				"foo",
			},
			want: DailyUniqueTransactionsPerUser{"foo"},
		},
		{
			name: "add to existing, unique",
			v:    DailyUniqueTransactionsPerUser{"foo"},
			args: args{
				"bar",
			},
			want: DailyUniqueTransactionsPerUser{"foo", "bar"},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				tt.v.Add(tt.args.s)
				assert.Equal(t, tt.v, tt.want, "DailyUniqueTransactionsPerUser{}.Add() error")
			},
		)
	}
}

func Test_reader(t *testing.T) {
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name             string
		args             args
		wantTransactions transactions
		wantUsers        users
		wantErr          bool
	}{
		{
			name: "happy path: single row",
			args: args{
				r: strings.NewReader(
					`transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id
1cd42418-9f9a-42c2-bec5-ad8bc2bba426,2022-10-18,4c3bd08a-2615-49b9-b24e-563603b5a837,True,5471,0
`,
				),
			},
			wantTransactions: transactions{
				{
					TransactionID: "1cd42418-9f9a-42c2-bec5-ad8bc2bba426",
					Date: time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					),
					UserID:                "4c3bd08a-2615-49b9-b24e-563603b5a837",
					IsBlocked:             true,
					TransactionAmount:     5471,
					TransactionCategoryID: 0,
				},
			},
			wantUsers: map[string]map[time.Time]*userDailyUniqueTransactions{
				"4c3bd08a-2615-49b9-b24e-563603b5a837": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID:    &DailyUniqueTransactionsPerUser{"1cd42418-9f9a-42c2-bec5-ad8bc2bba426"},
						Count: 1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "happy path: duplicated rows",
			args: args{
				r: strings.NewReader(
					`transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id
1cd42418-9f9a-42c2-bec5-ad8bc2bba426,2022-10-18,4c3bd08a-2615-49b9-b24e-563603b5a837,True,5471,0
1cd42418-9f9a-42c2-bec5-ad8bc2bba426,2022-10-18,4c3bd08a-2615-49b9-b24e-563603b5a837,True,5471,0
`,
				),
			},
			wantTransactions: transactions{
				{
					TransactionID: "1cd42418-9f9a-42c2-bec5-ad8bc2bba426",
					Date: time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					),
					UserID:                "4c3bd08a-2615-49b9-b24e-563603b5a837",
					IsBlocked:             true,
					TransactionAmount:     5471,
					TransactionCategoryID: 0,
				},
			},
			wantUsers: map[string]map[time.Time]*userDailyUniqueTransactions{
				"4c3bd08a-2615-49b9-b24e-563603b5a837": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID:    &DailyUniqueTransactionsPerUser{"1cd42418-9f9a-42c2-bec5-ad8bc2bba426"},
						Count: 1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "happy path: unique rows",
			args: args{
				r: strings.NewReader(
					`transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id
1cd42418-9f9a-42c2-bec5-ad8bc2bba426,2022-10-18,4c3bd08a-2615-49b9-b24e-563603b5a837,True,5471,0
ba4bec80-97ad-4637-824d-e110218b7ed2,2022-10-18,4c3bd08a-2615-49b9-b24e-563603b5a837,True,1856,0
ea4f4705-e2f2-4833-9c5f-d0b261fc0dc0,2022-10-15,f1520d27-c7c2-4321-b411-61a7f8f61a26,True,6290,1
`,
				),
			},
			wantTransactions: transactions{
				{
					TransactionID: "1cd42418-9f9a-42c2-bec5-ad8bc2bba426",
					Date: time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					),
					UserID:                "4c3bd08a-2615-49b9-b24e-563603b5a837",
					IsBlocked:             true,
					TransactionAmount:     5471,
					TransactionCategoryID: 0,
				},
				{
					TransactionID: "ba4bec80-97ad-4637-824d-e110218b7ed2",
					Date: time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					),
					UserID:                "4c3bd08a-2615-49b9-b24e-563603b5a837",
					IsBlocked:             true,
					TransactionAmount:     1856,
					TransactionCategoryID: 0,
				},
				{
					TransactionID: "ea4f4705-e2f2-4833-9c5f-d0b261fc0dc0",
					Date: time.Date(
						2022, 10, 15, 0, 0, 0, 0,
						time.UTC,
					),
					UserID:                "f1520d27-c7c2-4321-b411-61a7f8f61a26",
					IsBlocked:             true,
					TransactionAmount:     6290,
					TransactionCategoryID: 1,
				},
			},
			wantUsers: map[string]map[time.Time]*userDailyUniqueTransactions{
				"4c3bd08a-2615-49b9-b24e-563603b5a837": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID: &DailyUniqueTransactionsPerUser{
							"1cd42418-9f9a-42c2-bec5-ad8bc2bba426", "ba4bec80-97ad-4637-824d-e110218b7ed2",
						},
						Count: 2,
					},
				},
				"f1520d27-c7c2-4321-b411-61a7f8f61a26": {
					time.Date(
						2022, 10, 15, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID:    &DailyUniqueTransactionsPerUser{"ea4f4705-e2f2-4833-9c5f-d0b261fc0dc0"},
						Count: 1,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				tr, users, err := reader(tt.args.r)
				if (err != nil) != tt.wantErr {
					t.Errorf("reader() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				assert.Equal(
					t, tt.wantTransactions, tr, "reader() transactions", tr, tt.wantTransactions,
				)
				assert.Equal(t, tt.wantUsers, users, "reader() users", users, tt.wantUsers)
			},
		)
	}
}

func Test_users_CalculateTotalTransactionsPrev7Days(t *testing.T) {
	tests := []struct {
		name string
		u    users
		want dailyTransactions7days
	}{
		{
			name: "2 users, 1: three dates in, two with non-0 out; 2: two dates in, 0 non-0 out",
			u: users{
				"4c3bd08a-2615-49b9-b24e-563603b5a837": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID: &DailyUniqueTransactionsPerUser{
							"1cd42418-9f9a-42c2-bec5-ad8bc2bba426", "ba4bec80-97ad-4637-824d-e110218b7ed2",
						},
						Count: 2,
					},
					time.Date(
						2022, 10, 12, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID: &DailyUniqueTransactionsPerUser{
							"b578f45d-36f0-4d30-a78b-575befb2b2f7",
							"3016a9af-6630-410f-b797-e65db92f4418",
						},
						Count: 2,
					},
					time.Date(
						2022, 10, 15, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID: &DailyUniqueTransactionsPerUser{
							"614f3a09-9cab-4271-8c3a-d612f5dd828c",
							"3949d893-2b56-40b7-a730-30ccc94a345f",
							"45ae98cb-2055-4018-9bdf-b14fbba596dd",
						},
						Count: 3,
					},
				},
				"28083761-e921-409f-bab4-9a9f694beab6": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID: &DailyUniqueTransactionsPerUser{
							"1cd42418-9f9a-42c2-bec5-ad8bc2bba426", "ba4bec80-97ad-4637-824d-e110218b7ed2",
						},
						Count: 2,
					},
					time.Date(
						2022, 10, 1, 0, 0, 0, 0,
						time.UTC,
					): &userDailyUniqueTransactions{
						ID: &DailyUniqueTransactionsPerUser{
							"b578f45d-36f0-4d30-a78b-575befb2b2f7",
							"3016a9af-6630-410f-b797-e65db92f4418",
						},
						Count: 2,
					},
				},
			},
			want: map[string]map[time.Time]int{
				"4c3bd08a-2615-49b9-b24e-563603b5a837": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): 5,
					time.Date(
						2022, 10, 15, 0, 0, 0, 0,
						time.UTC,
					): 2,
					time.Date(
						2022, 10, 12, 0, 0, 0, 0,
						time.UTC,
					): 0,
				},
				"28083761-e921-409f-bab4-9a9f694beab6": {
					time.Date(
						2022, 10, 18, 0, 0, 0, 0,
						time.UTC,
					): 0,
					time.Date(
						2022, 10, 1, 0, 0, 0, 0,
						time.UTC,
					): 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				assert.Equalf(
					t, tt.want, tt.u.CalculateTotalTransactionsPrev7Days(), "CalculateTotalTransactionsPrev7Days()",
				)
			},
		)
	}
}
