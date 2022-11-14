/*
 Copyright 2022 Dmitry Kisler <admin@dkisler.com>

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
*/

package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_readUsers(t *testing.T) {
	type args struct {
		f io.Reader
	}

	tests := []struct {
		name    string
		args    args
		want    UniqueUsers
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "happy path, 2 active out of 3 uniqueUsers",
			args: args{
				bytes.NewReader(
					[]byte(`user_id,is_active
ede06128-d6c3-4203-a28e-06adadc6d2db,True
d07f1e39-4b31-406a-a5b8-b7ae6be878d9,True
c27ce481-69d7-4f54-9ffb-73c8f582d5a0,False
`),
				),
			},
			want: UniqueUsers{
				mustParseUUIDBytes([]byte("ede06128-d6c3-4203-a28e-06adadc6d2db")): {},
				mustParseUUIDBytes([]byte("d07f1e39-4b31-406a-a5b8-b7ae6be878d9")): {},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := ReadActiveUsers(tt.args.f)
				if !tt.wantErr(t, err, fmt.Sprintf("ReadActiveUsers(%v)", tt.args.f)) {
					return
				}
				assert.Equal(t, tt.want, got, "ReadActiveUsers() error")
			},
		)
	}
}

func TestJoinResult_Output(t *testing.T) {
	type fields struct {
		CategoryID []uint8
		NumUsers   arrayUInt32
		SumAmount  arrayUInt32
	}
	tests := []struct {
		name       string
		fields     fields
		wantWriter string
		wantErr    assert.ErrorAssertionFunc
	}{
		{
			name: "happy path",
			fields: fields{
				CategoryID: []uint8{0, 1},
				NumUsers:   arrayUInt32{1, 2},
				SumAmount:  arrayUInt32{10, 12},
			},
			wantWriter: `transaction_category_id,sum_amount,num_users
0,10,1
1,12,2
`,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				v := &JoinResult{
					CategoryID: tt.fields.CategoryID,
					NumUsers:   tt.fields.NumUsers,
					SumAmount:  tt.fields.SumAmount,
				}
				writer := &bytes.Buffer{}
				err := v.Output(writer)
				if !tt.wantErr(t, err, fmt.Sprintf("Output(%v)", writer)) {
					return
				}
				assert.Equalf(t, tt.wantWriter, writer.String(), "Output(%Data)", writer)
			},
		)
	}
}

func TestJoinResult_Sort(t *testing.T) {
	type fields struct {
		CategoryID []uint8
		NumUsers   arrayUInt32
		SumAmount  arrayUInt32
	}
	type args struct {
		desc bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   JoinResult
	}{
		{
			name: "3 elements, desc",
			fields: fields{
				CategoryID: []uint8{0, 1, 2},
				NumUsers:   arrayUInt32{2, 1, 4},
				SumAmount:  arrayUInt32{10, 30, 20},
			},
			args: args{true},
			want: JoinResult{
				CategoryID: []uint8{1, 2, 0},
				NumUsers:   arrayUInt32{1, 4, 2},
				SumAmount:  arrayUInt32{30, 20, 10},
			},
		},
		{
			name: "3 elements, asc",
			fields: fields{
				CategoryID: []uint8{0, 1, 2},
				NumUsers:   arrayUInt32{2, 1, 4},
				SumAmount:  arrayUInt32{10, 30, 20},
			},
			args: args{false},
			want: JoinResult{
				CategoryID: []uint8{0, 2, 1},
				NumUsers:   arrayUInt32{2, 4, 1},
				SumAmount:  arrayUInt32{10, 20, 30},
			},
		},
		{
			name: "4 elements, desc, two have the same amount",
			fields: fields{
				CategoryID: []uint8{0, 3, 2, 1},
				NumUsers:   arrayUInt32{2, 1, 4, 3},
				SumAmount:  arrayUInt32{10, 30, 20, 30},
			},
			args: args{true},
			want: JoinResult{
				CategoryID: []uint8{3, 1, 2, 0},
				NumUsers:   arrayUInt32{1, 3, 4, 2},
				SumAmount:  arrayUInt32{30, 30, 20, 10},
			},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				x := JoinResult{
					CategoryID: tt.fields.CategoryID,
					NumUsers:   tt.fields.NumUsers,
					SumAmount:  tt.fields.SumAmount,
				}
				x.Sort(tt.args.desc)
			},
		)
	}
}

func TestReadNJoinNonBlockedTransactionsWithActiveUsers(t *testing.T) {
	type args struct {
		in          io.Reader
		activeUsers UniqueUsers
	}
	tests := []struct {
		name    string
		args    args
		want    JoinResult
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "happy path",
			args: args{
				activeUsers: UniqueUsers{
					mustParseUUIDBytes([]byte("ede06128-d6c3-4203-a28e-06adadc6d2db")): {},
					mustParseUUIDBytes([]byte("d07f1e39-4b31-406a-a5b8-b7ae6be878d9")): {},
				},
				in: bytes.NewReader(
					[]byte(`transaction_id,date,user_id,is_blocked,transaction_amount,transaction_category_id
7035ce25-9e5d-488e-88e3-4d131dde2687,2022-10-16,ede06128-d6c3-4203-a28e-06adadc6d2db,True,7561,4
c5add928-2cab-4b6e-a22b-d31254651909,2022-08-07,ede06128-d6c3-4203-a28e-06adadc6d2db,False,5148,7
4580e86c-8e9c-43b8-b85f-754ee3364ad3,2022-10-18,d07f1e39-4b31-406a-a5b8-b7ae6be878d9,True,2664,9
8ac6bd39-5567-4ea7-8fa4-713336877d32,2022-09-29,d07f1e39-4b31-406a-a5b8-b7ae6be878d9,True,1343,4
dab6559a-1d91-4edb-9765-cbbd603a5b08,2022-08-29,d07f1e39-4b31-406a-a5b8-b7ae6be878d9,False,5654,3
`),
				),
			},
			want: JoinResult{
				CategoryID: []uint8{7, 3},
				NumUsers:   arrayUInt32{1, 1},
				SumAmount:  arrayUInt32{5148, 5654},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := ReadNJoinNonBlockedTransactionsWithActiveUsers(tt.args.in, tt.args.activeUsers)
				if !tt.wantErr(
					t, err, fmt.Sprintf(
						"ReadNJoinNonBlockedTransactionsWithActiveUsers(%v, %v)", tt.args.in, tt.args.activeUsers,
					),
				) {
					return
				}
				assert.Equalf(
					t, tt.want, got, "ReadNJoinNonBlockedTransactionsWithActiveUsers(%v, %v)", tt.args.in,
					tt.args.activeUsers,
				)
			},
		)
	}
}
