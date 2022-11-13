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
		f io.ReaderAt
	}

	tests := []struct {
		name    string
		args    args
		want    UniqueUsers
		wantErr bool
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := ReadActiveUsers(tt.args.f)
				if (err != nil) != tt.wantErr {
					t.Errorf("ReadActiveUsers() error = %v, wantErr %v", err, tt.wantErr)
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
				assert.Equalf(t, tt.wantWriter, writer.String(), "Output(%v)", writer)
			},
		)
	}
}
