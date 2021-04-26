package main

import (
	"testing"
)

func TestHexToAbey(t *testing.T) {
	type args struct {
		hex string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test-1",
			args: args{
				hex: "0x46498c274686bE5e3c01B9268eA4604dA5142265",
			},
			want: "ABEYFdsRAZYV4EsAmjB9zkUTu3b8WVCGHTFu9",
		},
		{
			name: "test-2",
			args: args{
				hex: "0x62ba473C78C777fa7Cc1aC17A7D02Be0A5294A21",
			},
			want: "ABEYJEFPD7b3mTXUF9nHVhU9ngQMTqFQMkSqd",
		},
		{
			name: "test-3",
			args: args{
				hex: "0x6B46c8DB05cbD02bd4a4b3E425f3Dca8080D3866",
			},
			want: "ABEYK1T7J4WNqFmPHZRHrrCsX7yUXmEecJJvK",
		},
		{
			name: "test-4",
			args: args{
				hex: "0x62ba473C78C7e0A5294A21",
			},
			want: "ABEY9EE1Zypi4uaGA8c15VH8M4tqJsfuNLbCY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HexToAbey(tt.args.hex); got != tt.want {
				t.Errorf("HexToAbey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAbeyToHex(t *testing.T) {
	type args struct {
		abey string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test-1",
			args: args{
				abey: "ABEYFdsRAZYV4EsAmjB9zkUTu3b8WVCGHTFu9",
			},
			want:    "0x46498c274686bE5e3c01B9268eA4604dA5142265",
			wantErr: false,
		},
		{
			name: "test-2",
			args: args{
				abey: "ABEYJEFPD7b3mTXUF9nHVhU9ngQMTqFQMkSqd",
			},
			want:    "0x62ba473C78C777fa7Cc1aC17A7D02Be0A5294A21",
			wantErr: false,
		},
		{
			name: "test-3",
			args: args{
				abey: "ABEYK1T7J4WNqFmPHZRHrrCsX7yUXmEecJJvK",
			},
			want: "0x6B46c8DB05cbD02bd4a4b3E425f3Dca8080D3866",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AbeyToHex(tt.args.abey)
			if (err != nil) != tt.wantErr {
				t.Errorf("AbeyToHex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AbeyToHex() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeAddress(t *testing.T) {
	makeAddress(2)
}

// convert abey ABEYFdsRAZYV4EsAmjB9zkUTu3b8WVCGHTFu9 // 0x46498c274686bE5e3c01B9268eA4604dA5142265
// convert hex 0x46498c274686bE5e3c01B9268eA4604dA5142265  // ABEYFdsRAZYV4EsAmjB9zkUTu3b8WVCGHTFu9
