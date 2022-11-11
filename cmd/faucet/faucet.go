// Copyright 2017 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// faucet is an Ether faucet backed by a light client.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
)

// authTwitterWithTokenV1 tries to authenticate a faucet request using Twitter's v1
// API, returning the user id, username, avatar URL and Ethereum address to fund on
// success.
func authTwitterWithTokenV1(tweetID string, token string) (string, string, string, common.Address, error) {
	// Query the tweet details from Twitter
	url := fmt.Sprintf("https://api.twitter.com/1.1/statuses/show.json?id=%s", tweetID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	defer res.Body.Close()

	var result struct {
		Text string `json:"text"`
		User struct {
			ID       string `json:"id_str"`
			Username string `json:"screen_name"`
			Avatar   string `json:"profile_image_url"`
		} `json:"user"`
	}
	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(result.Text))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("No ABEYCHAIN address found to fund")
	}
	return result.User.ID + "@twitter", result.User.Username, result.User.Avatar, address, nil
}

// authTwitterWithTokenV2 tries to authenticate a faucet request using Twitter's v2
// API, returning the user id, username, avatar URL and Ethereum address to fund on
// success.
func authTwitterWithTokenV2(tweetID string, token string) (string, string, string, common.Address, error) {
	// Query the tweet details from Twitter
	url := fmt.Sprintf("https://api.twitter.com/2/tweets/%s?expansions=author_id&user.fields=profile_image_url", tweetID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", "", common.Address{}, err
	}
	defer res.Body.Close()

	var result struct {
		Data struct {
			AuthorID string `json:"author_id"`
			Text     string `json:"text"`
		} `json:"data"`
		Includes struct {
			Users []struct {
				ID       string `json:"id"`
				Username string `json:"username"`
				Avatar   string `json:"profile_image_url"`
			} `json:"users"`
		} `json:"includes"`
	}

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return "", "", "", common.Address{}, err
	}

	address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(result.Data.Text))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", "", common.Address{}, errors.New("No ABEYCHAIN address found to fund")
	}
	return result.Data.AuthorID + "@twitter", result.Includes.Users[0].Username, result.Includes.Users[0].Avatar, address, nil
}

// authFacebook tries to authenticate a faucet request using Facebook posts,
// returning the username, avatar URL and ABEYChain address to fund on success.
func authFacebook(url string) (string, string, common.Address, error) {
	// Ensure the user specified a meaningful URL, no fancy nonsense
	parts := strings.Split(strings.Split(url, "?")[0], "/")
	if parts[len(parts)-1] == "" {
		parts = parts[0 : len(parts)-1]
	}
	if len(parts) < 4 || parts[len(parts)-2] != "posts" {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", common.Address{}, errors.New("Invalid Facebook post URL")
	}
	username := parts[len(parts)-3]

	// Facebook's Graph API isn't really friendly with direct links. Still, we don't
	// want to do ask read permissions from users, so just load the public posts and
	// scrape it for the ABEYChain address and profile URL.
	//
	// Facebook recently changed their desktop webpage to use AJAX for loading post
	// content, so switch over to the mobile site for now. Will probably end up having
	// to use the API eventually.
	crawl := strings.Replace(url, "www.facebook.com", "m.facebook.com", 1)

	res, err := http.Get(crawl)
	if err != nil {
		return "", "", common.Address{}, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", "", common.Address{}, err
	}
	address := common.HexToAddress(string(regexp.MustCompile("0x[0-9a-fA-F]{40}").Find(body)))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", common.Address{}, errors.New("No ABEYChain address found to fund")
	}
	var avatar string
	if parts = regexp.MustCompile(`src="([^"]+fbcdn\.net[^"]+)"`).FindStringSubmatch(string(body)); len(parts) == 2 {
		avatar = parts[1]
	}
	return username + "@facebook", avatar, address, nil
}

// authNoAuth tries to interpret a faucet request as a plain ABEYChain address,
// without actually performing any remote authentication. This mode is prone to
// Byzantine attack, so only ever use for truly private networks.
func authNoAuth(url string) (string, string, common.Address, error) {
	address := common.HexToAddress(regexp.MustCompile("0x[0-9a-fA-F]{40}").FindString(url))
	if address == (common.Address{}) {
		//lint:ignore ST1005 This error is to be displayed in the browser
		return "", "", common.Address{}, errors.New("No ABEYChain address found to fund")
	}
	return address.Hex() + "@noauth", "", address, nil
}

// getGenesis returns a genesis based on input args
func getGenesis(genesisFlag string, testnetFlag bool, devnetFlag bool) (*core.Genesis, error) {
	switch {
	case genesisFlag != "":
		var genesis core.Genesis
		err := common.LoadJSON(genesisFlag, &genesis)
		return &genesis, err
	case testnetFlag:
		return core.DefaultTestnetGenesisBlock(), nil
	case devnetFlag:
		return core.DefaultDevGenesisBlock(), nil
	default:
		return nil, fmt.Errorf("no genesis flag provided")
	}
}
