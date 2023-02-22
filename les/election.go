// Copyright 2018 The AbeyChain Authors
// This file is part of the abey library.
//
// The abey library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The abey library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the abey library. If not, see <http://www.gnu.org/licenses/>.

package les

import (
	"bytes"
	"context"
	"errors"
	"math/big"

	"github.com/abeychain/go-abey/common"
	"github.com/abeychain/go-abey/core/types"
	"github.com/abeychain/go-abey/crypto"
	"github.com/abeychain/go-abey/light"
	"github.com/abeychain/go-abey/log"
	"github.com/abeychain/go-abey/params"
	"github.com/hashicorp/golang-lru"
)

const (
	committeeCacheLimit = 256
	// The sha3 of empy switchinfo rlp encoded data
	emptyCommittee = "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
)

var (
	ErrCommittee     = errors.New("get committee failed")
	ErrInvalidMember = errors.New("invalid committee member")
	ErrInvalidSwitch = errors.New("invalid switch block info")
)

type Election struct {
	genesisCommittee []*types.CommitteeMember
	defaultMembers   []*types.CommitteeMember

	fastchain *light.LightChain

	commiteeCache *lru.Cache
	switchCache   *lru.Cache
}

type switchPoint struct {
	switches    []uint64
	checkNumber *big.Int
}

func ElectionEpoch(id *big.Int) (begin *big.Int, end *big.Int) {
	end = new(big.Int).Mul(id, params.ElectionPeriodNumber)
	end = end.Sub(end, params.SnailConfirmInterval)
	if id.Cmp(common.Big1) <= 0 {
		begin = big.NewInt(1)
	} else {
		begin = new(big.Int).Add(new(big.Int).Sub(end, params.ElectionPeriodNumber), common.Big1)
	}
	return
}

// NewLightElection create the instance of committee electioin
func NewLightElection(fastBlockChain *light.LightChain) *Election {
	// init
	election := &Election{
		fastchain: fastBlockChain,
	}
	election.commiteeCache, _ = lru.New(committeeCacheLimit)
	election.switchCache, _ = lru.New(committeeCacheLimit)

	// Genesis committee is stroed on block 0
	election.genesisCommittee = election.getGenesisCommittee()
	for _, m := range election.genesisCommittee {
		var member = *m
		//member.Flag = types.StateUnusedFlag
		election.defaultMembers = append(election.defaultMembers, &member)
	}
	return election
}

func (e *Election) Start() {
	num := e.fastchain.CurrentHeader().Number

	log.Info("Latest block", "number", num)
	members := e.GetCommittee(num)
	log.Info("Current committee", "count", len(members))
}

func (e *Election) getGenesisCommittee() []*types.CommitteeMember {
	block := e.fastchain.Genesis()
	if block != nil {
		return block.SwitchInfos()
	}
	return nil
}

func (e *Election) GenerateFakeSigns(fb *types.Block) ([]*types.PbftSign, error) {
	return nil, nil
}

// GetMemberByPubkey returns committeeMember specified by public key bytes
func (e *Election) GetMemberByPubkey(members []*types.CommitteeMember, publickey []byte) *types.CommitteeMember {
	if len(members) == 0 {
		log.Error("GetMemberByPubkey method len(members)= 0")
		return nil
	}
	for _, member := range members {
		if bytes.Equal(publickey, member.Publickey) {
			return member
		}
	}
	return nil
}

func (e *Election) GetMemberFlag(members []*types.CommitteeMember, publickey []byte) uint32 {
	if len(members) == 0 {
		log.Error("IsCommitteeMember method len(members)= 0")
		return 0
	}
	for _, member := range members {
		if bytes.Equal(publickey, member.Publickey) {
			return member.Flag
		}
	}
	return 0
}

// IsCommitteeMember reports whether the provided public key is in committee
func (e *Election) IsCommitteeMember(members []*types.CommitteeMember, publickey []byte) bool {
	flag := e.GetMemberFlag(members, publickey)
	return flag == types.StateUsedFlag
}

// VerifyPublicKey get the committee member by public key
func (e *Election) VerifyPublicKey(fastHeight *big.Int, pubKeyByte []byte) (*types.CommitteeMember, error) {
	members := e.GetCommittee(fastHeight)
	if members == nil {
		log.Info("GetCommittee members is nil", "fastHeight", fastHeight)
		return nil, ErrCommittee
	}
	member := e.GetMemberByPubkey(members, pubKeyByte)
	/*if member == nil {
		return nil, ErrInvalidMember
	}*/
	return member, nil
}

// VerifySign lookup the pbft sign and return the committee member who signs it
func (e *Election) VerifySign(sign *types.PbftSign) (*types.CommitteeMember, error) {
	pubkey, err := crypto.SigToPub(sign.HashWithNoSign().Bytes(), sign.Sign)
	if err != nil {
		return nil, err
	}
	pubkeyByte := crypto.FromECDSAPub(pubkey)
	member, err := e.VerifyPublicKey(sign.FastHeight, pubkeyByte)
	return member, err
}

// VerifySigns verify signatures of bft committee in batches
func (e *Election) VerifySigns(signs []*types.PbftSign) ([]*types.CommitteeMember, []error) {
	members := make([]*types.CommitteeMember, len(signs))
	errs := make([]error, len(signs))

	if len(signs) == 0 {
		log.Warn("Veriry signs get nil pbftsigns")
		return nil, nil
	}
	// All signs should have the same fastblock height
	committeeMembers := e.GetCommittee(signs[0].FastHeight)
	if len(committeeMembers) == 0 {
		log.Error("Election get none committee for verify pbft signs")
		for i := range errs {
			errs[i] = ErrCommittee
		}
		return members, errs
	}

	for i, sign := range signs {
		// member, err := e.VerifySign(sign)
		pubkey, _ := crypto.SigToPub(sign.HashWithNoSign().Bytes(), sign.Sign)
		member := e.GetMemberByPubkey(committeeMembers, crypto.FromECDSAPub(pubkey))
		if member == nil {
			errs[i] = ErrInvalidMember
		} else {
			members[i] = member
		}
	}
	return members, errs
}

// VerifySwitchInfo verify committee members and it's state
func (e *Election) VerifySwitchInfo(fastNumber *big.Int, info []*types.CommitteeMember) error {
	return nil
}

// GetCommittee gets committee members which propose the fast block
func (e *Election) GetCommittee(fastNumber *big.Int) []*types.CommitteeMember {
	if params.LesProtocolGenesisBlock > fastNumber.Uint64() {
		return nil
	}
	begin, _, epochid := LesEpochFromHeight(fastNumber.Uint64())

	c := e.getCommittee(big.NewInt(int64(epochid)))

	// Load switch block to calculate committee members
	switches := e.loadSwitchPoint(big.NewInt(int64(epochid)), big.NewInt(int64(begin)), fastNumber)
	if len(switches) > 0 {
		return e.filterWithSwitchInfo(c, fastNumber, switches)
	} else {
		return c.Members
	}
}

func (e *Election) loadSwitchPoint(id *big.Int, begin *big.Int, fastNumber *big.Int) []uint64 {
	var (
		switches     []uint64
		switchBlocks *switchPoint
	)

	if cache, ok := e.switchCache.Get(id.Uint64()); ok {
		switchBlocks = cache.(*switchPoint)
		begin = switchBlocks.checkNumber
		switches = switchBlocks.switches
	}
	emptyCommitteeHash := common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")
	// Retrieve block including switchinfo
	for i := begin.Uint64() + 1; i < fastNumber.Uint64(); i++ {
		head := e.fastchain.GetHeaderByNumber(i)
		if head.CommitteeHash == emptyCommitteeHash {
			continue
		}
		log.Info("Light committee apply switchinfo", "number", i)
		switches = append(switches, i)
	}
	if fastNumber.Cmp(begin) > 0 && (switchBlocks == nil || fastNumber.Cmp(switchBlocks.checkNumber) > 0) {
		switchBlocks = &switchPoint{
			checkNumber: fastNumber,
			switches:    switches,
		}
		e.switchCache.Add(id.Uint64(), switchBlocks)
	}
	return switches
}

func (e *Election) filterWithSwitchInfo(c *types.ElectionCommittee, fastNumber *big.Int, switches []uint64) (members []*types.CommitteeMember) {

	if len(switches) == 0 {
		log.Info("Committee filter get no switch infos", "number", fastNumber)
		members = c.Members
		return
	}

	states := make(map[common.Address]uint32)
	for _, num := range switches {
		if num >= fastNumber.Uint64() {
			break
		}
		b := e.GetCommitteeFromFullnode(big.NewInt(int64(num)))
		if b == nil {
			log.Warn("Switch block not exists", "number", num)
			break
		}
		for _, s := range b.Members {
			switch s.Flag {
			case types.StateAppendFlag:
				states[s.CommitteeBase] = types.StateAppendFlag
			case types.StateRemovedFlag:
				states[s.CommitteeBase] = types.StateRemovedFlag
			}
		}
	}

	for _, m := range c.Members {
		if flag, ok := states[m.CommitteeBase]; ok {
			if flag != types.StateRemovedFlag {
				members = append(members, m)
			}
		} else {
			members = append(members, m)
		}
	}
	for _, m := range c.Backups {
		if flag, ok := states[m.CommitteeBase]; ok {
			if flag == types.StateAppendFlag {
				members = append(members, m)
			}
		}
	}

	return
}
func (e *Election) GetCommitteeFromFullnode(id *big.Int) *types.ElectionCommittee {
	// TODO get the committee from the full node by rpc
	height, _ := LesEpochToHeight(id.Uint64())
	if block, err := e.fastchain.GetBlockByNumber(context.Background(), height); err != nil {
		log.Error("light chain GetBlockByNumber err", "height", height, "err", err)
		return &types.ElectionCommittee{Members: e.defaultMembers}
	} else {
		infos := block.SwitchInfos()
		if infos != nil {
			return &types.ElectionCommittee{Members: infos}
		} else {
			return &types.ElectionCommittee{Members: e.defaultMembers}
		}
	}
}
func (e *Election) getCommittee(id *big.Int) *types.ElectionCommittee {
	if cache, ok := e.commiteeCache.Get(id.Uint64()); ok {
		committee := cache.(*types.ElectionCommittee)
		return committee
	}

	var c *types.ElectionCommittee
	if id.Cmp(common.Big0) == 0 {
		// genesis committee for committee 0
		c = &types.ElectionCommittee{Members: e.genesisCommittee}
	} else {
		c = e.GetCommitteeFromFullnode(id)
		log.Info("Committee members", "committee", id, "count", len(c.Members), "backup", len(c.Backups))
	}
	e.commiteeCache.Add(id.Uint64(), c)
	return c
}

// FinalizeCommittee upddate current committee state
func (e *Election) FinalizeCommittee(block *types.Block) error {
	return nil
}
