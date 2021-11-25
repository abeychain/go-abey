package parallel

import (
	"github.com/abeychain/go-abey/common"
)

type StorageAddress struct {
	accountAddress common.Address
	key            common.Hash
}

type TouchedAddressObject struct {
	accountOp map[common.Address]bool
	storageOp map[StorageAddress]bool
}

func NewTouchedAddressObject() *TouchedAddressObject {
	return &TouchedAddressObject{}
}

func (self *TouchedAddressObject) AccountOp() map[common.Address]bool {
	return self.accountOp
}

func (self *TouchedAddressObject) AddAccountOp(add common.Address, op bool) {
	if op {
		self.accountOp[add] = op
	} else {
		if _, exist := self.accountOp[add]; !exist {
			self.accountOp[add] = op
		}
	}
}

func (self *TouchedAddressObject) StorageOp() map[StorageAddress]bool {
	return self.storageOp
}

func (self *TouchedAddressObject) AddStorageOp(add StorageAddress, op bool) {
	if op {
		self.storageOp[add] = op
	} else {
		if _, exist := self.storageOp[add]; !exist {
			self.storageOp[add] = op
		}
	}
}

