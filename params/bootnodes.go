// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

// MainnetBootnodes are the enode URLs of the P2P bootstrap nodes running on
// the main Abeychain network.
var MainnetBootnodes = []string{
	"enode://ddeb1ebc489fe85b37586dc4b38052b7186f2e166d07d3127757afd3698240cc197cc5712bf82c0fbe13c6ab2d4c9a6ba012a057735d3f33b9fffc1c1e878c45@107.22.156.116:30313",
	"enode://ca456572021ad267b0e833c20d707ab7ed5faf908829505d04d85e593cd849e3ece94eb8522aef400574788db54591c8a076671c17a2595ee3cf57d0fed4f21a@13.57.236.253:30313",
	"enode://031569aae8f6ab0bb19fc0feb719b5312fffcee3f703e8719aeca6cc1ae338889e23f85d6d4f92926dbf62aa2c8d7f40637e9202b34e0cfdb6910184cc5f0a39@3.66.27.8:30313",
	"enode://49c2cd85519592bb9094007ae55b46588ca478c3208cad64c33af8d538ad2537cdc7951063ca3b5f6d6f4d4b4247b98e037100b1c9f9e9dfd07f1b113496d324@13.250.40.243:30313",
}

// TestnetBootnodes are the enode URLs of the P2P bootstrap nodes running on the
// Ropsten test network.
var TestnetBootnodes = []string{
	"enode://cbbd20d50bda36ac0a52ca4d48441cb5b3438686ac0faa13f69818d7498a52e227ee4975fecafb4828458e423d1cfee4aadbd442a889213e16029e18063e7d25@10.0.196.190:30313",
	"enode://1b4fd85fe9e7de46e60e7893447d5fd7ce3f3c613c2c4b54bee2391a0a9b4de2a08f949fbbec01c6a4881f2ff37edd60b00426fad4208d925bcaafacbcc342d2@10.0.196.12:30313",
	"enode://8f5c4da356913c0210a3b9325a2e931ecb2ae6f9f898e813dd3c0da6dcb88ad0c5117c3c0ec40d73d214dca32e0e2a89844a51895f2c9b8632b558a71a57376c@10.0.196.26:30313",
	"enode://1cf7284d4933f670a9c53cc28b1d1e7da4edc693e9b77b9a4a4bafaa17a121ed8b4b61b91575a3abce1d9b11bbe3a9da8534c89ccefffc2d3936fda29da0df3c@10.0.196.216:30313",
}

// DevnetBootnodes are the enode URLs of the P2P bootstrap nodes running on
// the dev Abeychain network.
var DevnetBootnodes = []string{
	//"enode://e8e1af516589b5b49d0110bf52dcbc8c4fe592697cfd94eb01fdc302e2b4f1f9c7d3667ca4be64d094a15635204195681219dc59febe17c7c83f3b71b6258454@18.138.171.105:30313",
	//"enode://8b1af471dd393f2f228cefd663e3176963592adcd452e69b868ffb2cbb2f7b67ca44f60269e031952a00ba708799c8c1c2a41c279df80461e9b4d384ea49f9fc@18.140.45.222:30313",
	//"enode://1554bd2e50456dbf1e5b94aabf88fc7eb1f274d39ae52067541d46e2b8043cb102bffffecb0ba42657c8904aec6bde4f1728110107f27f50616c18ede4af983c@52.220.163.103:30313",
	//"enode://d3b5fb4283424e6011d6ad1bcad7e3890fc94db4e6d221571a61985b1f48b6ed26733b9871debb18924cb299600611b683f08e1be08e9a320ffba44494388d1f@54.151.132.19:30313",

	"enode://f3dc851caa883964c072a96c6bae9a21f5d185df6b8d8d6f52424f8b7b789150a59485655b227371bfe8dea15eed94542fa1238f5c07dc10c8905d7085f0ddb3@127.0.0.1:30501",
	"enode://ebdb3cd8b133e1b07f1367ac5493426359cebdf0d62fa342439a3e1a5fc4e828e42dd771fd83bedd36546d394162b151f29afd904f7ce9aa40435b1f2e9d1e9d@127.0.0.1:30601",
	"enode://6822116d47833585ddcac8a972e2ae2db742a350a4ee3e78166b82477ea6bbbccac01e9cf07b96cceda752a4f4a0fed2a293950541334a01d9ba5dded74688e1@127.0.0.1:30701",
	"enode://c725c3b63f65627e3049ca4917d5e6b3985b4ee2996cbf48b7f07a1aec03822207d2a79272d0526b27b039b2f418f2b4c5dd266bb8118268568d9cb5b6d29808@127.0.0.1:30801",
}

// DiscoveryV5Bootnodes are the enode URLs of the P2P bootstrap nodes for the
// experimental RLPx v5 topic-discovery network.
var DiscoveryV5Bootnodes = []string{
	"enode://ebb007b1efeea668d888157df36cf8fe49aa3f6fd63a0a67c45e4745dc081feea031f49de87fa8524ca29343a21a249d5f656e6daeda55cbe5800d973b75e061@39.98.171.41:30315",
	"enode://b5062c25dc78f8d2a8a216cebd23658f170a8f6595df16a63adfabbbc76b81b849569145a2629a65fe50bfd034e38821880f93697648991ba786021cb65fb2ec@39.98.43.179:30312",
}
