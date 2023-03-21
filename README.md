
## ABEY Chain


Processing up to 3,000 TPS while featuring ultra-low gas fees, ABEYCHAIN 3.0 is a multi-Layered programmable blockchain with a DPoS consensus specializing in building decentralized applications, business use cases, and achieving cross-chain Interoperability.  It is based on parallel transaction execution and the ABEY Virtual Machine (AVM). 

Why ABEY:

ABEYCHAIN is the first fully operational, third-generation public chain to solve one of the most pressing challenges in the public chain space: the ability to achieve simultaneously a high degree of both decentralization, security, and efficiency, which is commonly known as the “Impossible Triangle.”

High-Speed Transactions:

ABEYCHAIN 3.0 features a Delegated Proof-of-Stake consensus in order to boost throughput to a whole new level. ABEYCHAIN’s DPoS committee is responsible for transaction validation.  Committee members are elected by ABEY token holders. Membership will be on a rotating basis in order to prevent corruption in a timely manner.

The dPoS consensus is currently capable of processing up to 3,000 transactions per second (TPS) with the ability to scale to even greater processing power with an eventual upgrade to “sharding”. Sharding allows the ABEY blockchain to be broken up into smaller pieces called “shards.” These shards act as semi-autonomous fragments of the main blockchain and can process transactions on their own. With sharding, ABEYCHAIN’s TPS is estimated to increase to 100,000. Further, unrelated transactions are splitted and executed them in parallel to greatly optimize execution efficiency and throughput.

Cross-Chain Interoperability:

Although many different blockchains exist in 2021, very few can achieve interoperability with other chains like ABEYCHAIN. ABEYCHAIN was created with maximum interoperability in mind – this means that a variety of high-quality crypto assets native to other blockchains can be seamlessly transferred onto or processed through the ABEYCHAIN without experiencing any significant delays.

Current state:

ABEYCHAIN 3.0 is quickly becoming the go-to smart contract platform for Metaverse, GameFi & DeFi dApp developers around the globe.


Click to visit [ABEY developer documents](https://docs.abeychain.com)


<a href="https://github.com/abeychain/go-abey/blob/master/COPYING"><img src="https://img.shields.io/badge/license-GPL%20%20Abeychain-lightgrey.svg"></a>



## Building the source


Building gabey requires both a Go (version 1.14 or later) and a C compiler.
You can install them using your favourite package manager.
Once the dependencies are installed, run

    make gabey

or, to build the full suite of utilities:

    make all

The execuable command gabey will be found in the `cmd` directory.

## Running gabey

Going through all the possible command line flags is out of scope here (please consult our
[CLI Wiki page](https://github.com/abeychain/go-abey/wiki/Command-Line-Options)), 
also you can quickly run your own gabey instance with a few common parameter combos.

### Running on the AbeyChain main network

```
$ gabey console
```

This command will:

 * Start gabey with network ID `19330` in full node mode(default, can be changed with the `--syncmode` flag after version 1.1).
 * Start up Gabey's built-in interactive console,
   (via the trailing `console` subcommand) through which you can invoke all official [`web3` methods](https://github.com/abeychain/go-abey/wiki/RPC-API)
   as well as Geth's own [management APIs](https://github.com/abeychain/go-abey/wiki/Management-API).
   This too is optional and if you leave it out you can always attach to an already running Gabey instance
   with `gabey attach`.


### Running on the ABEY Chain test network

To test your contracts, you can join the test network with your node.

```
$ gabey --testnet console
```

The `console` subcommand has the exact same meaning as above and they are equally useful on the
testnet too. Please see above for their explanations if you've skipped here.

Specifying the `--testnet` flag, however, will reconfigure your Geth instance a bit:

 * Test network uses different network ID `18928`
 * Instead of connecting the main ABEY chain network, the client will connect to the test network, which uses testnet P2P bootnodes,  and genesis states.


### Configuration

As an alternative to passing the numerous flags to the `gabey` binary, you can also pass a configuration file via:

```
$ gabey --config /path/to/your_config.toml
```

To get an idea how the file should look like you can use the `dumpconfig` subcommand to export your existing configuration:

```
$ gabey --your-favourite-flags dumpconfig
```


### Running on the ABEY Chain singlenode(private) network

To start a g
instance for single node,  run it with these flags:

```
$ gabey --singlenode  console
```

Specifying the `--singlenode` flag, however, will reconfigure your Geth instance a bit:

 * singlenode network uses different network ID `400`
 * Instead of connecting the main or test Abeychain network, the client has no peers, and generate fast block without committee.

Which will start sending transactions periodly to this node and mining fruits and snail blocks.
