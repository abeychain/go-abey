
## ABEY Chain


Click to visit [ABEY developer documents](https://docs.abeychain.com)

ABEY chain is a truly fast, permissionless, secure and scalable public blockchain platform 
which is supported by hybrid consensus technology called Minerva and a global developer community. 
 
ABEY chain uses hybrid consensus combining PBFT and fPoW to solve the biggest problem confronting public blockchain: 
the contradiction between decentralization and efficiency. 

ABEY chain uses PBFT as fast-chain to process transactions, and leave the oversight and election of PBFT to the hands of PoW nodes. 
Besides, Abeychain integrates fruitchain technology into the traditional PoW protocol to become fPoW, 
to make the chain even more decentralized and fair. 
 
ABEY chain also creates a hybrid consensus incentive model and a stable gas fee mechanism to lower the cost for the developers 
and operators of DApps, and provide better infrastructure for decentralized eco-system. 

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
