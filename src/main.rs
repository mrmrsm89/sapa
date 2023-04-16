#[macro_use]
extern crate clap;

use crossbeam::channel;
use ed25519_dalek::Keypair;
use log::{debug, error, info};
use piper;
use sapa::api::Server as ApiServer;
use sapa::blockchain::BlockChain;
use sapa::blockdb::BlockDatabase;
use sapa::config::BlockchainConfig;
use sapa::crypto::hash::H256;
use sapa::experiment::transaction_generator::TransactionGenerator;
use sapa::ledger_manager::LedgerManager;
use sapa::miner;
use sapa::miner::memory_pool::MemoryPool;
use sapa::network::server;
use sapa::network::worker;
use sapa::transaction::Address;
use sapa::utxodb::UtxoDatabase;
use sapa::visualization::Server as VisualizationServer;
use sapa::wallet::Wallet;
use rand::rngs::OsRng;
use std::convert::TryInto;
use std::net;
use std::process;
use std::sync::Arc;
use std::thread;
use std::time;

fn main() {
    // parse command line arguments
    let matches = clap_app!(Sapa =>
     (version: "0.1")
     (about: "Sapa blockchain full client")
     (@arg verbose: -v ... "Increases the verbosity of logging")
     (@arg peer_addr: --p2p [ADDR] default_value("127.0.0.1:6000") "Sets the IP address and the port of the P2P server")
     (@arg api_addr: --api [ADDR] default_value("127.0.0.1:7000") "Sets the IP address and the port of the API server")
     (@arg visualization: --visual [ADDR] "Enables the visualization server and sets its address and port")
     (@arg known_peer: -c --connect ... [PEER] "Sets the peers to connect to at start")
     (@arg block_db: --blockdb [PATH] default_value("/tmp/sapa-blocks.rocksdb") "Sets the path to the block database")
     (@arg utxo_db: --utxodb [PATH] default_value("/tmp/sapa-utxo.rocksdb") "Sets the path to the UTXO database")
     (@arg blockchain_db: --blockchaindb [PATH] default_value("/tmp/sapa-blockchain.rocksdb") "Sets the path to the blockchain database")
     (@arg wallet_db: --walletdb [PATH] default_value("/tmp/sapa-wallet.rocksdb") "Sets the path to the wallet database")
     (@arg init_fund_addr: --("fund-addr") ... [ADDR] "Endows the given address an initial fund in the genesis block")
     (@arg init_fund_coins: --("fund-coins") [INT] default_value("50000") "Sets the number of initial coins for each address")
     (@arg init_fund_value: --("fund-value") [INT] default_value("100") "Sets the value of each initial coin")
     (@arg load_key_path: --("load-key") ... [PATH] "Loads a key pair into the wallet from the given path")
     (@arg mempool_size: --("mempool-size") [INT] default_value("500000") "Sets the maximum number of transactions for the memory pool")
     (@arg execution_workers: --("execution-workers") [INT] default_value("8") "Sets the number of worker threads for transaction execution")
     (@arg execution_buffer: --("execution-buffer") [INT] default_value("3") "Sets the size of the buffer between pipeline stages in transaction execution")
     (@arg p2p_workers: --("p2p-workers") [INT] default_value("16") "Sets the number of worker threads for P2P server")
     (@arg picker_chains: --("picker-chains") [INT] default_value("1000") "Sets the number of picker chains")
     (@arg tx_throughput: --("tx-throughput") [INT] default_value("80000") "Sets the target transaction throughput")
     (@arg tx_block_size: --("tx-block-size") [INT] default_value("64000") "Sets the maximum size of the transaction block in Bytes")
     (@arg originator_mining_rate: --("originator-mining-rate") [FLOAT] default_value("0.1") "Sets the originator chain mining rate")
     (@arg picker_mining_rate: --("picker-mining-rate") [FLOAT] default_value("0.1") "Sets the picker chain mining rate")
     (@arg adv_ratio: --("adversary-ratio") [FLOAT] default_value("0.4") "Sets the ratio of adversary hashing power")
     (@arg log_epsilon: --("confirm-confidence") [FLOAT] default_value("20.0") "Sets -log(epsilon) for confirmation")

     (@subcommand keygen =>
      (about: "Generates Sapa wallet key pair")
      (@arg display_address: --addr "Prints the address of the key pair to STDERR")
     )
    )
    .get_matches();

    // match subcommands
    match matches.subcommand() {
        ("keygen", Some(m)) => {
            let mut csprng = OsRng;
            let keypair: Keypair = Keypair::generate(&mut csprng);
            let base64_encoded = base64::encode(&keypair.to_bytes().to_vec());
            println!("{}", base64_encoded);
            if m.is_present("display_address") {
                let addr: Address = ring::digest::digest(
                    &ring::digest::SHA256,
                    &keypair.public.as_bytes().as_ref(),
                )
                .into();
                let base64_encoded = base64::encode(&addr);
                eprintln!("{}", base64_encoded);
            }
            return;
        }
        _ => {}
    }

    // init logger
    let verbosity = matches.occurrences_of("verbose") as usize;
    stderrlog::new().verbosity(verbosity).init().unwrap();

    // init config struct
    let picker_chains: u16 = matches
        .value_of("picker_chains")
        .unwrap()
        .parse()
        .unwrap_or_else(|e| {
            error!("Error parsing picker chain number: {}", e);
            process::exit(1);
        });
    let tx_throughput = matches
        .value_of("tx_throughput")
        .unwrap()
        .parse::<u32>()
        .unwrap_or_else(|e| {
            error!("Error parsing target transaction throughput: {}", e);
            process::exit(1);
        });
    let tx_blk_size = matches
        .value_of("tx_block_size")
        .unwrap()
        .parse::<u32>()
        .unwrap_or_else(|e| {
            error!("Error parsing transaction block size: {}", e);
            process::exit(1);
        });
    let originator_mining_rate = matches
        .value_of("originator_mining_rate")
        .unwrap()
        .parse::<f32>()
        .unwrap_or_else(|e| {
            error!("Error parsing originator chain mining rate: {}", e);
            process::exit(1);
        });
    let picker_mining_rate = matches
        .value_of("picker_mining_rate")
        .unwrap()
        .parse::<f32>()
        .unwrap_or_else(|e| {
            error!("Error parsing picker chain mining rate: {}", e);
            process::exit(1);
        });
    let adv_ratio = matches
        .value_of("adv_ratio")
        .unwrap()
        .parse::<f32>()
        .unwrap_or_else(|e| {
            error!("Error parsing adversary power ratio: {}", e);
            process::exit(1);
        });
    let log_epsilon = matches
        .value_of("log_epsilon")
        .unwrap()
        .parse::<f32>()
        .unwrap_or_else(|e| {
            error!("Error parsing confirm confidence: {}", e);
            process::exit(1);
        });
    let config = BlockchainConfig::new(
        picker_chains,
        tx_blk_size,
        tx_throughput,
        originator_mining_rate,
        picker_mining_rate,
        adv_ratio,
        log_epsilon,
    );
    info!(
        "Originator block mining rate set to {} blks/s",
        config.originator_mining_rate
    );
    info!(
        "Picker block mining rate set to {} blks/s per chain, {} chains",
        config.originator_mining_rate, config.picker_chains
    );
    info!(
        "Transaction block mining rate set to {} blks/s",
        config.tx_mining_rate
    );

    // init mempool
    let mempool_size = matches
        .value_of("mempool_size")
        .unwrap()
        .parse::<u64>()
        .unwrap_or_else(|e| {
            error!("Error parsing memory pool size limit: {}", e);
            process::exit(1);
        });
    let mempool = MemoryPool::new(mempool_size);
    let mempool = Arc::new(std::sync::Mutex::new(mempool));
    debug!("Initialized mempool, maximum size set to {}", mempool_size);

    // init block database
    let blockdb =
        BlockDatabase::new(&matches.value_of("block_db").unwrap(), config.clone()).unwrap();
    let blockdb = Arc::new(blockdb);
    debug!("Initialized block database");

    // init utxo database
    let utxodb = UtxoDatabase::new(&matches.value_of("utxo_db").unwrap()).unwrap();
    let utxodb = Arc::new(utxodb);
    debug!("Initialized UTXO database");

    // init blockchain database
    let blockchain =
        BlockChain::new(&matches.value_of("blockchain_db").unwrap(), config.clone()).unwrap();
    let blockchain = Arc::new(blockchain);
    debug!("Initialized blockchain database");

    // init wallet database
    let wallet = Wallet::new(&matches.value_of("wallet_db").unwrap()).unwrap();
    let wallet = Arc::new(wallet);
    debug!("Initialized wallet");

    // load wallet keys
    if let Some(wallet_keys) = matches.values_of("load_key_path") {
        for key_path in wallet_keys {
            let content = match std::fs::read_to_string(&key_path) {
                Ok(c) => c,
                Err(e) => {
                    error!("Error loading key pair at {}: {}", &key_path, &e);
                    process::exit(1);
                }
            };
            let decoded = match base64::decode(&content.trim()) {
                Ok(d) => d,
                Err(e) => {
                    error!("Error decoding key pair at {}: {}", &key_path, &e);
                    process::exit(1);
                }
            };
            let keypair = Keypair::from_bytes(&decoded).unwrap();
            match wallet.load_keypair(keypair) {
                Ok(a) => info!("Loaded key pair for address {}", &a),
                Err(e) => {
                    error!("Error loading key pair into wallet: {}", &e);
                    process::exit(1);
                }
            }
        }
    }

    // start thread to update ledger
    let tx_workers = matches
        .value_of("execution_workers")
        .unwrap()
        .parse::<usize>()
        .unwrap_or_else(|e| {
            error!("Error parsing transaction execution workers: {}", e);
            process::exit(1);
        });
    let tx_buffer = matches
        .value_of("execution_buffer")
        .unwrap()
        .parse::<usize>()
        .unwrap_or_else(|e| {
            error!("Error parsing transaction execution buffer size: {}", e);
            process::exit(1);
        });
    let ledger_manager = LedgerManager::new(&blockdb, &blockchain, &utxodb, &wallet);
    ledger_manager.start(tx_buffer, tx_workers);
    debug!(
        "Initialized ledger manager with buffer size {} and {} workers",
        tx_buffer, tx_workers
    );

    // parse p2p server address
    let p2p_addr = matches
        .value_of("peer_addr")
        .unwrap()
        .parse::<net::SocketAddr>()
        .unwrap_or_else(|e| {
            error!("Error parsing P2P server address: {}", e);
            process::exit(1);
        });

    // parse api server address
    let api_addr = matches
        .value_of("api_addr")
        .unwrap()
        .parse::<net::SocketAddr>()
        .unwrap_or_else(|e| {
            error!("Error parsing API server address: {}", e);
            process::exit(1);
        });

    // create channels between server and worker, worker and miner, miner and worker
    let (msg_tx, msg_rx) = piper::chan(100); // TODO: make this buffer adjustable
    let (ctx_tx, ctx_rx) = channel::unbounded();
    let ctx_tx_miner = ctx_tx.clone();

    // start the p2p server
    let (server_ctx, server) = server::new(p2p_addr, msg_tx).unwrap();
    server_ctx.start().unwrap();

    // start the worker
    let p2p_workers = matches
        .value_of("p2p_workers")
        .unwrap()
        .parse::<usize>()
        .unwrap_or_else(|e| {
            error!("Error parsing P2P workers: {}", e);
            process::exit(1);
        });
    let worker_ctx = worker::new(
        p2p_workers,
        msg_rx,
        &blockchain,
        &blockdb,
        &utxodb,
        &wallet,
        &mempool,
        ctx_tx,
        &server,
        config.clone(),
    );
    worker_ctx.start();

    // start the miner
    let (miner_ctx, miner) = miner::new(
        &mempool,
        &blockchain,
        &blockdb,
        ctx_rx,
        &ctx_tx_miner,
        &server,
        config.clone(),
    );
    miner_ctx.start();

    // connect to known peers
    if let Some(known_peers) = matches.values_of("known_peer") {
        let known_peers: Vec<String> = known_peers.map(|x| x.to_owned()).collect();
        let server = server.clone();
        thread::spawn(move || {
            for peer in known_peers {
                loop {
                    let addr = match peer.parse::<net::SocketAddr>() {
                        Ok(x) => x,
                        Err(e) => {
                            error!("Error parsing peer address {}: {}", &peer, e);
                            break;
                        }
                    };
                    match server.connect(addr) {
                        Ok(_) => {
                            info!("Connected to outgoing peer {}", &addr);
                            break;
                        }
                        Err(e) => {
                            error!(
                                "Error connecting to peer {}, retrying in one second: {}",
                                addr, e
                            );
                            thread::sleep(time::Duration::from_millis(1000));
                            continue;
                        }
                    }
                }
            }
        });
    }

    // fund the given addresses
    if let Some(fund_addrs) = matches.values_of("init_fund_addr") {
        let num_coins = matches
            .value_of("init_fund_coins")
            .unwrap()
            .parse::<usize>()
            .unwrap_or_else(|e| {
                error!("Error parsing number of initial fund coins: {}", e);
                process::exit(1);
            });
        let coin_value = matches
            .value_of("init_fund_value")
            .unwrap()
            .parse::<u64>()
            .unwrap_or_else(|e| {
                error!("Error parsing value of initial fund coins: {}", e);
                process::exit(1);
            });
        let mut addrs = vec![];
        for addr in fund_addrs {
            let decoded = match base64::decode(&addr.trim()) {
                Ok(d) => d,
                Err(e) => {
                    error!("Error decoding address {}: {}", &addr.trim(), e);
                    process::exit(1);
                }
            };
            let addr_bytes: [u8; 32] = (&decoded[0..32]).try_into().unwrap();
            let hash: H256 = addr_bytes.into();
            addrs.push(hash);
        }
        info!(
            "Funding {} addresses with {} initial coins of {}",
            addrs.len(),
            num_coins,
            coin_value
        );
        sapa::experiment::ico(&addrs, &utxodb, &wallet, num_coins, coin_value).unwrap();
    }

    // create wallet key pair if there is none
    if wallet.addresses().unwrap().is_empty() {
        wallet.generate_keypair().unwrap();
    }

    // start the transaction generator
    let (txgen_ctx, txgen_control_chan) = TransactionGenerator::new(&wallet, &server, &mempool);
    txgen_ctx.start();

    // start the API server
    ApiServer::start(
        api_addr,
        &wallet,
        &blockchain,
        &utxodb,
        &server,
        &miner,
        &mempool,
        txgen_control_chan,
    );

    // start the visualization server
    if let Some(addr) = matches.value_of("visualization") {
        let addr = addr.parse::<net::SocketAddr>().unwrap_or_else(|e| {
            error!("Error parsing visualization server socket address: {}", e);
            process::exit(1);
        });
        info!("Starting visualization server at {}", &addr);
        VisualizationServer::start(addr, &blockchain, &blockdb, &utxodb);
    }

    loop {
        std::thread::park();
    }
}
