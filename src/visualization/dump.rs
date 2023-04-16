use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;
use crate::crypto::hash::H256;
use crate::utxodb::UtxoDatabase;

/*
#[derive(Serialize)]
pub struct Input {
    hash: String,
    index: u32,
}

#[derive(Serialize)]
pub struct Output {
    value: u64,
    recipient: String,
}

#[derive(Serialize)]
pub struct Transaction {
    hash: String,
    input: Vec<Input>,
    output: Vec<Output>,
}

#[derive(Serialize)]
pub struct TransactionBlock {
    /// Hash of this block
    pub hash: String,
    /// List of transactions
    pub transactions: Vec<Transaction>, //TODO: Add tx validity
    /// List of tx hashes and list output indices which are unspent
    pub utxos: Vec<Input>,
}

#[derive(Serialize)]
pub struct OriginatorBlock {
    /// Hash of this block
    pub hash: String,
    /// List of transaction blocks
    pub transaction_refs: Vec<TransactionBlock>,
}
*/

#[derive(Serialize)]
pub struct Dump {
    /// Ordered tx blocks
    pub originator: Vec<String>,
}

pub fn dump_ledger(
    blockchain: &BlockChain,
    _blockdb: &BlockDatabase,
    _utxodb: &UtxoDatabase,
    limit: u64,
) -> String {
    let ledger = match blockchain.originator_transaction_in_ledger(limit) {
        Err(_) => return "database err".to_string(),
        Ok(v) => v,
    };

    let mut originator_blocks: Vec<String> = vec![];
    // loop over all tx blocks in the ledger
    for (originator_hash, _tx_block_hashes) in &ledger {
        /*
        let mut transactions_blocks: Vec<TransactionBlock> = vec![];
        for tx_block_hash in tx_block_hashes {
            let mut transactions = vec![];
            let mut utxos = vec![];
            let transactions_in_block: Vec<RawTransaction> = match blockdb.get(tx_block_hash) {
                Err(_) => return "database err".to_string(),
                Ok(None) => return "transaction block not found".to_string(),
                Ok(Some(block)) => match block.content {
                    Content::Transaction(content) => content.transactions,
                    _ => return "wrong block type, not transaction block".to_string(),
                },
            };

            // loop over all the tx in this transaction block
            for tx in transactions_in_block {
                let hash: H256 = tx.hash();
                // loop over the outputs to check if they are unspent
                for index in 0..tx.output.len() {
                    let coin_id = CoinId {
                        hash,
                        index: index as u32,
                    };
                    if let Ok(true) = utxodb.contains(&coin_id) {
                        utxos.push(Input {
                            hash: hash.to_string(),
                            index: index as u32,
                        });
                    }
                }

                // add this transaction to the list
                transactions.push(Transaction {
                    hash: hash.to_string(),
                    input: tx
                        .input
                        .iter()
                        .map(|x| Input {
                            hash: x.coin.hash.to_string(),
                            index: x.coin.index,
                        })
                        .collect(),
                    output: tx
                        .output
                        .iter()
                        .map(|x| Output {
                            value: x.value,
                            recipient: x.recipient.to_string(),
                        })
                        .collect(),
                });
            }
            transactions_blocks.push(TransactionBlock {
                hash: tx_block_hash.to_string(),
                transactions,
                utxos,
            });
        }
        */
        originator_blocks.push(originator_hash.to_string());
    }
    let dump = Dump {
        originator: originator_blocks,
    };
    serde_json::to_string_pretty(&dump).unwrap()
}

pub fn dump_picker_timestamp(blockchain: &BlockChain, blockdb: &BlockDatabase) -> String {
    let originator_bottom_tip =
        blockchain
            .originator_bottom_tip()
            .unwrap_or((H256::default(), H256::default(), 0));
    let picker_bottom_tip = blockchain.picker_bottom_tip().unwrap_or(vec![]);
    let mut dump = vec![];
    let bottom_timestamp = match blockdb.get(&originator_bottom_tip.0).unwrap_or(None) {
        Some(block) => block.header.timestamp,
        _ => 0,
    };
    let tip_timestamp = match blockdb.get(&originator_bottom_tip.1).unwrap_or(None) {
        Some(block) => block.header.timestamp,
        _ => 0,
    };
    if originator_bottom_tip.2 > 1 && tip_timestamp != bottom_timestamp {
        dump.push(format!(
            "Originator tree, {:6.3} s / {:3} level = {:10.3}",
            (tip_timestamp - bottom_timestamp) as f64 / 1000f64,
            originator_bottom_tip.2 - 1,
            (tip_timestamp - bottom_timestamp) as f64
                / (originator_bottom_tip.2 - 1) as f64
                / 1000f64
        ));
    } else {
        dump.push("Originator tree only grows zero or one level.".to_string());
    }
    for (chain, (bottom, tip, level)) in picker_bottom_tip.iter().enumerate() {
        let bottom_timestamp = match blockdb.get(bottom).unwrap_or(None) {
            Some(block) => block.header.timestamp,
            _ => 0,
        };
        let tip_timestamp = match blockdb.get(tip).unwrap_or(None) {
            Some(block) => block.header.timestamp,
            _ => 0,
        };
        if *level > 1 && tip_timestamp != bottom_timestamp {
            dump.push(format!(
                "Chain {:7}, {:6.3} s / {:3} level = {:10.3}",
                chain,
                (tip_timestamp - bottom_timestamp) as f64 / 1000f64,
                *level - 1,
                (tip_timestamp - bottom_timestamp) as f64 / (*level - 1) as f64 / 1000f64
            ));
        } else {
            dump.push(format!("Chain {:7} only grows zero or one level.", chain));
        }
    }
    serde_json::to_string_pretty(&dump).unwrap()
}
