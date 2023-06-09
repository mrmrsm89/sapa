use super::{check_originator_block_exists, check_transaction_block_exists};
use crate::block::originator::Content;
use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;
use crate::crypto::hash::H256;

pub fn get_missing_references(
    content: &Content,
    blockchain: &BlockChain,
    _blockdb: &BlockDatabase,
) -> Vec<H256> {
    let mut missing_blocks: Vec<H256> = vec![];

    // check whether the tx block referred are present
    for tx_block_hash in content.transaction_refs.iter() {
        let tx_block = check_transaction_block_exists(*tx_block_hash, blockchain);
        if !tx_block {
            missing_blocks.push(*tx_block_hash);
        }
    }

    // check whether the originator blocks referred are present
    for prop_block_hash in content.originator_refs.iter() {
        let prop_block = check_originator_block_exists(*prop_block_hash, blockchain);
        if !prop_block {
            missing_blocks.push(*prop_block_hash);
        }
    }

    missing_blocks
}

pub fn check_ref_originator_level(parent: &H256, content: &Content, blockchain: &BlockChain) -> bool {
    let parent_level = blockchain.originator_level(parent).unwrap();
    for prop_block_hash in content.originator_refs.iter() {
        let l = blockchain.originator_level(prop_block_hash).unwrap();
        if l > parent_level {
            return false;
        }
    }
    true
}
