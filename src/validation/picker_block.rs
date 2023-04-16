use super::{check_originator_block_exists, check_picker_block_exists};
use crate::block::picker::Content;

use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;

use crate::crypto::hash::H256;

pub fn get_missing_references(
    content: &Content,
    blockchain: &BlockChain,
    _blockdb: &BlockDatabase,
) -> Vec<H256> {
    let mut missing_blocks = vec![];

    // check the picker parent
    let picker_parent = check_picker_block_exists(content.picker_parent, blockchain);
    if !picker_parent {
        missing_blocks.push(content.picker_parent);
    }

    // check the picks
    for prop_hash in content.picks.iter() {
        let avail = check_originator_block_exists(*prop_hash, blockchain);
        if !avail {
            missing_blocks.push(*prop_hash);
        }
    }

    missing_blocks
}

pub fn check_chain_number(content: &Content, blockchain: &BlockChain) -> bool {
    let chain_num = blockchain
        .picker_chain_number(&content.picker_parent)
        .unwrap();
    chain_num == content.chain_number
}

pub fn check_levels_picked(content: &Content, blockchain: &BlockChain, parent: &H256) -> bool {
    let mut start = blockchain
        .deepest_picked_level(&content.picker_parent)
        .unwrap(); //need to be +1
    let end = blockchain.originator_level(parent).unwrap();

    if start > end {
        return false;
    } //end < start means incorrect parent level
    if content.picks.len() != (end - start) as usize {
        return false;
    } //
    for pick in content.picks.iter() {
        start += 1;
        if start != blockchain.originator_level(pick).unwrap() {
            return false;
        }
    }
    true
}
