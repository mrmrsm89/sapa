use super::Block;
use super::Content as BlockContent;
use crate::config::*;
use crate::crypto::hash::{Hashable, H256};
use crate::crypto::merkle::MerkleTree;
use crate::experiment::performance_counter::PayloadSize;

/// The content of a originator block.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Content {
    /// List of transaction blocks referred by this originator block.
    pub transaction_refs: Vec<H256>,
    /// List of originator blocks referred by this originator block.
    pub originator_refs: Vec<H256>,
    // TODO: coinbase transaction, and maybe refer to picker blocks to include their coinbase
    // transactions.
}

impl Content {
    /// Create new originator block content.
    pub fn new(transaction_refs: Vec<H256>, originator_refs: Vec<H256>) -> Self {
        Self {
            transaction_refs,
            originator_refs,
        }
    }
}

impl PayloadSize for Content {
    fn size(&self) -> usize {
        std::mem::size_of::<H256>() * (self.transaction_refs.len() + self.originator_refs.len())
    }
}

impl Hashable for Content {
    fn hash(&self) -> H256 {
        let tx_merkle_tree = MerkleTree::new(&self.transaction_refs);
        let prop_merkle_tree = MerkleTree::new(&self.originator_refs);
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(tx_merkle_tree.root().as_ref());
        bytes[32..64].copy_from_slice(prop_merkle_tree.root().as_ref());
        ring::digest::digest(&ring::digest::SHA256, &bytes).into()
    }
}

/// Generate the genesis block of the originator chain.
pub fn genesis() -> Block {
    let content = Content {
        transaction_refs: vec![],
        originator_refs: vec![],
    };
    let all_zero: [u8; 32] = [0; 32];
    // TODO: this will not pass validation.
    Block::new(
        all_zero.into(),
        0,
        0,
        all_zero.into(),
        vec![],
        BlockContent::Originator(content),
        all_zero,
        *DEFAULT_DIFFICULTY,
    )
}

#[cfg(test)]
pub mod tests {}
