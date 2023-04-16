use super::Block;
use super::Content as BlockContent;
use crate::config::*;
use crate::crypto::hash::{Hashable, H256};
use crate::crypto::merkle::MerkleTree;
use crate::experiment::performance_counter::PayloadSize;

/// The content of a picker block.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Content {
    /// ID of the picker chain this block is attaching to.
    pub chain_number: u16,
    /// Hash of the parent picker block.
    pub picker_parent: H256,
    /// List of picks on originator blocks.
    pub picks: Vec<H256>,
}

impl Content {
    /// Create new picker block content.
    pub fn new(chain_number: u16, picker_parent: H256, picks: Vec<H256>) -> Self {
        Self {
            chain_number,
            picker_parent,
            picks,
        }
    }
}

impl PayloadSize for Content {
    fn size(&self) -> usize {
        std::mem::size_of::<u16>()
            + std::mem::size_of::<H256>()
            + self.picks.len() * std::mem::size_of::<H256>()
    }
}

impl Hashable for Content {
    fn hash(&self) -> H256 {
        // TODO: we are hashing in a merkle tree. why do we need so?
        let merkle_tree = MerkleTree::new(&self.picks);
        let mut bytes = [0u8; 66];
        bytes[..2].copy_from_slice(&self.chain_number.to_be_bytes());
        bytes[2..34].copy_from_slice(self.picker_parent.as_ref());
        bytes[34..66].copy_from_slice(merkle_tree.root().as_ref());
        ring::digest::digest(&ring::digest::SHA256, &bytes).into()
    }
}

/// Generate the genesis block of the picker chain with the given chain ID.
pub fn genesis(chain_num: u16) -> Block {
    let all_zero: [u8; 32] = [0; 32];
    let content = Content {
        chain_number: chain_num,
        picker_parent: all_zero.into(),
        picks: vec![],
    };
    // TODO: this block will definitely not pass validation. We depend on the fact that genesis
    // blocks are added to the system at initialization. Seems like a moderate hack.
    Block::new(
        all_zero.into(),
        0,
        0,
        all_zero.into(),
        vec![],
        BlockContent::Picker(content),
        all_zero,
        *DEFAULT_DIFFICULTY,
    )
}

#[cfg(test)]
pub mod test {}
