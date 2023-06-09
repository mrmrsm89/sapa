pub mod header;
pub mod originator;
pub mod transaction;
pub mod picker;
use crate::crypto::hash::{Hashable, H256};
use crate::experiment::performance_counter::PayloadSize;

/// A block in the Sapa blockchain.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    /// The header of the block.
    pub header: header::Header,
    /// The content of the block. It could contain transactions, references, or picks, depending on
    /// the block type.
    pub content: Content,
    /// The sortition proof of the content. In addition to the content Merkle root in block header, we are
    /// able to verify that the block is mined on a set of content candidates.
    pub sortition_proof: Vec<H256>,
}

impl Block {
    /// Create a new block.
    pub fn new(
        parent: H256,
        timestamp: u128,
        nonce: u32,
        content_merkle_root: H256,
        sortition_proof: Vec<H256>,
        content: Content,
        extra_content: [u8; 32],
        difficulty: H256,
    ) -> Self {
        let header = header::Header::new(
            parent,
            timestamp,
            nonce,
            content_merkle_root,
            extra_content,
            difficulty,
        );
        Self {
            header,
            content,
            sortition_proof,
        }
    }

    // TODO: use another name
    /// Create a new block from header.
    pub fn from_header(
        header: header::Header,
        content: Content,
        sortition_proof: Vec<H256>,
    ) -> Self {
        Self {
            header,
            content,
            sortition_proof,
        }
    }
}

impl Hashable for Block {
    fn hash(&self) -> H256 {
        // TODO: we are only hashing the header here.
        self.header.hash()
    }
}

impl PayloadSize for Block {
    fn size(&self) -> usize {
        std::mem::size_of::<header::Header>()
            + self.content.size()
            + self.sortition_proof.len() * std::mem::size_of::<H256>()
    }
}

/// The content of a block. It could be transaction content, originator content, or picker content,
/// depending on the type of the block.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Content {
    /// Transaction block content.
    Transaction(transaction::Content),
    /// Originator block content.
    Originator(originator::Content),
    /// Picker block content.
    Picker(picker::Content),
}

impl Hashable for Content {
    fn hash(&self) -> H256 {
        match self {
            Content::Transaction(c) => c.hash(),
            Content::Originator(c) => c.hash(),
            Content::Picker(c) => c.hash(),
        }
    }
}

impl PayloadSize for Content {
    fn size(&self) -> usize {
        // TODO: we are not counting the 2 bits that are used to store block type
        match self {
            Content::Transaction(c) => c.size(),
            Content::Originator(c) => c.size(),
            Content::Picker(c) => c.size(),
        }
    }
}

#[cfg(any(test, feature = "test-utilities"))]
pub mod tests {

    use super::*;
    use crate::config;
    use crate::transaction::Transaction;
    use rand::Rng;

    macro_rules! random_nonce {
        () => {{
            let mut rng = rand::thread_rng();
            let random_u32: u32 = rng.gen();
            random_u32
        }};
    }

    pub fn originator_block(
        parent: H256,
        timestamp: u128,
        originator_refs: Vec<H256>,
        transaction_refs: Vec<H256>,
    ) -> Block {
        let content = Content::Originator(originator::Content {
            transaction_refs,
            originator_refs,
        });
        let content_hash = content.hash();
        Block::new(
            parent,
            timestamp,
            random_nonce!(),
            content_hash,
            vec![content_hash],
            content,
            [0u8; 32],
            *config::DEFAULT_DIFFICULTY,
        )
    }

    pub fn picker_block(
        parent: H256,
        timestamp: u128,
        chain_number: u16,
        picker_parent: H256,
        picks: Vec<H256>,
    ) -> Block {
        let content = Content::Picker(picker::Content {
            chain_number,
            picker_parent,
            picks,
        });
        let content_hash = content.hash();
        Block::new(
            parent,
            timestamp,
            random_nonce!(),
            content_hash,
            vec![content_hash],
            content,
            [0u8; 32],
            *config::DEFAULT_DIFFICULTY,
        )
    }

    pub fn transaction_block(
        parent: H256,
        timestamp: u128,
        transactions: Vec<Transaction>,
    ) -> Block {
        let content = Content::Transaction(transaction::Content { transactions });
        let content_hash = content.hash();
        Block::new(
            parent,
            timestamp,
            random_nonce!(),
            content_hash,
            vec![content_hash],
            content,
            [0u8; 32],
            *config::DEFAULT_DIFFICULTY,
        )
    }
}
