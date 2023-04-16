use crate::crypto::hash::H256;
use bigint::uint::U256;

const AVG_TX_SIZE: u32 = 168; // average size of a transaction (in Bytes)
const ORIGINATOR_TX_REF_HEADROOM: f32 = 10.0;
const SORTITION_PRECISION: u64 = std::u64::MAX;
const DECONFIRM_HEADROOM: f32 = 1.05;

// Chain IDs
pub const ORIGINATOR_INDEX: u16 = 0;
pub const TRANSACTION_INDEX: u16 = 1;
pub const FIRST_PICKER_INDEX: u16 = 2;

#[derive(Clone)]
pub struct BlockchainConfig {
    /// Number of picker chains.
    pub picker_chains: u16,
    /// Maximum size of a transaction block in terms of transactions.
    pub tx_txs: u32,
    /// Maximum number of transaction block references in a originator block.
    pub originator_tx_refs: u32,
    /// Originator block minng rate in blocks/sec.
    pub originator_mining_rate: f32,
    /// Picker block minng rate for one picker chain, in blocks/sec.
    pub picker_mining_rate: f32,
    /// Transaction block minng rate in blocks/sec.
    pub tx_mining_rate: f32,
    /// Hash of originator genesis block.
    pub originator_genesis: H256,
    /// Hashes of picker genesis blocks.
    pub picker_genesis: Vec<H256>,
    total_mining_rate: f32,
    total_sortition_width: U256,
    originator_sortition_width: U256,
    picker_sortition_width: U256,
    tx_sortition_width: U256,
    pub adversary_ratio: f32,
    log_epsilon: f32,
    pub quantile_epsilon_confirm: f32,
    pub quantile_epsilon_deconfirm: f32,
}

impl BlockchainConfig {
    pub fn new(
        picker_chains: u16,
        tx_size: u32,
        tx_throughput: u32,
        originator_rate: f32,
        picker_rate: f32,
        adv_ratio: f32,
        log_epsilon: f32,
    ) -> Self {
        let tx_txs = tx_size / AVG_TX_SIZE;
        let originator_genesis: H256 = {
            let mut raw_hash: [u8; 32] = [0; 32];
            let bytes = ORIGINATOR_INDEX.to_be_bytes();
            raw_hash[30] = bytes[0];
            raw_hash[31] = bytes[1];
            raw_hash.into()
        };
        let picker_genesis_hashes: Vec<H256> = {
            let mut v: Vec<H256> = vec![];
            for chain_num in 0..picker_chains {
                let mut raw_hash: [u8; 32] = [0; 32];
                let bytes = (chain_num + FIRST_PICKER_INDEX).to_be_bytes();
                raw_hash[30] = bytes[0];
                raw_hash[31] = bytes[1];
                v.push(raw_hash.into());
            }
            v
        };
        let tx_mining_rate: f32 = {
            let tx_thruput: f32 = tx_throughput as f32;
            let tx_txs: f32 = tx_txs as f32;
            tx_thruput / tx_txs
        };
        let total_mining_rate: f32 =
            originator_rate + picker_rate * f32::from(picker_chains) + tx_mining_rate;
        let originator_width: u64 = {
            let precise: f32 = (originator_rate / total_mining_rate) * SORTITION_PRECISION as f32;
            precise.ceil() as u64
        };
        let picker_width: u64 = {
            let precise: f32 = (picker_rate / total_mining_rate) * SORTITION_PRECISION as f32;
            precise.ceil() as u64
        };
        let tx_width: u64 =
            SORTITION_PRECISION - originator_width - picker_width * u64::from(picker_chains);
        let log_epsilon_confirm = log_epsilon * DECONFIRM_HEADROOM;
        let quantile_confirm: f32 = (2.0 * log_epsilon_confirm
            - (2.0 * log_epsilon_confirm).ln()
            - (2.0 * 3.141_692_6 as f32).ln())
        .sqrt();
        let quantile_deconfirm: f32 =
            (2.0 * log_epsilon - (2.0 * log_epsilon).ln() - (2.0 * 3.141_692_6 as f32).ln()).sqrt();
        Self {
            picker_chains,
            tx_txs,
            originator_tx_refs: (tx_mining_rate / originator_rate * ORIGINATOR_TX_REF_HEADROOM).ceil()
                as u32,
            originator_mining_rate: originator_rate,
            picker_mining_rate: picker_rate,
            tx_mining_rate,
            originator_genesis,
            picker_genesis: picker_genesis_hashes,
            total_mining_rate,
            total_sortition_width: SORTITION_PRECISION.into(),
            originator_sortition_width: originator_width.into(),
            picker_sortition_width: picker_width.into(),
            tx_sortition_width: tx_width.into(),
            adversary_ratio: adv_ratio,
            log_epsilon,
            quantile_epsilon_confirm: quantile_confirm,
            quantile_epsilon_deconfirm: quantile_deconfirm,
        }
    }

    pub fn sortition_hash(&self, hash: &H256, difficulty: &H256) -> Option<u16> {
        let hash = U256::from_big_endian(hash.as_ref());
        let difficulty = U256::from_big_endian(difficulty.as_ref());
        let multiplier = difficulty / self.total_sortition_width;

        let originator_width = multiplier * self.originator_sortition_width;
        let transaction_width =
            multiplier * (self.originator_sortition_width + self.tx_sortition_width);
        if hash < originator_width {
            Some(ORIGINATOR_INDEX)
        } else if hash < (transaction_width + originator_width) {
            Some(TRANSACTION_INDEX)
        } else if hash < difficulty {
            let picker_idx = (hash - originator_width - transaction_width) % self.picker_chains.into();
            Some(picker_idx.as_u32() as u16 + FIRST_PICKER_INDEX)
        } else {
            None
        }
    }
}

lazy_static! {
    pub static ref DEFAULT_DIFFICULTY: H256 = {
        let raw: [u8; 32] = [255; 32];
        raw.into()
    };
}
