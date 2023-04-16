use crate::block::{Block, Content};
use crate::config::*;
use crate::crypto::hash::{Hashable, H256};

use crate::experiment::performance_counter::PERFORMANCE_COUNTER;
use bincode::{deserialize, serialize};
use log::{debug, info, warn};
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, DB};
use statrs::distribution::{Discrete, Poisson, Univariate};

use std::collections::{BTreeMap, HashMap, HashSet};

use std::ops::Range;
use std::sync::Mutex;

// Column family names for node/chain metadata
const ORIGINATOR_NODE_LEVEL_CF: &str = "ORIGINATOR_NODE_LEVEL"; // hash to node level (u64)
const PICKER_NODE_LEVEL_CF: &str = "PICKER_NODE_LEVEL"; // hash to node level (u64)
const PICKER_NODE_CHAIN_CF: &str = "PICKER_NODE_CHAIN"; // hash to chain number (u16)
const PICKER_TREE_LEVEL_COUNT_CF: &str = "PICKER_TREE_LEVEL_COUNT_CF"; // chain number and level (u16, u64) to number of blocks (u64)
const ORIGINATOR_TREE_LEVEL_CF: &str = "ORIGINATOR_TREE_LEVEL"; // level (u64) to hashes of blocks (Vec<hash>)
const PICKER_NODE_PICKED_LEVEL_CF: &str = "PICKER_NODE_PICKED_LEVEL"; // hash to max. picked level (u64)
const ORIGINATOR_NODE_PICK_CF: &str = "ORIGINATOR_NODE_PICK"; // hash to level and chain number of main chain picks (Vec<u16, u64>)
const ORIGINATOR_LEADER_SEQUENCE_CF: &str = "ORIGINATOR_LEADER_SEQUENCE"; // level (u64) to hash of leader block.
const ORIGINATOR_LEDGER_ORDER_CF: &str = "ORIGINATOR_LEDGER_ORDER"; // level (u64) to the list of originator blocks confirmed
                                                                // by this level, including the leader itself. The list
                                                                // is in the order that those blocks should live in the ledger.
const ORIGINATOR_PICK_COUNT_CF: &str = "ORIGINATOR_PICK_COUNT"; // number of all picks on a block

// Column family names for graph neighbors
const PARENT_NEIGHBOR_CF: &str = "GRAPH_PARENT_NEIGHBOR"; // the originator parent of a block
const PICK_NEIGHBOR_CF: &str = "GRAPH_PICK_NEIGHBOR"; // neighbors associated by a pick
const PICKER_PARENT_NEIGHBOR_CF: &str = "GRAPH_PICKER_PARENT_NEIGHBOR"; // the picker parent of a block
const TRANSACTION_REF_NEIGHBOR_CF: &str = "GRAPH_TRANSACTION_REF_NEIGHBOR";
const ORIGINATOR_REF_NEIGHBOR_CF: &str = "GRAPH_ORIGINATOR_REF_NEIGHBOR";

pub type Result<T> = std::result::Result<T, rocksdb::Error>;

// cf_handle is a lightweight operation, it takes 44000 micro seconds to get 100000 cf handles

pub struct BlockChain {
    db: DB,
    originator_best_level: Mutex<u64>,
    picker_best: Vec<Mutex<(H256, u64)>>,
    unreferred_transactions: Mutex<HashSet<H256>>,
    unreferred_originators: Mutex<HashSet<H256>>,
    unconfirmed_originators: Mutex<HashSet<H256>>,
    originator_ledger_tip: Mutex<u64>,
    picker_ledger_tips: Mutex<Vec<H256>>,
    config: BlockchainConfig,
}

// Functions to edit the blockchain
impl BlockChain {
    /// Open the blockchain database at the given path, and create missing column families.
    /// This function also populates the metadata fields with default values, and those
    /// fields must be initialized later.
    fn open<P: AsRef<std::path::Path>>(path: P, config: BlockchainConfig) -> Result<Self> {
        let mut cfs: Vec<ColumnFamilyDescriptor> = vec![];
        macro_rules! add_cf {
            ($cf:expr) => {{
                let cf_option = Options::default();
                let cf = ColumnFamilyDescriptor::new($cf, cf_option);
                cfs.push(cf);
            }};
            ($cf:expr, $merge_op:expr) => {{
                let mut cf_option = Options::default();
                cf_option.set_merge_operator("mo", $merge_op, None);
                let cf = ColumnFamilyDescriptor::new($cf, cf_option);
                cfs.push(cf);
            }};
        }
        add_cf!(ORIGINATOR_NODE_LEVEL_CF);
        add_cf!(PICKER_NODE_LEVEL_CF);
        add_cf!(PICKER_NODE_CHAIN_CF);
        add_cf!(PICKER_NODE_PICKED_LEVEL_CF);
        add_cf!(ORIGINATOR_LEADER_SEQUENCE_CF);
        add_cf!(ORIGINATOR_LEDGER_ORDER_CF);
        add_cf!(ORIGINATOR_TREE_LEVEL_CF, h256_vec_append_merge);
        add_cf!(ORIGINATOR_NODE_PICK_CF, pick_vec_merge);
        add_cf!(PARENT_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(PICK_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(PICKER_TREE_LEVEL_COUNT_CF, u64_plus_merge);
        add_cf!(ORIGINATOR_PICK_COUNT_CF, u64_plus_merge);
        add_cf!(PICKER_PARENT_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(TRANSACTION_REF_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(ORIGINATOR_REF_NEIGHBOR_CF, h256_vec_append_merge);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        let mut picker_best: Vec<Mutex<(H256, u64)>> = vec![];
        for _ in 0..config.picker_chains {
            picker_best.push(Mutex::new((H256::default(), 0)));
        }

        let blockchain_db = Self {
            db,
            originator_best_level: Mutex::new(0),
            picker_best,
            unreferred_transactions: Mutex::new(HashSet::new()),
            unreferred_originators: Mutex::new(HashSet::new()),
            unconfirmed_originators: Mutex::new(HashSet::new()),
            originator_ledger_tip: Mutex::new(0),
            picker_ledger_tips: Mutex::new(vec![H256::default(); config.picker_chains as usize]),
            config,
        };

        Ok(blockchain_db)
    }

    /// Destroy the existing database at the given path, create a new one, and initialize the content.
    pub fn new<P: AsRef<std::path::Path>>(path: P, config: BlockchainConfig) -> Result<Self> {
        DB::destroy(&Options::default(), &path)?;
        let db = Self::open(&path, config)?;
        // get cf handles
        let originator_node_level_cf = db.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        let picker_node_level_cf = db.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        let picker_node_chain_cf = db.db.cf_handle(PICKER_NODE_CHAIN_CF).unwrap();
        let picker_node_picked_level_cf = db.db.cf_handle(PICKER_NODE_PICKED_LEVEL_CF).unwrap();
        let originator_node_pick_cf = db.db.cf_handle(ORIGINATOR_NODE_PICK_CF).unwrap();
        let originator_tree_level_cf = db.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();
        let parent_neighbor_cf = db.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let pick_neighbor_cf = db.db.cf_handle(PICK_NEIGHBOR_CF).unwrap();
        let picker_tree_level_count_cf = db.db.cf_handle(PICKER_TREE_LEVEL_COUNT_CF).unwrap();
        let originator_pick_count_cf = db.db.cf_handle(ORIGINATOR_PICK_COUNT_CF).unwrap();
        let originator_leader_sequence_cf = db.db.cf_handle(ORIGINATOR_LEADER_SEQUENCE_CF).unwrap();
        let originator_ledger_order_cf = db.db.cf_handle(ORIGINATOR_LEDGER_ORDER_CF).unwrap();
        let originator_ref_neighbor_cf = db.db.cf_handle(ORIGINATOR_REF_NEIGHBOR_CF).unwrap();
        let transaction_ref_neighbor_cf = db.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();

        // insert genesis blocks
        let mut wb = WriteBatch::default();

        // originator genesis block
        wb.put_cf(
            originator_node_level_cf,
            serialize(&db.config.originator_genesis).unwrap(),
            serialize(&(0 as u64)).unwrap(),
        )?;
        wb.merge_cf(
            originator_tree_level_cf,
            serialize(&(0 as u64)).unwrap(),
            serialize(&db.config.originator_genesis).unwrap(),
        )?;
        let mut unreferred_originators = db.unreferred_originators.lock().unwrap();
        unreferred_originators.insert(db.config.originator_genesis);
        drop(unreferred_originators);
        wb.put_cf(
            originator_leader_sequence_cf,
            serialize(&(0 as u64)).unwrap(),
            serialize(&db.config.originator_genesis).unwrap(),
        )?;
        let originator_genesis_ledger: Vec<H256> = vec![db.config.originator_genesis];
        wb.put_cf(
            originator_ledger_order_cf,
            serialize(&(0 as u64)).unwrap(),
            serialize(&originator_genesis_ledger).unwrap(),
        )?;
        wb.put_cf(
            originator_ref_neighbor_cf,
            serialize(&db.config.originator_genesis).unwrap(),
            serialize(&Vec::<H256>::new()).unwrap(),
        )?;
        wb.put_cf(
            transaction_ref_neighbor_cf,
            serialize(&db.config.originator_genesis).unwrap(),
            serialize(&Vec::<H256>::new()).unwrap(),
        )?;

        // picker genesis blocks
        let mut picker_ledger_tips = db.picker_ledger_tips.lock().unwrap();
        for chain_num in 0..db.config.picker_chains {
            wb.put_cf(
                parent_neighbor_cf,
                serialize(&db.config.picker_genesis[chain_num as usize]).unwrap(),
                serialize(&db.config.originator_genesis).unwrap(),
            )?;
            wb.merge_cf(
                pick_neighbor_cf,
                serialize(&db.config.picker_genesis[chain_num as usize]).unwrap(),
                serialize(&db.config.originator_genesis).unwrap(),
            )?;
            wb.merge_cf(
                originator_pick_count_cf,
                serialize(&db.config.originator_genesis).unwrap(),
                serialize(&(1 as u64)).unwrap(),
            )?;
            wb.merge_cf(
                originator_node_pick_cf,
                serialize(&db.config.originator_genesis).unwrap(),
                serialize(&(true, chain_num as u16, 0 as u64)).unwrap(),
            )?;
            wb.put_cf(
                picker_node_level_cf,
                serialize(&db.config.picker_genesis[chain_num as usize]).unwrap(),
                serialize(&(0 as u64)).unwrap(),
            )?;
            wb.put_cf(
                picker_node_picked_level_cf,
                serialize(&db.config.picker_genesis[chain_num as usize]).unwrap(),
                serialize(&(0 as u64)).unwrap(),
            )?;
            wb.put_cf(
                picker_node_chain_cf,
                serialize(&db.config.picker_genesis[chain_num as usize]).unwrap(),
                serialize(&(chain_num as u16)).unwrap(),
            )?;
            wb.merge_cf(
                picker_tree_level_count_cf,
                serialize(&(chain_num as u16, 0 as u64)).unwrap(),
                serialize(&(1 as u64)).unwrap(),
            )?;
            let mut picker_best = db.picker_best[chain_num as usize].lock().unwrap();
            picker_best.0 = db.config.picker_genesis[chain_num as usize];
            drop(picker_best);
            picker_ledger_tips[chain_num as usize] = db.config.picker_genesis[chain_num as usize];
        }
        drop(picker_ledger_tips);
        db.db.write(wb)?;

        Ok(db)
    }

    /// Insert a new block into the ledger. Returns the list of added transaction blocks and
    /// removed transaction blocks.
    pub fn insert_block(&self, block: &Block) -> Result<()> {
        // get cf handles
        let originator_node_level_cf = self.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        let picker_node_level_cf = self.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        let picker_node_chain_cf = self.db.cf_handle(PICKER_NODE_CHAIN_CF).unwrap();
        let picker_node_picked_level_cf = self.db.cf_handle(PICKER_NODE_PICKED_LEVEL_CF).unwrap();
        let originator_tree_level_cf = self.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();
        let parent_neighbor_cf = self.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let pick_neighbor_cf = self.db.cf_handle(PICK_NEIGHBOR_CF).unwrap();
        let originator_pick_count_cf = self.db.cf_handle(ORIGINATOR_PICK_COUNT_CF).unwrap();
        let picker_parent_neighbor_cf = self.db.cf_handle(PICKER_PARENT_NEIGHBOR_CF).unwrap();
        let transaction_ref_neighbor_cf = self.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();
        let originator_ref_neighbor_cf = self.db.cf_handle(ORIGINATOR_REF_NEIGHBOR_CF).unwrap();
        let picker_tree_level_count_cf = self.db.cf_handle(PICKER_TREE_LEVEL_COUNT_CF).unwrap();

        let mut wb = WriteBatch::default();

        macro_rules! get_value {
            ($cf:expr, $key:expr) => {{
                deserialize(
                    &self
                        .db
                        .get_pinned_cf($cf, serialize(&$key).unwrap())?
                        .unwrap(),
                )
                .unwrap()
            }};
        }

        macro_rules! put_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.put_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
            }};
        }

        macro_rules! merge_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.merge_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
            }};
        }

        // insert parent link
        let block_hash = block.hash();
        let parent_hash = block.header.parent;
        put_value!(parent_neighbor_cf, block_hash, parent_hash);

        match &block.content {
            Content::Originator(content) => {
                // add ref'ed blocks
                // note that the parent is the first originator block that we refer
                let mut refed_originator: Vec<H256> = vec![parent_hash];
                refed_originator.extend(&content.originator_refs);
                put_value!(originator_ref_neighbor_cf, block_hash, refed_originator);
                put_value!(
                    transaction_ref_neighbor_cf,
                    block_hash,
                    content.transaction_refs
                );
                // get current block level
                let parent_level: u64 = get_value!(originator_node_level_cf, parent_hash);
                let self_level = parent_level + 1;
                // set current block level
                put_value!(originator_node_level_cf, block_hash, self_level as u64);
                merge_value!(originator_tree_level_cf, self_level, block_hash);

                // mark ourself as unreferred originator
                // This should happen before committing to the database, since we want this
                // add operation to happen before later block deletes it. NOTE: we could do this
                // after committing to the database. The solution is to add a "pre-delete" set in
                // unreferred_originators that collects the entries to delete before they are even
                // inserted. If we have this, we don't need to add/remove entries in order.
                let mut unreferred_originators = self.unreferred_originators.lock().unwrap();
                unreferred_originators.insert(block_hash);
                drop(unreferred_originators);

                // mark outself as unconfirmed originator
                // This could happen before committing to database, since this block has to become
                // the leader or be referred by a leader. However, both requires the block to be
                // committed to the database. For the same reason, this should not happen after
                // committing to the database (think about the case where this block immediately
                // becomes the leader and is ready to be confirmed).
                let mut unconfirmed_originators = self.unconfirmed_originators.lock().unwrap();
                unconfirmed_originators.insert(block_hash);
                drop(unconfirmed_originators);

                // commit to the database and update originator best in the same atomic operation
                // These two happen together to ensure that if a picker/originator/transaction block
                // depend on this originator block, the miner must have already known about this
                // originator block and is using it as the originator parent.
                let mut originator_best = self.originator_best_level.lock().unwrap();
                self.db.write(wb)?;
                if self_level > *originator_best {
                    *originator_best = self_level;
                    PERFORMANCE_COUNTER.record_update_originator_main_chain(self_level as usize);
                }
                drop(originator_best);

                // remove referenced originator and transaction blocks from the unreferred list
                // This could happen after committing to the database. It's because that we are
                // only removing transaction blocks here, and the entries we are trying to remove
                // are guaranteed to be already there (since they are inserted before the
                // corresponding transaction blocks are committed).
                let mut unreferred_originators = self.unreferred_originators.lock().unwrap();
                for ref_hash in &content.originator_refs {
                    unreferred_originators.remove(&ref_hash);
                }
                unreferred_originators.remove(&parent_hash);
                drop(unreferred_originators);
                let mut unreferred_transactions = self.unreferred_transactions.lock().unwrap();
                for ref_hash in &content.transaction_refs {
                    unreferred_transactions.remove(&ref_hash);
                }
                drop(unreferred_transactions);

                debug!(
                    "Adding originator block {:.8} at level {}",
                    block_hash, self_level
                );
            }
            Content::Picker(content) => {
                // add picker parent
                let picker_parent_hash = content.picker_parent;
                put_value!(picker_parent_neighbor_cf, block_hash, picker_parent_hash);
                // get current block level and chain number
                let picker_parent_level: u64 = get_value!(picker_node_level_cf, picker_parent_hash);
                let picker_parent_chain: u16 = get_value!(picker_node_chain_cf, picker_parent_hash);
                let self_level = picker_parent_level + 1;
                let self_chain = picker_parent_chain;
                // set current block level and chain number
                put_value!(picker_node_level_cf, block_hash, self_level as u64);
                put_value!(picker_node_chain_cf, block_hash, self_chain as u16);
                merge_value!(
                    picker_tree_level_count_cf,
                    (self_chain as u16, self_level as u64),
                    1 as u64
                );
                // add voting blocks for the originator
                for originator_hash in &content.picks {
                    merge_value!(originator_pick_count_cf, originator_hash, 1 as u64);
                }
                // add picked blocks and set deepest picked level
                put_value!(pick_neighbor_cf, block_hash, content.picks);
                // set the picked level to be until originator parent
                let originator_parent_level: u64 = get_value!(originator_node_level_cf, parent_hash);
                put_value!(
                    picker_node_picked_level_cf,
                    block_hash,
                    originator_parent_level as u64
                );

                self.db.write(wb)?;

                // This should happen after writing to db, because other modules will follow
                // picker_best to query its metadata. We need to get the metadata into database
                // before we can "announce" this block to other modules. Also, this does not create
                // race condition, since this update is "stateless" - we are not append/removing
                // from a record.
                let mut picker_best = self.picker_best[self_chain as usize].lock().unwrap();
                // update best block
                if self_level > picker_best.1 {
                    PERFORMANCE_COUNTER
                        .record_update_picker_main_chain(picker_best.1 as usize, self_level as usize);
                    picker_best.0 = block_hash;
                    picker_best.1 = self_level;
                }
                drop(picker_best);
                debug!(
                    "Adding picker block {:.8} at chain {} level {}",
                    block_hash, self_chain, self_level
                );
            }
            Content::Transaction(_content) => {
                // mark itself as unreferred
                // Note that this could happen before committing to db, because no module will try
                // to access transaction content based on pointers in unreferred_transactions.
                let mut unreferred_transactions = self.unreferred_transactions.lock().unwrap();
                unreferred_transactions.insert(block_hash);
                drop(unreferred_transactions);

                // This db write is only to facilitate check_existence
                self.db.write(wb)?;
            }
        }
        Ok(())
    }

    pub fn update_ledger(&self) -> Result<(Vec<H256>, Vec<H256>)> {
        let originator_node_pick_cf = self.db.cf_handle(ORIGINATOR_NODE_PICK_CF).unwrap();
        let originator_node_level_cf = self.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        let originator_leader_sequence_cf = self.db.cf_handle(ORIGINATOR_LEADER_SEQUENCE_CF).unwrap();
        let originator_ledger_order_cf = self.db.cf_handle(ORIGINATOR_LEDGER_ORDER_CF).unwrap();
        let originator_ref_neighbor_cf = self.db.cf_handle(ORIGINATOR_REF_NEIGHBOR_CF).unwrap();
        let transaction_ref_neighbor_cf = self.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();

        macro_rules! get_value {
            ($cf:expr, $key:expr) => {{
                match self.db.get_pinned_cf($cf, serialize(&$key).unwrap())? {
                    Some(raw) => Some(deserialize(&raw).unwrap()),
                    None => None,
                }
            }};
        }

        // apply the pick diff while tracking the picks of which originator levels are affected
        let mut wb = WriteBatch::default();
        macro_rules! merge_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.merge_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
            }};
        }

        let mut picker_ledger_tips = self.picker_ledger_tips.lock().unwrap();
        let mut affected_range: Range<u64> = Range {
            start: std::u64::MAX,
            end: std::u64::MIN,
        };

        for chain_num in 0..self.config.picker_chains {
            // get the diff of picks on this picker chain
            let from = picker_ledger_tips[chain_num as usize];
            let picker_best = self.picker_best[chain_num as usize].lock().unwrap();
            let to = picker_best.0;
            drop(picker_best);
            picker_ledger_tips[chain_num as usize] = to;

            let (added, removed) = self.pick_diff(from, to)?;

            // apply the pick diff on the originator main chain pick cf
            for pick in &removed {
                merge_value!(
                    originator_node_pick_cf,
                    pick.0,
                    (false, chain_num as u16, pick.1)
                );
                let originator_level: u64 = get_value!(originator_node_level_cf, pick.0).unwrap();
                if originator_level < affected_range.start {
                    affected_range.start = originator_level;
                }
                if originator_level >= affected_range.end {
                    affected_range.end = originator_level + 1;
                }
            }

            for pick in &added {
                merge_value!(
                    originator_node_pick_cf,
                    pick.0,
                    (true, chain_num as u16, pick.1)
                );
                let originator_level: u64 = get_value!(originator_node_level_cf, pick.0).unwrap();
                if originator_level < affected_range.start {
                    affected_range.start = originator_level;
                }
                if originator_level >= affected_range.end {
                    affected_range.end = originator_level + 1;
                }
            }
        }
        drop(picker_ledger_tips);
        // commit the picks into the database
        self.db.write(wb)?;

        // recompute the leader of each level that was affected
        let mut wb = WriteBatch::default();
        /*
        macro_rules! merge_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.merge_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
            }};
        }
        */
        macro_rules! put_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.put_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
            }};
        }
        macro_rules! delete_value {
            ($cf:expr, $key:expr) => {{
                wb.delete_cf($cf, serialize(&$key).unwrap())?;
            }};
        }

        // we will recompute the leader starting from min. affected level or ledger tip + 1,
        // whichever is smaller. so make min. affected level the smaller of the two
        if affected_range.start < affected_range.end {
            let originator_ledger_tip_lock = self.originator_ledger_tip.lock().unwrap();
            let originator_ledger_tip: u64 = *originator_ledger_tip_lock;
            drop(originator_ledger_tip_lock);
            if originator_ledger_tip + 1 < affected_range.start {
                affected_range.start = originator_ledger_tip + 1;
            }
        }

        // start actually recomputing the leaders
        let mut change_begin: Option<u64> = None;

        for level in affected_range {
            let existing_leader: Option<H256> =
                get_value!(originator_leader_sequence_cf, level as u64);
            let new_leader_confirm: Option<H256> =
                self.originator_leader(level as u64, self.config.quantile_epsilon_confirm)?;
            let new_leader_deconfirm: Option<H256> =
                self.originator_leader(level as u64, self.config.quantile_epsilon_deconfirm)?;

            // we confirm with a higher confidence so we don't have false deconfirmation
            let new_leader = {
                if existing_leader.is_some() {
                    new_leader_deconfirm
                } else {
                    new_leader_confirm
                }
            };

            if new_leader != existing_leader {
                match new_leader {
                    Some(hash) => info!(
                        "New originator leader selected for level {}: {:.8}",
                        level, hash
                    ),
                    None => warn!("Originator leader deconfirmed for level {}", level),
                }
                // mark it's the beginning of the change
                if change_begin.is_none() {
                    change_begin = Some(level);
                }
                match new_leader {
                    None => delete_value!(originator_leader_sequence_cf, level as u64),
                    Some(new) => put_value!(originator_leader_sequence_cf, level as u64, new),
                };
            }
        }
        // commit the new leaders into the database
        self.db.write(wb)?;

        // recompute the ledger from the first level whose leader changed
        if let Some(change_begin) = change_begin {
            let mut originator_ledger_tip = self.originator_ledger_tip.lock().unwrap();
            let mut unconfirmed_originators = self.unconfirmed_originators.lock().unwrap();
            let mut removed: Vec<H256> = vec![];
            let mut added: Vec<H256> = vec![];
            let mut wb = WriteBatch::default();
            /*
            macro_rules! merge_value {
                ($cf:expr, $key:expr, $value:expr) => {{
                    wb.merge_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
                }};
            }
            */
            macro_rules! put_value {
                ($cf:expr, $key:expr, $value:expr) => {{
                    wb.put_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap())?;
                }};
            }
            macro_rules! delete_value {
                ($cf:expr, $key:expr) => {{
                    wb.delete_cf($cf, serialize(&$key).unwrap())?;
                }};
            }

            // deconfirm the blocks from change_begin all the way to previous ledger tip
            for level in change_begin..=*originator_ledger_tip {
                let original_ledger: Vec<H256> =
                    get_value!(originator_ledger_order_cf, level as u64).unwrap();
                delete_value!(originator_ledger_order_cf, level as u64);
                for block in &original_ledger {
                    unconfirmed_originators.insert(*block);
                    removed.push(*block);
                }
            }

            // recompute the ledger from change_begin until the first level where there's no leader
            // make sure that the ledger is continuous
            if change_begin <= *originator_ledger_tip + 1 {
                for level in change_begin.. {
                    let leader: H256 = match get_value!(originator_leader_sequence_cf, level as u64) {
                        None => {
                            *originator_ledger_tip = level - 1;
                            break;
                        }
                        Some(leader) => leader,
                    };
                    // Get the sequence of blocks by doing a depth-first traverse
                    let mut order: Vec<H256> = vec![];
                    let mut stack: Vec<H256> = vec![leader];
                    while let Some(top) = stack.pop() {
                        // if it's already
                        // confirmed before, ignore it
                        if !unconfirmed_originators.contains(&top) {
                            continue;
                        }
                        let refs: Vec<H256> = get_value!(originator_ref_neighbor_cf, top).unwrap();

                        // add the current block to the ordered ledger, could be duplicated
                        order.push(top);

                        // search all referred blocks
                        for ref_hash in &refs {
                            stack.push(*ref_hash);
                        }
                    }

                    // reverse the order we just got
                    order.reverse();
                    // deduplicate, keep the one copy that is former in this order
                    order = order
                        .into_iter()
                        .filter(|h| unconfirmed_originators.remove(h))
                        .collect();
                    put_value!(originator_ledger_order_cf, level as u64, order);
                    added.extend(&order);
                }
            }
            // commit the new ledger into the database
            self.db.write(wb)?;

            let mut removed_transaction_blocks: Vec<H256> = vec![];
            let mut added_transaction_blocks: Vec<H256> = vec![];
            for block in &removed {
                let t: Vec<H256> = get_value!(transaction_ref_neighbor_cf, block).unwrap();
                removed_transaction_blocks.extend(&t);
            }
            for block in &added {
                let t: Vec<H256> = get_value!(transaction_ref_neighbor_cf, block).unwrap();
                added_transaction_blocks.extend(&t);
            }
            Ok((added_transaction_blocks, removed_transaction_blocks))
        } else {
            Ok((vec![], vec![]))
        }
    }

    fn originator_leader(&self, level: u64, quantile: f32) -> Result<Option<H256>> {
        let originator_node_pick_cf = self.db.cf_handle(ORIGINATOR_NODE_PICK_CF).unwrap();
        let originator_tree_level_cf = self.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();

        macro_rules! get_value {
            ($cf:expr, $key:expr) => {{
                match self.db.get_pinned_cf($cf, serialize(&$key).unwrap())? {
                    Some(raw) => Some(deserialize(&raw).unwrap()),
                    None => None,
                }
            }};
        }
        let originator_blocks: Vec<H256> = get_value!(originator_tree_level_cf, level as u64).unwrap();
        // compute the new leader of this level
        // we use the confirmation policy from https://arxiv.org/abs/1810.08092
        let mut new_leader: Option<H256> = None;

        // collect the depth of each pick on each originator block
        let mut picks_depth: HashMap<&H256, Vec<u64>> = HashMap::new(); // chain number and pick depth casted on the originator block

        // collect the total picks on all originator blocks, and the number of
        // picker blocks mined after those picks are casted
        let mut total_pick_count: u16 = 0;
        let mut total_pick_blocks: u64 = 0;

        for block in &originator_blocks {
            let picks: Vec<(u16, u64)> = match get_value!(originator_node_pick_cf, block) {
                None => vec![],
                Some(d) => d,
            };
            let mut pick_depth: Vec<u64> = vec![];
            for (chain_num, pick_level) in &picks {
                // TODO: cache the picker chain best levels
                let picker_best = self.picker_best[*chain_num as usize].lock().unwrap();
                let picker_best_level = picker_best.1;
                drop(picker_best);
                total_pick_blocks += self
                    .num_picker_blocks(*chain_num, *pick_level, picker_best_level)
                    .unwrap();
                total_pick_count += 1;
                let this_depth = picker_best_level - pick_level + 1;
                pick_depth.push(this_depth);
            }
            picks_depth.insert(block, pick_depth);
        }

        // For debugging purpose only. This is very important for security.
        // TODO: remove this check in the future
        if self.config.picker_chains < total_pick_count {
            panic!(
                "self.config.picker_chains: {} total_picks:{}",
                self.config.picker_chains, total_pick_count
            )
        }

        // no point in going further if less than 3/5 picks are cast
        if total_pick_count > self.config.picker_chains * 3 / 5 {
            // calculate the average number of picker blocks mined after
            // a pick is casted. we use this as an estimator of honest mining
            // rate, and then derive the believed malicious mining rate
            let avg_pick_blocks = total_pick_blocks as f32 / f32::from(total_pick_count);
            // expected picker depth of an adversary
            let adversary_expected_pick_depth =
                avg_pick_blocks / (1.0 - self.config.adversary_ratio) * self.config.adversary_ratio;
            let poisson = Poisson::new(f64::from(adversary_expected_pick_depth)).unwrap();

            // for each block calculate the lower bound on the number of picks
            let mut picks_lcb: HashMap<&H256, f32> = HashMap::new();
            let mut total_picks_lcb: f32 = 0.0;
            let mut max_pick_lcb: f32 = 0.0;

            for block in &originator_blocks {
                let picks = picks_depth.get(block).unwrap();

                let mut block_picks_mean: f32 = 0.0; // mean E[X]
                let mut block_picks_variance: f32 = 0.0; // Var[X]
                let mut block_picks_lcb: f32 = 0.0;
                for depth in picks.iter() {
                    // probability that the adversary will remove this pick
                    let mut p: f32 = 1.0 - poisson.cdf((*depth as f32 + 1.0).into()) as f32;
                    for k in 0..(*depth as u64) {
                        // probability that the adversary has mined k blocks
                        let p1 = poisson.pmf(k) as f32;
                        // probability that the adversary will overtake 'depth-k' blocks
                        let p2 = (self.config.adversary_ratio
                            / (1.0 - self.config.adversary_ratio))
                            .powi((depth - k + 1) as i32);
                        p += p1 * p2;
                    }
                    block_picks_mean += 1.0 - p;
                    block_picks_variance += p * (1.0 - p);
                }
                // using gaussian approximation
                let tmp = block_picks_mean - (block_picks_variance).sqrt() * quantile;
                if tmp > 0.0 {
                    block_picks_lcb += tmp;
                }
                picks_lcb.insert(block, block_picks_lcb);
                total_picks_lcb += block_picks_lcb;

                if max_pick_lcb < block_picks_lcb {
                    max_pick_lcb = block_picks_lcb;
                    new_leader = Some(*block);
                }
                // In case of a tie, choose block with lower hash.
                if (max_pick_lcb - block_picks_lcb).abs() < std::f32::EPSILON
                    && new_leader.is_some()
                {
                    // TODO: is_some required?
                    if *block < new_leader.unwrap() {
                        new_leader = Some(*block);
                    }
                }
            }
            // check if the lcb_pick of new_leader is bigger than second best ucb picks
            let remaining_picks = f32::from(self.config.picker_chains) - total_picks_lcb;

            // if max_pick_lcb is lesser than the remaining_picks, then a private block could
            // get the remaining picks and become the leader block
            if max_pick_lcb <= remaining_picks || new_leader.is_none() {
                new_leader = None;
            } else {
                for p_block in &originator_blocks {
                    // if the below condition is true, then final picks on p_block could overtake new_leader
                    if max_pick_lcb < picks_lcb.get(p_block).unwrap() + remaining_picks
                        && *p_block != new_leader.unwrap()
                    {
                        new_leader = None;
                        break;
                    }
                    //In case of a tie, choose block with lower hash.
                    if (max_pick_lcb - (picks_lcb.get(p_block).unwrap() + remaining_picks)).abs()
                        < std::f32::EPSILON
                        && *p_block < new_leader.unwrap()
                    {
                        new_leader = None;
                        break;
                    }
                }
            }
        }

        Ok(new_leader)
    }

    fn num_picker_blocks(&self, chain: u16, start_level: u64, end_level: u64) -> Result<u64> {
        let picker_tree_level_count_cf = self.db.cf_handle(PICKER_TREE_LEVEL_COUNT_CF).unwrap();
        let mut total: u64 = 0;
        for l in start_level..=end_level {
            let t: u64 = deserialize(
                &self
                    .db
                    .get_pinned_cf(picker_tree_level_count_cf, serialize(&(chain, l)).unwrap())?
                    .unwrap(),
            )
            .unwrap();
            total += t;
        }
        Ok(total)
    }

    /// Given two picker blocks on the same chain, calculate the added and removed picks when
    /// switching the main chain.
    fn pick_diff(&self, from: H256, to: H256) -> Result<(Vec<(H256, u64)>, Vec<(H256, u64)>)> {
        // get cf handles
        let picker_node_level_cf = self.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        let pick_neighbor_cf = self.db.cf_handle(PICK_NEIGHBOR_CF).unwrap();
        let picker_parent_neighbor_cf = self.db.cf_handle(PICKER_PARENT_NEIGHBOR_CF).unwrap();

        macro_rules! get_value {
            ($cf:expr, $key:expr) => {{
                deserialize(
                    &self
                        .db
                        .get_pinned_cf($cf, serialize(&$key).unwrap())?
                        .unwrap(),
                )
                .unwrap()
            }};
        }

        let mut to: H256 = to;
        let mut from: H256 = from;

        let mut to_level: u64 = get_value!(picker_node_level_cf, to);
        let mut from_level: u64 = get_value!(picker_node_level_cf, from);

        let mut added_picks: Vec<(H256, u64)> = vec![];
        let mut removed_picks: Vec<(H256, u64)> = vec![];

        // trace back the longer chain until the levels of the two tips are the same
        while to_level != from_level {
            if to_level > from_level {
                let picks: Vec<H256> = get_value!(pick_neighbor_cf, to);
                for pick in picks {
                    added_picks.push((pick, to_level));
                }
                to = get_value!(picker_parent_neighbor_cf, to);
                to_level -= 1;
            } else if to_level < from_level {
                let picks: Vec<H256> = get_value!(pick_neighbor_cf, from);
                for pick in picks {
                    removed_picks.push((pick, from_level));
                }
                from = get_value!(picker_parent_neighbor_cf, from);
                from_level -= 1;
            }
        }

        while to != from {
            let picks: Vec<H256> = get_value!(pick_neighbor_cf, to);
            for pick in picks {
                added_picks.push((pick, to_level));
            }
            to = get_value!(picker_parent_neighbor_cf, to);
            to_level -= 1;

            let picks: Vec<H256> = get_value!(pick_neighbor_cf, from);
            for pick in picks {
                removed_picks.push((pick, from_level));
            }
            from = get_value!(picker_parent_neighbor_cf, from);
            from_level -= 1;
        }
        Ok((added_picks, removed_picks))
    }

    pub fn best_originator(&self) -> Result<H256> {
        let originator_tree_level_cf = self.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();

        let originator_best = self.originator_best_level.lock().unwrap();
        let level: u64 = *originator_best;
        drop(originator_best);
        let blocks: Vec<H256> = deserialize(
            &self
                .db
                .get_pinned_cf(originator_tree_level_cf, serialize(&level).unwrap())?
                .unwrap(),
        )
        .unwrap();
        Ok(blocks[0])
    }

    pub fn best_picker(&self, chain_num: usize) -> H256 {
        let picker_best = self.picker_best[chain_num].lock().unwrap();
        let hash = picker_best.0;
        drop(picker_best);
        hash
    }

    pub fn unreferred_originators(&self) -> Vec<H256> {
        // TODO: does ordering matter?
        // TODO: should remove the parent block when mining
        let unreferred_originators = self.unreferred_originators.lock().unwrap();
        let list: Vec<H256> = unreferred_originators.iter().cloned().collect();
        drop(unreferred_originators);
        list
    }

    pub fn unreferred_transactions(&self) -> Vec<H256> {
        // TODO: does ordering matter?
        let unreferred_transactions = self.unreferred_transactions.lock().unwrap();
        let list: Vec<H256> = unreferred_transactions.iter().cloned().collect();
        drop(unreferred_transactions);
        list
    }

    /// Get the list of unpicked originator blocks that a picker chain should pick for, given the tip
    /// of the particular picker chain.
    pub fn unpicked_originator(&self, tip: &H256, originator_parent: &H256) -> Result<Vec<H256>> {
        let picker_node_picked_level_cf = self.db.cf_handle(PICKER_NODE_PICKED_LEVEL_CF).unwrap();
        let originator_node_level_cf = self.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        let originator_tree_level_cf = self.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();
        let originator_pick_count_cf = self.db.cf_handle(ORIGINATOR_PICK_COUNT_CF).unwrap();
        // get the deepest picked level
        let first_pick_level: u64 = deserialize(
            &self
                .db
                .get_pinned_cf(picker_node_picked_level_cf, serialize(&tip).unwrap())?
                .unwrap(),
        )
        .unwrap();

        let last_pick_level: u64 = deserialize(
            &self
                .db
                .get_pinned_cf(originator_node_level_cf, serialize(&originator_parent).unwrap())?
                .unwrap(),
        )
        .unwrap();

        // get the block with the most picks on each originator level
        // and break ties with hash value
        let mut list: Vec<H256> = vec![];
        for level in first_pick_level + 1..=last_pick_level {
            let mut blocks: Vec<H256> = deserialize(
                &self
                    .db
                    .get_pinned_cf(originator_tree_level_cf, serialize(&(level as u64)).unwrap())?
                    .unwrap(),
            )
            .unwrap();
            blocks.sort_unstable();
            // the current best originator block to pick for
            let mut best_pick: Option<(H256, u64)> = None;
            for block_hash in &blocks {
                let pick_count: u64 = match &self
                    .db
                    .get_pinned_cf(originator_pick_count_cf, serialize(&block_hash).unwrap())?
                {
                    Some(d) => deserialize(d).unwrap(),
                    None => 0,
                };
                match best_pick {
                    Some((_, num_picks)) => {
                        if pick_count > num_picks {
                            best_pick = Some((*block_hash, pick_count));
                        }
                    }
                    None => {
                        best_pick = Some((*block_hash, pick_count));
                    }
                }
            }
            list.push(best_pick.unwrap().0); //Note: the last pick in list could be other originator that at the same level of originator_parent
        }
        Ok(list)
    }

    /// Get the level of the originator block
    pub fn originator_level(&self, hash: &H256) -> Result<u64> {
        let originator_node_level_cf = self.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        let level: u64 = deserialize(
            &self
                .db
                .get_pinned_cf(originator_node_level_cf, serialize(&hash).unwrap())?
                .unwrap(),
        )
        .unwrap();
        Ok(level)
    }

    /// Get the deepest picked level of a picker
    pub fn deepest_picked_level(&self, picker: &H256) -> Result<u64> {
        let picker_node_picked_level_cf = self.db.cf_handle(PICKER_NODE_PICKED_LEVEL_CF).unwrap();
        // get the deepest picked level
        let picked_level: u64 = deserialize(
            &self
                .db
                .get_pinned_cf(picker_node_picked_level_cf, serialize(picker).unwrap())?
                .unwrap(),
        )
        .unwrap();
        Ok(picked_level)
    }

    /// Get the chain number of the picker block
    pub fn picker_chain_number(&self, hash: &H256) -> Result<u16> {
        let picker_node_chain_cf = self.db.cf_handle(PICKER_NODE_CHAIN_CF).unwrap();
        let chain: u16 = deserialize(
            &self
                .db
                .get_pinned_cf(picker_node_chain_cf, serialize(&hash).unwrap())?
                .unwrap(),
        )
        .unwrap();
        Ok(chain)
    }

    /// Check whether the given originator block exists in the database.
    pub fn contains_originator(&self, hash: &H256) -> Result<bool> {
        let originator_node_level_cf = self.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        match self
            .db
            .get_pinned_cf(originator_node_level_cf, serialize(&hash).unwrap())?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Check whether the given picker block exists in the database.
    pub fn contains_picker(&self, hash: &H256) -> Result<bool> {
        let picker_node_level_cf = self.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        match self
            .db
            .get_pinned_cf(picker_node_level_cf, serialize(&hash).unwrap())?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Check whether the given transaction block exists in the database.
    // TODO: we can't tell whether it's is a transaction block!
    pub fn contains_transaction(&self, hash: &H256) -> Result<bool> {
        let parent_neighbor_cf = self.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        match self
            .db
            .get_pinned_cf(parent_neighbor_cf, serialize(&hash).unwrap())?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn originator_leaders(&self) -> Result<Vec<H256>> {
        let originator_leader_sequence_cf = self.db.cf_handle(ORIGINATOR_LEADER_SEQUENCE_CF).unwrap();
        let originator_ledger_tip = self.originator_ledger_tip.lock().unwrap();
        let snapshot = self.db.snapshot();
        let ledger_tip_level = *originator_ledger_tip;
        let mut leaders = vec![];
        drop(originator_ledger_tip);
        for level in 0..=ledger_tip_level {
            match snapshot.get_cf(originator_leader_sequence_cf, serialize(&level).unwrap())? {
                Some(d) => {
                    let hash: H256 = deserialize(&d).unwrap();
                    leaders.push(hash);
                }
                None => unreachable!(),
            }
        }
        Ok(leaders)
    }
}

impl BlockChain {
    pub fn originator_transaction_in_ledger(&self, limit: u64) -> Result<Vec<(H256, Vec<H256>)>> {
        let ledger_tip_ = self.originator_ledger_tip.lock().unwrap();
        let ledger_tip = *ledger_tip_;
        // TODO: get snapshot here doesn't ensure consistency of snapshot, since we use multiple write batch in `insert_block`
        // and the ledger_tip lock doesn't ensure it either.
        let snapshot = self.db.snapshot();
        drop(ledger_tip_);

        let ledger_bottom: u64 = if ledger_tip > limit {
            ledger_tip - limit
        } else {
            0
        };
        let originator_ledger_order_cf = self.db.cf_handle(ORIGINATOR_LEDGER_ORDER_CF).unwrap();
        let transaction_ref_neighbor_cf = self.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();

        let mut originator_in_ledger: Vec<H256> = vec![];
        let mut ledger: Vec<(H256, Vec<H256>)> = vec![];

        for level in ledger_bottom..=ledger_tip {
            match snapshot.get_cf(originator_ledger_order_cf, serialize(&level).unwrap())? {
                Some(d) => {
                    let mut blocks: Vec<H256> = deserialize(&d).unwrap();
                    originator_in_ledger.append(&mut blocks);
                }
                None => {
                    unreachable!("level <= ledger tip should exist in originator_ledger_order_cf")
                }
            }
        }

        for hash in &originator_in_ledger {
            let blocks: Vec<H256> = match snapshot.get_cf(transaction_ref_neighbor_cf, serialize(&hash).unwrap())? {
                Some(d) => deserialize(&d).unwrap(),
                None => unreachable!("originator in ledger should have transaction ref in database (even for empty ref)"),
            };
            ledger.push((*hash, blocks));
        }
        Ok(ledger)
    }

    pub fn originator_bottom_tip(&self) -> Result<(H256, H256, u64)> {
        let originator_tree_level_cf = self.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();
        let originator_bottom = match self
            .db
            .get_pinned_cf(originator_tree_level_cf, serialize(&1u64).unwrap())?
        {
            Some(d) => {
                let blocks: Vec<H256> = deserialize(&d).unwrap();
                blocks.into_iter().next()
            }
            None => None,
        };
        if let Some(originator_bottom) = originator_bottom {
            let originator_best = self.originator_best_level.lock().unwrap();
            let originator_best_level = *originator_best;
            drop(originator_best);
            let originator_tip = match self.db.get_pinned_cf(
                originator_tree_level_cf,
                serialize(&originator_best_level).unwrap(),
            )? {
                Some(d) => {
                    let blocks: Vec<H256> = deserialize(&d).unwrap();
                    blocks[0]
                }
                None => unreachable!(),
            };
            Ok((originator_bottom, originator_tip, originator_best_level))
        } else {
            Ok((H256::default(), H256::default(), 0))
        }
    }

    pub fn picker_bottom_tip(&self) -> Result<Vec<(H256, H256, u64)>> {
        let picker_node_chain_cf = self.db.cf_handle(PICKER_NODE_CHAIN_CF).unwrap();
        let picker_node_level_cf = self.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        let iter = self
            .db
            .iterator_cf(picker_node_chain_cf, rocksdb::IteratorMode::Start)?;
        // vector of pair (level-1 picker, best picker, best level)
        let mut pickers = vec![(H256::default(), H256::default(), 0u64); self.picker_best.len()];
        for (k, v) in iter {
            let hash: H256 = deserialize(k.as_ref()).unwrap();
            let chain: u16 = deserialize(v.as_ref()).unwrap();
            let level: u64 = match self.db.get_pinned_cf(picker_node_level_cf, k.as_ref())? {
                Some(d) => deserialize(&d).unwrap(),
                None => unreachable!("picker should have level"),
            };
            if level == 1 {
                pickers[chain as usize].0 = hash;
            }
        }
        for chain in 0..self.picker_best.len() {
            let picker_best = self.picker_best[chain].lock().unwrap();
            pickers[chain as usize].1 = picker_best.0;
            pickers[chain as usize].2 = picker_best.1;
        }
        Ok(pickers)
    }

    pub fn dump(&self, limit: u64, display_fork: bool) -> Result<String> {
        /// Struct to hold blockchain data to be dumped
        #[derive(Serialize)]
        struct Dump {
            edges: Vec<Edge>,
            originator_levels: Vec<Vec<String>>,
            originator_leaders: BTreeMap<u64, String>,
            picker_longest: Vec<String>,
            originator_nodes: HashMap<String, Originator>,
            picker_nodes: HashMap<String, Picker>,
            //pub transaction_unconfirmed: Vec<String>, //what is the definition of this?
            transaction_in_ledger: Vec<String>,
            transaction_unreferred: Vec<String>,
            originator_in_ledger: Vec<String>,
            picker_chain_number: Vec<String>,
            originator_tree_number: String,
        }

        #[derive(Serialize)]
        enum EdgeType {
            PickerToPickerParent,
            OriginatorToOriginatorParent,
            PickerToOriginatorPick,
        }
        #[derive(Serialize)]
        struct Edge {
            from: String,
            to: String,
            edgetype: EdgeType,
        }

        #[derive(Serialize)]
        struct Originator {
            level: u64,
            status: OriginatorStatus,
            picks: u16,
        }

        #[derive(Serialize)]
        enum OriginatorStatus {
            Leader,
            Others, //don't know what other status we need for originator? like unconfirmed? unreferred?
        }

        #[derive(Serialize)]
        struct Picker {
            chain: u16,
            level: u64,
            status: PickerStatus,
            deepest_pick_level: u64,
        }

        #[derive(Serialize)]
        enum PickerStatus {
            OnMainChain,
            Orphan,
        }

        let originator_tree_level_cf = self.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();
        let originator_leader_sequence_cf = self.db.cf_handle(ORIGINATOR_LEADER_SEQUENCE_CF).unwrap();
        let parent_neighbor_cf = self.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let originator_node_pick_cf = self.db.cf_handle(ORIGINATOR_NODE_PICK_CF).unwrap();
        let picker_parent_neighbor_cf = self.db.cf_handle(PICKER_PARENT_NEIGHBOR_CF).unwrap();
        let picker_node_picked_level_cf = self.db.cf_handle(PICKER_NODE_PICKED_LEVEL_CF).unwrap();
        let originator_ledger_order_cf = self.db.cf_handle(ORIGINATOR_LEDGER_ORDER_CF).unwrap();
        let transaction_ref_neighbor_cf = self.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();
        let picker_node_chain_cf = self.db.cf_handle(PICKER_NODE_CHAIN_CF).unwrap();
        let picker_node_level_cf = self.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        let picker_neighbor_cf = self.db.cf_handle(PICK_NEIGHBOR_CF).unwrap();

        // for computing the lowest level for picker chains related to the 100 levels of originator nodes
        let mut picker_lowest: Vec<u64> = vec![];
        // get picker best blocks and levels, this is from memory
        let mut picker_longest: Vec<(H256, u64)> = vec![];
        // total picker number for each chain (using db iteration)
        let mut picker_number: Vec<u64> = vec![];

        // get the ledger tip and bottom, for processing ledger
        let ledger_tip_ = self.originator_ledger_tip.lock().unwrap();
        let ledger_tip = *ledger_tip_;
        for picker_chain in self.picker_best.iter() {
            let longest = picker_chain.lock().unwrap();
            picker_longest.push((longest.0, longest.1));
            picker_lowest.push(longest.1);
            picker_number.push(0);
        }
        // TODO: get snapshot here doesn't ensure consistency of snapshot, since we use multiple write batch in `insert_block`
        // and the ledger_tip lock doesn't ensure it either.
        let snapshot = self.db.snapshot();
        drop(ledger_tip_);
        let ledger_bottom: u64 = if ledger_tip > limit {
            ledger_tip - limit
        } else {
            0
        };

        // memory cache for picks
        let mut pick_cache: HashMap<(u16, u64), Vec<H256>> = HashMap::new();

        let mut edges: Vec<Edge> = vec![];
        let mut originator_tree: BTreeMap<u64, Vec<H256>> = BTreeMap::new();
        let mut originator_nodes: HashMap<String, Originator> = HashMap::new();
        let mut picker_nodes: HashMap<String, Picker> = HashMap::new();
        let mut originator_leaders: BTreeMap<u64, String> = BTreeMap::new();
        let mut originator_in_ledger: Vec<H256> = vec![];
        let mut transaction_in_ledger: Vec<String> = vec![];

        // originator tree
        for level in ledger_bottom.. {
            match snapshot.get_cf(originator_tree_level_cf, serialize(&level).unwrap())? {
                Some(d) => {
                    let blocks: Vec<H256> = deserialize(&d).unwrap();
                    originator_tree.insert(level, blocks);
                }
                None => break,
            }
        }

        // one pass of originator. get originator node info, cache picks.
        for (level, blocks) in originator_tree.iter() {
            for block in blocks {
                // get parent edges
                match snapshot.get_cf(parent_neighbor_cf, serialize(block).unwrap())? {
                    Some(d) => {
                        let parent: H256 = deserialize(&d).unwrap();
                        edges.push(Edge {
                            from: block.to_string(),
                            to: parent.to_string(),
                            edgetype: EdgeType::OriginatorToOriginatorParent,
                        });
                    }
                    None => {}
                }
                // get originator node info
                match snapshot.get_cf(originator_node_pick_cf, serialize(block).unwrap())? {
                    Some(d) => {
                        let picks: Vec<(u16, u64)> = deserialize(&d).unwrap();
                        originator_nodes.insert(
                            block.to_string(),
                            Originator {
                                level: *level,
                                status: OriginatorStatus::Others,
                                picks: picks.len() as u16,
                            },
                        );

                        // get picker edges
                        for (chain, level) in &picks {
                            let lowest = picker_lowest
                                .get_mut(*chain as usize)
                                .expect("should've computed lowest level");
                            if *lowest > *level {
                                *lowest = *level;
                            }

                            // cache the picks
                            if let Some(v) = pick_cache.get_mut(&(*chain, *level)) {
                                v.push(*block);
                            } else {
                                pick_cache.insert((*chain, *level), vec![*block]);
                            }
                        }
                    }
                    None => {
                        originator_nodes.insert(
                            block.to_string(),
                            Originator {
                                level: *level,
                                status: OriginatorStatus::Others,
                                picks: 0, //no picks for originator in database, so 0 pick
                            },
                        );
                    }
                }
            }
        }

        // one pass of pickers. get picker info, picker parent, and pick edges. notice the picks are cached
        for (chain, longest) in picker_longest.iter().enumerate() {
            let mut picker_block = longest.0;
            let mut level = longest.1;
            let lowest = picker_lowest
                .get(chain)
                .expect("should've computed lowest level");
            while level >= *lowest {
                // picker info
                let deepest_pick_level: u64 = match snapshot
                    .get_cf(picker_node_picked_level_cf, serialize(&picker_block).unwrap())?
                {
                    Some(d) => deserialize(&d).unwrap(),
                    None => unreachable!("picker block should have picked level in database"),
                };
                picker_nodes.insert(
                    picker_block.to_string(),
                    Picker {
                        chain: chain as u16,
                        level,
                        status: PickerStatus::OnMainChain,
                        deepest_pick_level,
                    },
                );
                // pick edges
                if let Some(picks) = pick_cache.get(&(chain as u16, level)) {
                    for pick in picks {
                        edges.push(Edge {
                            from: picker_block.to_string(),
                            to: pick.to_string(),
                            edgetype: EdgeType::PickerToOriginatorPick,
                        });
                    }
                }
                // picker parent
                match snapshot.get_cf(picker_parent_neighbor_cf, serialize(&picker_block).unwrap())? {
                    Some(d) => {
                        let parent: H256 = deserialize(&d).unwrap();
                        edges.push(Edge {
                            from: picker_block.to_string(),
                            to: parent.to_string(),
                            edgetype: EdgeType::PickerToPickerParent,
                        });
                        picker_block = parent;
                        level -= 1;
                    }
                    None => {
                        break; // if no parent, must break the while loop
                    }
                }
            }
        }

        // use iterator to find picker fork and orphan pickers, may be slow
        if display_fork {
            let iter = snapshot.iterator_cf(picker_node_chain_cf, rocksdb::IteratorMode::Start)?;
            for (k, v) in iter {
                let hash: H256 = deserialize(k.as_ref()).unwrap();
                let picker_block = hash.to_string();
                let chain: u16 = deserialize(v.as_ref()).unwrap();
                picker_number[chain as usize] += 1;
                let level: u64 = match snapshot.get_cf(picker_node_level_cf, k.as_ref())? {
                    Some(d) => deserialize(&d).unwrap(),
                    None => unreachable!("picker should have level"),
                };
                let lowest = picker_lowest
                    .get(chain as usize)
                    .expect("should've computed lowest level");
                if level >= *lowest && !picker_nodes.contains_key(&picker_block) {
                    let deepest_pick_level: u64 =
                        match snapshot.get_cf(picker_node_picked_level_cf, k.as_ref())? {
                            Some(d) => deserialize(&d).unwrap(),
                            None => unreachable!("picker block should have picked level in database"),
                        };
                    picker_nodes.insert(
                        picker_block.clone(),
                        Picker {
                            chain,
                            level,
                            status: PickerStatus::Orphan,
                            deepest_pick_level,
                        },
                    );
                    // pick edges
                    match snapshot.get_cf(picker_neighbor_cf, k.as_ref())? {
                        Some(d) => {
                            let picks: Vec<H256> = deserialize(&d).unwrap();
                            for pick in &picks {
                                edges.push(Edge {
                                    from: picker_block.clone(),
                                    to: pick.to_string(),
                                    edgetype: EdgeType::PickerToOriginatorPick,
                                });
                            }
                        }
                        None => unreachable!("picker block should have picks level in database"),
                    }
                    // picker parent
                    match snapshot.get_cf(picker_parent_neighbor_cf, k.as_ref())? {
                        Some(d) => {
                            let parent: H256 = deserialize(&d).unwrap();
                            edges.push(Edge {
                                from: picker_block.clone(),
                                to: parent.to_string(),
                                edgetype: EdgeType::PickerToPickerParent,
                            });
                        }
                        None => {}
                    }
                }
            }
        }

        // originator leader
        for level in originator_tree.keys() {
            match snapshot.get_cf(originator_leader_sequence_cf, serialize(level).unwrap())? {
                Some(d) => {
                    let h256: H256 = deserialize(&d).unwrap();
                    originator_leaders.insert(*level, h256.to_string());
                    if let Some(originator) = originator_nodes.get_mut(&h256.to_string()) {
                        originator.status = OriginatorStatus::Leader;
                    }
                }
                None => {}
            }
        }

        // ledger
        for level in ledger_bottom..=ledger_tip {
            match snapshot.get_cf(
                originator_ledger_order_cf,
                serialize(&(level as u64)).unwrap(),
            )? {
                Some(d) => {
                    let mut blocks: Vec<H256> = deserialize(&d).unwrap();
                    originator_in_ledger.append(&mut blocks);
                }
                None => {
                    unreachable!("level <= ledger tip should exist in originator_ledger_order_cf")
                }
            }
        }

        for hash in &originator_in_ledger {
            match snapshot.get_cf(transaction_ref_neighbor_cf, serialize(&hash).unwrap())? {
                Some(d) => {
                    let blocks: Vec<H256> = deserialize(&d).unwrap();
                    let mut blocks = blocks.into_iter().map(|h|h.to_string()).collect();
                    transaction_in_ledger.append(&mut blocks);
                }
                None => unreachable!("originator in ledger should have transaction ref in database (even for empty ref)"),
            }
        }

        // TODO: transaction_unreferred may be inconsistent with other things
        let transaction_unreferred_ = self.unreferred_transactions.lock().unwrap();
        let transaction_unreferred: Vec<String> = transaction_unreferred_
            .iter()
            .map(|h| h.to_string())
            .collect();
        drop(transaction_unreferred_);

        // originator number
        let mut originator_number: usize = 0;
        let mut originator_level: u64 = 0;
        for level in 0u64.. {
            match snapshot.get_cf(originator_tree_level_cf, serialize(&level).unwrap())? {
                Some(d) => {
                    let blocks: Vec<H256> = deserialize(&d).unwrap();
                    originator_number += blocks.len();
                }
                None => {
                    originator_level = level;
                    break;
                }
            }
        }
        let originator_tree_number = format!("({}/{}) ", originator_level, originator_number);

        // picker numbers
        let mut picker_chain_number: Vec<String> = vec![];
        for (chain, longest) in picker_longest.iter().enumerate() {
            if picker_number[chain] == 0 {
                picker_chain_number.push("".to_string());
            } else {
                picker_chain_number.push(format!("({}/{}) ", 1 + longest.1, picker_number[chain]));
            }
        }

        let originator_levels: Vec<Vec<String>> = originator_tree
            .into_iter()
            .map(|(_k, v)| v.into_iter().map(|h256| h256.to_string()).collect())
            .collect();
        let picker_longest: Vec<String> = picker_longest
            .into_iter()
            .map(|(h, _u)| h.to_string())
            .collect();
        let originator_in_ledger: Vec<String> = originator_in_ledger
            .into_iter()
            .map(|h| h.to_string())
            .collect();
        // filter the edges for nodes_to_show
        let mut originator_to_show: Vec<String> = originator_nodes.keys().cloned().collect();
        let mut picker_to_show: Vec<String> = picker_nodes.keys().cloned().collect();
        originator_to_show.append(&mut picker_to_show);
        let nodes_to_show: HashSet<String> = originator_to_show.into_iter().collect();
        let edges: Vec<Edge> = edges
            .into_iter()
            .filter(|e| nodes_to_show.contains(&e.from) && nodes_to_show.contains(&e.to))
            .collect();

        let dump = Dump {
            edges,
            originator_levels,
            originator_leaders,
            picker_longest,
            originator_nodes,
            picker_nodes,
            originator_in_ledger,
            transaction_in_ledger,
            transaction_unreferred,
            picker_chain_number,
            originator_tree_number,
        };

        Ok(serde_json::to_string_pretty(&dump).unwrap())
    }
}

fn pick_vec_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut rocksdb::merge_operator::MergeOperands,
) -> Option<Vec<u8>> {
    let mut existing: Vec<(u16, u64)> = match existing_val {
        Some(v) => deserialize(v).unwrap(),
        None => vec![],
    };
    for op in operands {
        // parse the operation as add(true)/remove(false), chain(u16), level(u64)
        let operation: (bool, u16, u64) = deserialize(op).unwrap();
        match operation.0 {
            true => {
                if !existing.contains(&(operation.1, operation.2)) {
                    existing.push((operation.1, operation.2));
                }
            }
            false => {
                match existing.iter().position(|&x| x.0 == operation.1) {
                    Some(p) => existing.swap_remove(p),
                    None => continue, // TODO: potential bug here - what if we delete a nonexisting item
                };
            }
        }
    }
    let result: Vec<u8> = serialize(&existing).unwrap();
    Some(result)
}

fn h256_vec_append_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut rocksdb::merge_operator::MergeOperands,
) -> Option<Vec<u8>> {
    let mut existing: Vec<H256> = match existing_val {
        Some(v) => deserialize(v).unwrap(),
        None => vec![],
    };
    for op in operands {
        let new_hash: H256 = deserialize(op).unwrap();
        if !existing.contains(&new_hash) {
            existing.push(new_hash);
        }
    }
    let result: Vec<u8> = serialize(&existing).unwrap();
    Some(result)
}

fn u64_plus_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut rocksdb::merge_operator::MergeOperands,
) -> Option<Vec<u8>> {
    let mut existing: u64 = match existing_val {
        Some(v) => deserialize(v).unwrap(),
        None => 0,
    };
    for op in operands {
        let to_add: u64 = deserialize(op).unwrap();
        existing += to_add;
    }
    let result: Vec<u8> = serialize(&existing).unwrap();
    Some(result)
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::{originator, transaction, picker, Block, Content};
    use crate::crypto::hash::H256;

    #[test]
    fn initialize_new() {
        let db = BlockChain::new("/tmp/sapa_test_blockchain_new.rocksdb").unwrap();
        // get cf handles
        let originator_node_pick_cf = db.db.cf_handle(ORIGINATOR_NODE_PICK_CF).unwrap();
        let originator_node_level_cf = db.db.cf_handle(ORIGINATOR_NODE_LEVEL_CF).unwrap();
        let picker_node_level_cf = db.db.cf_handle(PICKER_NODE_LEVEL_CF).unwrap();
        let picker_node_chain_cf = db.db.cf_handle(PICKER_NODE_CHAIN_CF).unwrap();
        let picker_node_picked_level_cf = db.db.cf_handle(PICKER_NODE_PICKED_LEVEL_CF).unwrap();
        let originator_tree_level_cf = db.db.cf_handle(ORIGINATOR_TREE_LEVEL_CF).unwrap();
        let parent_neighbor_cf = db.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let pick_neighbor_cf = db.db.cf_handle(PICK_NEIGHBOR_CF).unwrap();
        let originator_leader_sequence_cf = db.db.cf_handle(ORIGINATOR_LEADER_SEQUENCE_CF).unwrap();
        let originator_ledger_order_cf = db.db.cf_handle(ORIGINATOR_LEDGER_ORDER_CF).unwrap();

        // validate originator genesis
        let genesis_level: u64 = deserialize(
            &db.db
                .get_pinned_cf(
                    originator_node_level_cf,
                    serialize(&(*ORIGINATOR_GENESIS_HASH)).unwrap(),
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(genesis_level, 0);
        let level_0_blocks: Vec<H256> = deserialize(
            &db.db
                .get_pinned_cf(originator_tree_level_cf, serialize(&(0 as u64)).unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(level_0_blocks, vec![*ORIGINATOR_GENESIS_HASH]);
        let genesis_picks: Vec<(u16, u64)> = deserialize(
            &db.db
                .get_pinned_cf(
                    originator_node_pick_cf,
                    serialize(&(*ORIGINATOR_GENESIS_HASH)).unwrap(),
                )
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let mut true_genesis_picks: Vec<(u16, u64)> = vec![];
        for chain_num in 0..NUM_PICKER_CHAINS {
            true_genesis_picks.push((chain_num as u16, 0));
        }
        assert_eq!(genesis_picks, true_genesis_picks);
        assert_eq!(*db.originator_best_level.lock().unwrap(), 0);
        assert_eq!(*db.unconfirmed_originators.lock().unwrap(), HashSet::new());
        assert_eq!(db.unreferred_originators.lock().unwrap().len(), 1);
        assert_eq!(
            db.unreferred_originators
                .lock()
                .unwrap()
                .contains(&(ORIGINATOR_GENESIS_HASH)),
            true
        );
        let level_0_leader: H256 = deserialize(
            &db.db
                .get_pinned_cf(originator_leader_sequence_cf, serialize(&(0 as u64)).unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(level_0_leader, *ORIGINATOR_GENESIS_HASH);
        let level_0_confirms: Vec<H256> = deserialize(
            &db.db
                .get_pinned_cf(originator_ledger_order_cf, serialize(&(0 as u64)).unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(level_0_confirms, vec![*ORIGINATOR_GENESIS_HASH]);

        // validate picker genesis
        for chain_num in 0..NUM_PICKER_CHAINS {
            let genesis_level: u64 = deserialize(
                &db.db
                    .get_pinned_cf(
                        picker_node_level_cf,
                        serialize(&PICKER_GENESIS_HASHES[chain_num as usize]).unwrap(),
                    )
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(genesis_level, 0);
            let picked_level: u64 = deserialize(
                &db.db
                    .get_pinned_cf(
                        picker_node_picked_level_cf,
                        serialize(&PICKER_GENESIS_HASHES[chain_num as usize]).unwrap(),
                    )
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(picked_level, 0);
            let genesis_chain: u16 = deserialize(
                &db.db
                    .get_pinned_cf(
                        picker_node_chain_cf,
                        serialize(&PICKER_GENESIS_HASHES[chain_num as usize]).unwrap(),
                    )
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(genesis_chain, chain_num as u16);
            let parent: H256 = deserialize(
                &db.db
                    .get_pinned_cf(
                        parent_neighbor_cf,
                        serialize(&PICKER_GENESIS_HASHES[chain_num as usize]).unwrap(),
                    )
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(parent, *ORIGINATOR_GENESIS_HASH);
            let picked_originator: Vec<H256> = deserialize(
                &db.db
                    .get_pinned_cf(
                        pick_neighbor_cf,
                        serialize(&PICKER_GENESIS_HASHES[chain_num as usize]).unwrap(),
                    )
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(picked_originator, vec![*ORIGINATOR_GENESIS_HASH]);
            assert_eq!(
                *db.picker_best[chain_num as usize].lock().unwrap(),
                (PICKER_GENESIS_HASHES[chain_num as usize], 0)
            );
        }
    }

    #[test]
    fn best_originator_and_picker() {
        let db =
            BlockChain::new("/tmp/sapa_test_blockchain_best_originator_and_picker.rocksdb").unwrap();
        assert_eq!(db.best_originator().unwrap(), *ORIGINATOR_GENESIS_HASH);
        assert_eq!(db.best_picker(0), PICKER_GENESIS_HASHES[0]);

        let new_originator_content = Content::Originator(originator::Content::new(vec![], vec![]));
        let new_originator_block = Block::new(
            *ORIGINATOR_GENESIS_HASH,
            0,
            0,
            H256::default(),
            vec![],
            new_originator_content,
            [0; 32],
            H256::default(),
        );
        db.insert_block(&new_originator_block).unwrap();
        let new_picker_content = Content::Picker(picker::Content::new(
            0,
            PICKER_GENESIS_HASHES[0],
            vec![new_originator_block.hash()],
        ));
        let new_picker_block = Block::new(
            new_originator_block.hash(),
            0,
            0,
            H256::default(),
            vec![],
            new_picker_content,
            [1; 32],
            H256::default(),
        );
        db.insert_block(&new_picker_block).unwrap();
        assert_eq!(db.best_originator().unwrap(), new_originator_block.hash());
        assert_eq!(db.best_picker(0), new_picker_block.hash());
    }

    #[test]
    fn unreferred_transactions_and_originator() {
        let db =
            BlockChain::new("/tmp/sapa_test_blockchain_unreferred_transactions_originator.rocksdb")
                .unwrap();

        let new_transaction_content = Content::Transaction(transaction::Content::new(vec![]));
        let new_transaction_block = Block::new(
            *ORIGINATOR_GENESIS_HASH,
            0,
            0,
            H256::default(),
            vec![],
            new_transaction_content,
            [0; 32],
            H256::default(),
        );
        db.insert_block(&new_transaction_block).unwrap();
        assert_eq!(
            db.unreferred_transactions(),
            vec![new_transaction_block.hash()]
        );
        assert_eq!(db.unreferred_originators(), vec![*ORIGINATOR_GENESIS_HASH]);

        let new_originator_content = Content::Originator(originator::Content::new(vec![], vec![]));
        let new_originator_block_1 = Block::new(
            *ORIGINATOR_GENESIS_HASH,
            0,
            0,
            H256::default(),
            vec![],
            new_originator_content,
            [1; 32],
            H256::default(),
        );
        db.insert_block(&new_originator_block_1).unwrap();
        assert_eq!(
            db.unreferred_transactions(),
            vec![new_transaction_block.hash()]
        );
        assert_eq!(db.unreferred_originators(), vec![new_originator_block_1.hash()]);

        let new_originator_content = Content::Originator(originator::Content::new(
            vec![new_transaction_block.hash()],
            vec![new_originator_block_1.hash()],
        ));
        let new_originator_block_2 = Block::new(
            *ORIGINATOR_GENESIS_HASH,
            0,
            0,
            H256::default(),
            vec![],
            new_originator_content,
            [2; 32],
            H256::default(),
        );
        db.insert_block(&new_originator_block_2).unwrap();
        assert_eq!(db.unreferred_transactions(), vec![]);
        assert_eq!(db.unreferred_originators(), vec![new_originator_block_2.hash()]);
    }

    #[test]
    fn unpicked_originator() {
        let db = BlockChain::new("/tmp/sapa_test_blockchain_unpicked_originator.rocksdb").unwrap();
        assert_eq!(
            db.unpicked_originator(&PICKER_GENESIS_HASHES[0], &db.best_originator().unwrap())
                .unwrap(),
            vec![]
        );

        let new_originator_content = Content::Originator(originator::Content::new(vec![], vec![]));
        let new_originator_block_1 = Block::new(
            *ORIGINATOR_GENESIS_HASH,
            0,
            0,
            H256::default(),
            vec![],
            new_originator_content,
            [0; 32],
            H256::default(),
        );
        db.insert_block(&new_originator_block_1).unwrap();

        let new_originator_content = Content::Originator(originator::Content::new(vec![], vec![]));
        let new_originator_block_2 = Block::new(
            *ORIGINATOR_GENESIS_HASH,
            0,
            0,
            H256::default(),
            vec![],
            new_originator_content,
            [1; 32],
            H256::default(),
        );
        db.insert_block(&new_originator_block_2).unwrap();
        assert_eq!(
            db.unpicked_originator(&PICKER_GENESIS_HASHES[0], &db.best_originator().unwrap())
                .unwrap(),
            vec![new_originator_block_1.hash()]
        );

        let new_picker_content = Content::Picker(picker::Content::new(
            0,
            PICKER_GENESIS_HASHES[0],
            vec![new_originator_block_1.hash()],
        ));
        let new_picker_block = Block::new(
            new_originator_block_2.hash(),
            0,
            0,
            H256::default(),
            vec![],
            new_picker_content,
            [2; 32],
            H256::default(),
        );
        db.insert_block(&new_picker_block).unwrap();

        assert_eq!(
            db.unpicked_originator(&PICKER_GENESIS_HASHES[0], &db.best_originator().unwrap())
                .unwrap(),
            vec![new_originator_block_1.hash()]
        );
        assert_eq!(
            db.unpicked_originator(&new_picker_block.hash(), &db.best_originator().unwrap())
                .unwrap(),
            vec![]
        );
    }

    #[test]
    fn merge_operator_h256_vec() {
        let db = BlockChain::new("/tmp/sapa_test_blockchain_merge_op_h256_vec.rocksdb").unwrap();
        let cf = db.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();

        let hash_1: H256 = [0u8; 32].into();
        let hash_2: H256 = [1u8; 32].into();
        let hash_3: H256 = [2u8; 32].into();
        // merge with an nonexistent entry
        db.db
            .merge_cf(cf, b"testkey", serialize(&hash_1).unwrap())
            .unwrap();
        let result: Vec<H256> =
            deserialize(&db.db.get_pinned_cf(cf, b"testkey").unwrap().unwrap()).unwrap();
        assert_eq!(result, vec![hash_1]);

        // merge with an existing entry
        db.db
            .merge_cf(cf, b"testkey", serialize(&hash_2).unwrap())
            .unwrap();
        db.db
            .merge_cf(cf, b"testkey", serialize(&hash_3).unwrap())
            .unwrap();
        let result: Vec<H256> =
            deserialize(&db.db.get_pinned_cf(cf, b"testkey").unwrap().unwrap()).unwrap();
        assert_eq!(result, vec![hash_1, hash_2, hash_3]);
    }

    #[test]
    fn merge_operator_btreemap() {
        let db = BlockChain::new("/tmp/sapa_test_blockchain_merge_op_u64_vec.rocksdb").unwrap();
        let cf = db.db.cf_handle(ORIGINATOR_NODE_PICK_CF).unwrap();

        // merge with an nonexistent entry
        db.db
            .merge_cf(
                cf,
                b"testkey",
                serialize(&(true, 0 as u16, 0 as u64)).unwrap(),
            )
            .unwrap();
        let result: Vec<(u16, u64)> =
            deserialize(&db.db.get_pinned_cf(cf, b"testkey").unwrap().unwrap()).unwrap();
        assert_eq!(result, vec![(0, 0)]);

        // insert
        db.db
            .merge_cf(
                cf,
                b"testkey",
                serialize(&(true, 10 as u16, 0 as u64)).unwrap(),
            )
            .unwrap();
        db.db
            .merge_cf(
                cf,
                b"testkey",
                serialize(&(true, 5 as u16, 0 as u64)).unwrap(),
            )
            .unwrap();
        let result: Vec<(u16, u64)> =
            deserialize(&db.db.get_pinned_cf(cf, b"testkey").unwrap().unwrap()).unwrap();
        assert_eq!(result, vec![(0, 0), (10, 0), (5, 0)]);

        // remove
        db.db
            .merge_cf(
                cf,
                b"testkey",
                serialize(&(false, 5 as u16, 0 as u64)).unwrap(),
            )
            .unwrap();
        let result: Vec<(u16, u64)> =
            deserialize(&db.db.get_pinned_cf(cf, b"testkey").unwrap().unwrap()).unwrap();
        assert_eq!(result, vec![(0, 0), (10, 0)]);
    }
}
*/
