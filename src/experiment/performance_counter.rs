use crate::block::Block;
use crate::block::Content as BlockContent;

use crate::transaction::Transaction;
use crate::wallet::WalletError;
use log::debug;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::time::SystemTime;

lazy_static! {
    pub static ref PERFORMANCE_COUNTER: Counter = { Counter::default() };
}

pub trait PayloadSize {
    fn size(&self) -> usize;
}

#[derive(Default)]
pub struct Counter {
    generated_transactions: AtomicUsize,
    generated_transaction_bytes: AtomicUsize,
    generate_transaction_failures: AtomicUsize,
    confirmed_transactions: AtomicUsize,
    confirmed_transaction_bytes: AtomicUsize,
    deconfirmed_transactions: AtomicUsize,
    deconfirmed_transaction_bytes: AtomicUsize,
    confirmed_transaction_blocks: AtomicUsize,
    deconfirmed_transaction_blocks: AtomicUsize,
    processed_originator_blocks: AtomicUsize,
    processed_originator_block_bytes: AtomicUsize,
    processed_picker_blocks: AtomicUsize,
    processed_picker_block_bytes: AtomicUsize,
    processed_transaction_blocks: AtomicUsize,
    processed_transaction_block_bytes: AtomicUsize,
    mined_originator_blocks: AtomicUsize,
    mined_originator_block_bytes: AtomicUsize,
    mined_picker_blocks: AtomicUsize,
    mined_picker_block_bytes: AtomicUsize,
    mined_transaction_blocks: AtomicUsize,
    mined_transaction_block_bytes: AtomicUsize,
    total_originator_block_delay: AtomicUsize,
    total_picker_block_delay: AtomicUsize,
    total_transaction_block_delay: AtomicUsize,
    total_originator_block_squared_delay: AtomicUsize,
    total_picker_block_squared_delay: AtomicUsize,
    total_transaction_block_squared_delay: AtomicUsize,
    received_originator_blocks: AtomicUsize,
    received_picker_blocks: AtomicUsize,
    received_transaction_blocks: AtomicUsize,
    incoming_message_queue: AtomicIsize,
    total_transaction_block_confirmation_latency: AtomicUsize,
    total_transaction_block_squared_confirmation_latency: AtomicUsize,
    originator_main_chain_length: AtomicUsize,
    picker_main_chain_length_sum: AtomicIsize,
}

#[derive(Serialize)]
pub struct Snapshot {
    pub generated_transactions: usize,
    pub generated_transaction_bytes: usize,
    pub generate_transaction_failures: usize,
    pub confirmed_transactions: usize,
    pub confirmed_transaction_bytes: usize,
    pub deconfirmed_transactions: usize,
    pub deconfirmed_transaction_bytes: usize,
    pub confirmed_transaction_blocks: usize,
    pub deconfirmed_transaction_blocks: usize,
    pub processed_originator_blocks: usize,
    pub processed_originator_block_bytes: usize,
    pub processed_picker_blocks: usize,
    pub processed_picker_block_bytes: usize,
    pub processed_transaction_blocks: usize,
    pub processed_transaction_block_bytes: usize,
    pub mined_originator_blocks: usize,
    pub mined_originator_block_bytes: usize,
    pub mined_picker_blocks: usize,
    pub mined_picker_block_bytes: usize,
    pub mined_transaction_blocks: usize,
    pub mined_transaction_block_bytes: usize,
    pub total_originator_block_delay: usize,
    pub total_picker_block_delay: usize,
    pub total_transaction_block_delay: usize,
    pub total_originator_block_squared_delay: usize,
    pub total_picker_block_squared_delay: usize,
    pub total_transaction_block_squared_delay: usize,
    pub received_originator_blocks: usize,
    pub received_picker_blocks: usize,
    pub received_transaction_blocks: usize,
    pub incoming_message_queue: isize,
    pub total_transaction_block_confirmation_latency: usize,
    pub total_transaction_block_squared_confirmation_latency: usize,
    pub originator_main_chain_length: usize,
    pub picker_main_chain_length_sum: isize,
}

impl Counter {
    pub fn record_process_message(&self) {
        self.incoming_message_queue.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_receive_message(&self) {
        self.incoming_message_queue.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_receive_block(&self, b: &Block) {
        let mined_time = b.header.timestamp;
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let delay = if current_time <= mined_time {
            0
        } else {
            current_time - mined_time
        };
        match b.content {
            BlockContent::Transaction(_) => {
                debug!("Received Transaction block, delay={} ms", delay);
                self.total_transaction_block_delay
                    .fetch_add(delay as usize, Ordering::Relaxed);
                self.total_transaction_block_squared_delay
                    .fetch_add((delay * delay) as usize, Ordering::Relaxed);
                self.received_transaction_blocks
                    .fetch_add(1, Ordering::Relaxed);
            }
            BlockContent::Originator(_) => {
                debug!("Received Originator block, delay={} ms", delay);
                self.total_originator_block_delay
                    .fetch_add(delay as usize, Ordering::Relaxed);
                self.total_originator_block_squared_delay
                    .fetch_add((delay * delay) as usize, Ordering::Relaxed);
                self.received_originator_blocks
                    .fetch_add(1, Ordering::Relaxed);
            }
            BlockContent::Picker(_) => {
                debug!("Received Picker block, delay={} ms", delay);
                self.total_picker_block_delay
                    .fetch_add(delay as usize, Ordering::Relaxed);
                self.total_picker_block_squared_delay
                    .fetch_add((delay * delay) as usize, Ordering::Relaxed);
                self.received_picker_blocks.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_process_block(&self, b: &Block) {
        match b.content {
            BlockContent::Transaction(_) => {
                self.processed_transaction_blocks
                    .fetch_add(1, Ordering::Relaxed);
                self.processed_transaction_block_bytes
                    .fetch_add(b.size(), Ordering::Relaxed);
            }
            BlockContent::Picker(_) => {
                self.processed_picker_blocks.fetch_add(1, Ordering::Relaxed);
                self.processed_picker_block_bytes
                    .fetch_add(b.size(), Ordering::Relaxed);
            }
            BlockContent::Originator(_) => {
                self.processed_originator_blocks
                    .fetch_add(1, Ordering::Relaxed);
                self.processed_originator_block_bytes
                    .fetch_add(b.size(), Ordering::Relaxed);
            }
        }
    }

    pub fn record_mine_block(&self, b: &Block) {
        match b.content {
            BlockContent::Transaction(_) => {
                self.mined_transaction_blocks
                    .fetch_add(1, Ordering::Relaxed);
                self.mined_transaction_block_bytes
                    .fetch_add(b.size(), Ordering::Relaxed);
            }
            BlockContent::Picker(_) => {
                self.mined_picker_blocks.fetch_add(1, Ordering::Relaxed);
                self.mined_picker_block_bytes
                    .fetch_add(b.size(), Ordering::Relaxed);
            }
            BlockContent::Originator(_) => {
                self.mined_originator_blocks.fetch_add(1, Ordering::Relaxed);
                self.mined_originator_block_bytes
                    .fetch_add(b.size(), Ordering::Relaxed);
            }
        }
    }

    pub fn record_update_originator_main_chain(&self, new_height: usize) {
        self.originator_main_chain_length
            .store(new_height, Ordering::Relaxed);
    }

    pub fn record_update_picker_main_chain(&self, prev_height: usize, new_height: usize) {
        if prev_height <= new_height {
            let diff: isize = (new_height - prev_height) as isize;
            self.picker_main_chain_length_sum
                .fetch_add(diff, Ordering::Relaxed);
        } else {
            let diff: isize = (prev_height - new_height) as isize;
            self.picker_main_chain_length_sum
                .fetch_sub(diff, Ordering::Relaxed);
        }
    }

    pub fn record_confirm_transaction_block(&self, b: &Block) {
        let mined_time = b.header.timestamp;
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let delay = if current_time <= mined_time {
            0
        } else {
            current_time - mined_time
        };
        self.total_transaction_block_confirmation_latency
            .fetch_add(delay as usize, Ordering::Relaxed);
        self.total_transaction_block_squared_confirmation_latency
            .fetch_add((delay * delay) as usize, Ordering::Relaxed);
        self.confirmed_transaction_blocks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_deconfirm_transaction_blocks(&self, num_blocks: usize) {
        self.deconfirmed_transaction_blocks
            .fetch_add(num_blocks, Ordering::Relaxed);
    }

    pub fn record_confirm_transaction(&self, t: &Transaction) {
        self.confirmed_transactions.fetch_add(1, Ordering::Relaxed);
        self.confirmed_transaction_bytes
            .fetch_add(t.size(), Ordering::Relaxed);
    }

    pub fn record_deconfirm_transaction(&self, t: &Transaction) {
        self.deconfirmed_transactions
            .fetch_add(1, Ordering::Relaxed);
        self.deconfirmed_transaction_bytes
            .fetch_add(t.size(), Ordering::Relaxed);
    }

    pub fn record_generate_transaction(&self, t: &Result<Transaction, WalletError>) {
        match t {
            Ok(t) => {
                self.generated_transactions.fetch_add(1, Ordering::Relaxed);
                self.generated_transaction_bytes
                    .fetch_add(t.size(), Ordering::Relaxed);
            }
            Err(_) => {
                self.generate_transaction_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let incoming_message_queue = self.incoming_message_queue.load(Ordering::Relaxed);
        let incoming_message_queue = if incoming_message_queue < 0 {
            0
        } else {
            incoming_message_queue
        };

        let picker_main_chain_length_sum = self.picker_main_chain_length_sum.load(Ordering::Relaxed);
        let picker_main_chain_length_sum = if picker_main_chain_length_sum < 0 {
            0
        } else {
            picker_main_chain_length_sum
        };
        Snapshot {
            generated_transactions: self.generated_transactions.load(Ordering::Relaxed),
            generated_transaction_bytes: self.generated_transaction_bytes.load(Ordering::Relaxed),
            generate_transaction_failures: self
                .generate_transaction_failures
                .load(Ordering::Relaxed),
            confirmed_transactions: self.confirmed_transactions.load(Ordering::Relaxed),
            confirmed_transaction_bytes: self.confirmed_transaction_bytes.load(Ordering::Relaxed),
            deconfirmed_transactions: self.deconfirmed_transactions.load(Ordering::Relaxed),
            deconfirmed_transaction_bytes: self
                .deconfirmed_transaction_bytes
                .load(Ordering::Relaxed),
            confirmed_transaction_blocks: self.confirmed_transaction_blocks.load(Ordering::Relaxed),
            deconfirmed_transaction_blocks: self
                .deconfirmed_transaction_blocks
                .load(Ordering::Relaxed),
            processed_originator_blocks: self.processed_originator_blocks.load(Ordering::Relaxed),
            processed_originator_block_bytes: self
                .processed_originator_block_bytes
                .load(Ordering::Relaxed),
            processed_picker_blocks: self.processed_picker_blocks.load(Ordering::Relaxed),
            processed_picker_block_bytes: self.processed_picker_block_bytes.load(Ordering::Relaxed),
            processed_transaction_blocks: self.processed_transaction_blocks.load(Ordering::Relaxed),
            processed_transaction_block_bytes: self
                .processed_transaction_block_bytes
                .load(Ordering::Relaxed),
            mined_originator_blocks: self.mined_originator_blocks.load(Ordering::Relaxed),
            mined_originator_block_bytes: self.mined_originator_block_bytes.load(Ordering::Relaxed),
            mined_picker_blocks: self.mined_picker_blocks.load(Ordering::Relaxed),
            mined_picker_block_bytes: self.mined_picker_block_bytes.load(Ordering::Relaxed),
            mined_transaction_blocks: self.mined_transaction_blocks.load(Ordering::Relaxed),
            mined_transaction_block_bytes: self
                .mined_transaction_block_bytes
                .load(Ordering::Relaxed),
            total_originator_block_delay: self.total_originator_block_delay.load(Ordering::Relaxed),
            total_picker_block_delay: self.total_picker_block_delay.load(Ordering::Relaxed),
            total_transaction_block_delay: self
                .total_transaction_block_delay
                .load(Ordering::Relaxed),
            total_originator_block_squared_delay: self
                .total_originator_block_squared_delay
                .load(Ordering::Relaxed),
            total_picker_block_squared_delay: self
                .total_picker_block_squared_delay
                .load(Ordering::Relaxed),
            total_transaction_block_squared_delay: self
                .total_transaction_block_squared_delay
                .load(Ordering::Relaxed),
            received_originator_blocks: self.received_originator_blocks.load(Ordering::Relaxed),
            received_picker_blocks: self.received_picker_blocks.load(Ordering::Relaxed),
            received_transaction_blocks: self.received_transaction_blocks.load(Ordering::Relaxed),
            incoming_message_queue,
            total_transaction_block_confirmation_latency: self
                .total_transaction_block_confirmation_latency
                .load(Ordering::Relaxed),
            total_transaction_block_squared_confirmation_latency: self
                .total_transaction_block_squared_confirmation_latency
                .load(Ordering::Relaxed),
            originator_main_chain_length: self.originator_main_chain_length.load(Ordering::Relaxed),
            picker_main_chain_length_sum,
        }
    }
}
