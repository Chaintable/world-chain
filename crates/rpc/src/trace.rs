use std::sync::Arc;

use alloy_consensus::BlockHeader as _;
use alloy_consensus::transaction::TxHashRef as _;
use alloy_rpc_types_eth::{BlockId, Header, TransactionInfo};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee_core::RpcResult;
use op_alloy_consensus::OpReceipt;
use op_alloy_rpc_types::OpTransactionReceipt;
use reth_chainspec::EthChainSpec as _;
use reth_evm::{evm::EvmFactoryExt, revm::context_interface::Block as _, ConfigureEvm, InspectorFor};
use reth_primitives_traits::{Block, BlockBody, RecoveredBlock};
use reth_rpc_eth_api::{
    helpers::{EthBlocks, LoadReceipt, Trace},
    EthApiTypes,
};
use reth_rpc_eth_types::{cache::db::StateCacheDb, EthApiError};
use revm_bytecode::opcode::OpCode;
use revm_inspectors::tracing::{OpcodeFilter, TracingInspector, TracingInspectorConfig};

use crate::debank::{
    build_debank_traces, build_genesis_txs_and_traces, build_storage_diff_from_bundle,
    get_storage_contracts_from_bundle, get_storage_contracts_from_genesis, BlockFile,
    BlockStorageDiff, DebankBlock, DebankOutPut, DebankTransaction,
};

#[rpc(server, namespace = "trace")]
pub trait DebankTraceApi {
    #[method(name = "debankBlock")]
    async fn trace_debank_block(&self, block_id: BlockId) -> RpcResult<DebankOutPut>;
}

pub struct DebankTraceApi<Eth> {
    eth: Arc<Eth>,
}

impl<Eth> DebankTraceApi<Eth> {
    pub fn new(eth: Eth) -> Self {
        Self { eth: Arc::new(eth) }
    }
}

impl<Eth> DebankTraceApi<Eth>
where
    Eth: Trace
        + EthBlocks
        + LoadReceipt
        + EthApiTypes<NetworkTypes: reth_rpc_eth_api::RpcTypes<Receipt = OpTransactionReceipt>>
        + Clone
        + 'static,
    Eth::Evm: ConfigureEvm,
    for<'a> TracingInspector: InspectorFor<Eth::Evm, &'a mut StateCacheDb>,
{
    pub async fn trace_debank_block_inner(
        &self,
        block_id: BlockId,
    ) -> Result<DebankOutPut, Eth::Error> {
        let eth = &*self.eth;

        let block = eth.recovered_block(block_id).await?;
        let Some(block) = block else {
            return Err(EthApiError::HeaderNotFound(block_id).into());
        };

        let debank_block: DebankBlock = block.as_ref().into();
        let debank_header = build_debank_header(&block);

        if block.number() == 0 {
            use reth_chainspec::ChainSpecProvider;
            let chain_spec = eth.provider().chain_spec();
            let genesis = chain_spec.genesis();
            let mut state_diff: BlockStorageDiff = genesis.into();
            state_diff.hash = block.state_root();
            let (transactions, traces) = build_genesis_txs_and_traces(genesis);
            let block_file = BlockFile {
                block: debank_block,
                transactions,
                traces,
                storage_contracts: get_storage_contracts_from_genesis(genesis),
                ..Default::default()
            };
            let validation_hash = block_file.validation().validation_hash;
            return Ok(DebankOutPut {
                block_file,
                header: debank_header,
                state_diff: alloy_rlp::encode(state_diff).into(),
                validation_hash,
            });
        }

        let receipts = eth.block_receipts(block_id).await?;
        let Some(receipts) = receipts else {
            return Err(EthApiError::HeaderNotFound(block_id).into());
        };

        let mut debank_txs: Vec<DebankTransaction> =
            Vec::with_capacity(block.body().transactions().len());
        for (index, tx) in block.body().transactions().iter().enumerate() {
            let receipt = &receipts[index];
            let (deposit_nonce, l1_fee) = extract_op_receipt_fields(receipt);
            debank_txs.push((receipt, tx, deposit_nonce, l1_fee).into());
        }

        let parent_block = eth.recovered_block(block.parent_hash().into()).await?;
        let Some(parent_block) = parent_block else {
            return Err(EthApiError::HeaderNotFound(block_id).into());
        };

        let mut block_file =
            BlockFile { block: debank_block, transactions: debank_txs, ..Default::default() };

        if parent_block.state_root() == block.state_root() {
            let state_diff = BlockStorageDiff {
                hash: block.state_root(),
                parent_hash: parent_block.state_root(),
                ..Default::default()
            };
            let validation_hash = block_file.validation().validation_hash;
            return Ok(DebankOutPut {
                block_file,
                header: debank_header,
                state_diff: alloy_rlp::encode(state_diff).into(),
                validation_hash,
            });
        }

        let block_state_root = block.state_root();
        let parent_state_root = parent_block.state_root();
        let parent_hash = block.parent_hash();
        let (evm_env, _) = eth.evm_env_at(block_id).await?;
        let block_arc = Arc::new(block);

        let (mut traces, mut state_diff, change_addresses) = eth
            .spawn_with_state_at_block(parent_hash, move |this, mut db| {
                this.apply_pre_execution_changes(&block_arc, &mut db, &evm_env)?;

                let block_hash = block_arc.hash();
                let block_number = evm_env.block_env.number().saturating_to::<u64>();
                let base_fee = evm_env.block_env.basefee();
                let log_index = std::cell::RefCell::new(0);
                let mut idx = 0u64;

                let mut trace_cfg = TracingInspectorConfig::default_parity()
                    .set_steps(true)
                    .set_record_logs(true)
                    .set_exclude_precompile_calls(false);
                trace_cfg.record_opcodes_filter = Some(OpcodeFilter::new().enabled(OpCode::SSTORE));

                let results: Vec<_> = this
                    .evm_config()
                    .evm_factory()
                    .create_tracer(&mut db, evm_env, TracingInspector::new(trace_cfg))
                    .try_trace_many(block_arc.transactions_recovered(), |mut ctx| {
                        let tx_info = TransactionInfo {
                            hash: Some(*ctx.tx.tx_hash()),
                            index: Some(idx),
                            block_hash: Some(block_hash),
                            block_number: Some(block_number),
                            base_fee: Some(base_fee),
                            ..Default::default()
                        };
                        idx += 1;
                        Ok::<_, Eth::Error>(build_debank_traces(
                            tx_info.hash.unwrap(),
                            ctx.take_inspector().into_traces(),
                            &log_index,
                        ))
                    })
                    .collect::<Result<_, _>>()?;

                let bundle = db.take_bundle();
                let bundle_state: Vec<(alloy_primitives::Address, Option<revm_database::BundleAccount>)> =
                    bundle.state.into_iter().map(|(addr, acc)| (addr, Some(acc))).collect();
                let storage_contracts = get_storage_contracts_from_bundle(
                    bundle_state.iter().map(|(addr, acc)| (addr, acc)),
                );
                let diff = build_storage_diff_from_bundle(bundle_state.into_iter(), |_| None);

                Ok((results, diff, storage_contracts))
            })
            .await?;

        for (trace, error_trace, event, error_event) in traces.drain(..) {
            block_file.traces.extend(trace);
            block_file.error_traces.extend(error_trace);
            block_file.events.extend(event);
            block_file.error_events.extend(error_event);
        }
        state_diff.hash = block_state_root;
        state_diff.parent_hash = parent_state_root;
        block_file.storage_contracts = change_addresses;

        let validation_hash = block_file.validation().validation_hash;
        Ok(DebankOutPut {
            block_file,
            header: debank_header,
            state_diff: alloy_rlp::encode(state_diff).into(),
            validation_hash,
        })
    }
}

fn build_debank_header<B: Block>(block: &RecoveredBlock<B>) -> Header
where
    B::Header: alloy_consensus::BlockHeader,
{
    Header {
        inner: alloy_consensus::Header {
            parent_hash: block.header().parent_hash(),
            ommers_hash: block.header().ommers_hash(),
            beneficiary: block.header().beneficiary(),
            state_root: block.header().state_root(),
            transactions_root: block.header().transactions_root(),
            receipts_root: block.header().receipts_root(),
            logs_bloom: block.header().logs_bloom(),
            difficulty: block.header().difficulty(),
            number: block.header().number(),
            gas_limit: block.header().gas_limit(),
            gas_used: block.header().gas_used(),
            timestamp: block.header().timestamp(),
            extra_data: block.header().extra_data().clone(),
            mix_hash: block.header().mix_hash().unwrap_or_default(),
            nonce: block.header().nonce().unwrap_or_default(),
            base_fee_per_gas: block.header().base_fee_per_gas(),
            withdrawals_root: block.header().withdrawals_root(),
            blob_gas_used: block.header().blob_gas_used(),
            excess_blob_gas: block.header().excess_blob_gas(),
            parent_beacon_block_root: block.header().parent_beacon_block_root(),
            requests_hash: block.header().requests_hash(),
        },
        hash: block.hash(),
        total_difficulty: None,
        size: None,
    }
}

fn extract_op_receipt_fields(receipt: &OpTransactionReceipt) -> (Option<u64>, Option<u128>) {
    let deposit_nonce = match &receipt.inner.inner.receipt {
        OpReceipt::Deposit(d) => d.deposit_nonce,
        _ => None,
    };
    let l1_fee = receipt.l1_block_info.l1_fee;
    (deposit_nonce, l1_fee)
}

#[async_trait::async_trait]
impl<Eth> DebankTraceApiServer for DebankTraceApi<Eth>
where
    Eth: Trace
        + EthBlocks
        + LoadReceipt
        + EthApiTypes<NetworkTypes: reth_rpc_eth_api::RpcTypes<Receipt = OpTransactionReceipt>>
        + Clone
        + 'static,
    Eth::Evm: ConfigureEvm,
    for<'a> TracingInspector: InspectorFor<Eth::Evm, &'a mut StateCacheDb>,
{
    async fn trace_debank_block(&self, block_id: BlockId) -> RpcResult<DebankOutPut> {
        Ok(Self::trace_debank_block_inner(self, block_id).await.map_err(Into::into)?)
    }
}

impl<Eth> std::fmt::Debug for DebankTraceApi<Eth> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebankTraceApi").finish_non_exhaustive()
    }
}
