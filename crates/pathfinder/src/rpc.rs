//! StarkNet node JSON-RPC related modules.
pub mod api;
pub mod serde;
pub mod types;

use crate::{
    core::{ContractAddress, StarknetTransactionHash, StarknetTransactionIndex},
    rpc::{
        api::RpcApi,
        types::{
            request::OverflowingStorageAddress,
            request::{BlockResponseScope, Call, EventFilter},
            BlockHashOrTag, BlockNumberOrTag,
        },
    },
};
use ::serde::Deserialize;
use jsonrpsee::{
    core::Error,
    http_server::{HttpServerBuilder, HttpServerHandle, RpcModule},
};
use std::{net::SocketAddr, result::Result};

/// Helper wrapper for attaching spans to rpc method implementations
struct RpcModuleWrapper<Context>(jsonrpsee::RpcModule<Context>);

impl<Context: Send + Sync + 'static> RpcModuleWrapper<Context> {
    /// This wrapper helper adds a tracing span around all rpc methods with name = method_name.
    ///
    /// It could do more, for example trace the outputs, durations.
    ///
    /// This is the only one method provided at the moment, because it's the only one used. If you
    /// need to use some other `register_*` method from [`jsonrpsee::RpcModule`], just add it to
    /// this wrapper.
    fn register_async_method<R, Fun, Fut>(
        &mut self,
        method_name: &'static str,
        callback: Fun,
    ) -> Result<jsonrpsee::core::server::rpc_module::MethodResourcesBuilder, jsonrpsee::core::Error>
    where
        R: ::serde::Serialize + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<R, Error>> + Send,
        Fun: (Fn(jsonrpsee::types::Params<'static>, std::sync::Arc<Context>) -> Fut)
            + Copy
            + Send
            + Sync
            + 'static,
    {
        use tracing::Instrument;

        self.0.register_async_method(method_name, move |p, c| {
            // why info here? it's the same used in warp tracing filter for example.
            let span = tracing::info_span!("rpc_method", name = method_name);
            callback(p, c).instrument(span)
        })
    }

    fn into_inner(self) -> jsonrpsee::RpcModule<Context> {
        self.0
    }
}

/// Starts the HTTP-RPC server.
pub async fn run_server(
    addr: SocketAddr,
    api: RpcApi,
) -> Result<(HttpServerHandle, SocketAddr), Error> {
    let server = HttpServerBuilder::default().build(addr).await?;
    let local_addr = server.local_addr()?;
    let mut module = RpcModuleWrapper(RpcModule::new(api));
    module.register_async_method("starknet_getBlockByHash", |params, context| async move {
        #[derive(Debug, Deserialize)]
        pub struct NamedArgs {
            pub block_hash: BlockHashOrTag,
            #[serde(default)]
            pub requested_scope: Option<BlockResponseScope>,
        }
        let params = params.parse::<NamedArgs>()?;
        context
            .get_block_by_hash(params.block_hash, params.requested_scope)
            .await
    })?;
    module.register_async_method("starknet_getBlockByNumber", |params, context| async move {
        #[derive(Debug, Deserialize)]
        pub struct NamedArgs {
            pub block_number: BlockNumberOrTag,
            #[serde(default)]
            pub requested_scope: Option<BlockResponseScope>,
        }
        let params = params.parse::<NamedArgs>()?;
        context
            .get_block_by_number(params.block_number, params.requested_scope)
            .await
    })?;
    // module.register_async_method(
    //     "starknet_getStateUpdateByHash",
    //     |params, context| async move {
    //         let hash = if params.is_object() {
    //             #[derive(Debug, Deserialize)]
    //             pub struct NamedArgs {
    //                 pub block_hash: BlockHashOrTag,
    //             }
    //             params.parse::<NamedArgs>()?.block_hash
    //         } else {
    //             params.one::<BlockHashOrTag>()?
    //         };
    //         context.get_state_update_by_hash(hash).await
    //     },
    // )?;
    module.register_async_method("starknet_getStorageAt", |params, context| async move {
        #[derive(Debug, Deserialize)]
        pub struct NamedArgs {
            pub contract_address: ContractAddress,
            // Accept overflowing type here to report INVALID_STORAGE_KEY properly
            pub key: OverflowingStorageAddress,
            pub block_hash: BlockHashOrTag,
        }
        let params = params.parse::<NamedArgs>()?;
        context
            .get_storage_at(params.contract_address, params.key, params.block_hash)
            .await
    })?;
    module.register_async_method(
        "starknet_getTransactionByHash",
        |params, context| async move {
            #[derive(Debug, Deserialize)]
            pub struct NamedArgs {
                pub transaction_hash: StarknetTransactionHash,
            }
            context
                .get_transaction_by_hash(params.parse::<NamedArgs>()?.transaction_hash)
                .await
        },
    )?;
    module.register_async_method(
        "starknet_getTransactionByBlockHashAndIndex",
        |params, context| async move {
            #[derive(Debug, Deserialize)]
            pub struct NamedArgs {
                pub block_hash: BlockHashOrTag,
                pub index: StarknetTransactionIndex,
            }
            let params = params.parse::<NamedArgs>()?;
            context
                .get_transaction_by_block_hash_and_index(params.block_hash, params.index)
                .await
        },
    )?;
    module.register_async_method(
        "starknet_getTransactionByBlockNumberAndIndex",
        |params, context| async move {
            #[derive(Debug, Deserialize)]
            pub struct NamedArgs {
                pub block_number: BlockNumberOrTag,
                pub index: StarknetTransactionIndex,
            }
            let params = params.parse::<NamedArgs>()?;
            context
                .get_transaction_by_block_number_and_index(params.block_number, params.index)
                .await
        },
    )?;
    module.register_async_method(
        "starknet_getTransactionReceipt",
        |params, context| async move {
            #[derive(Debug, Deserialize)]
            pub struct NamedArgs {
                pub transaction_hash: StarknetTransactionHash,
            }
            context
                .get_transaction_receipt(params.parse::<NamedArgs>()?.transaction_hash)
                .await
        },
    )?;
    module.register_async_method("starknet_getCode", |params, context| async move {
        #[derive(Debug, Deserialize)]
        pub struct NamedArgs {
            pub contract_address: ContractAddress,
        }
        context
            .get_code(params.parse::<NamedArgs>()?.contract_address)
            .await
    })?;
    module.register_async_method(
        "starknet_getBlockTransactionCountByHash",
        |params, context| async move {
            #[derive(Debug, Deserialize)]
            pub struct NamedArgs {
                pub block_hash: BlockHashOrTag,
            }
            context
                .get_block_transaction_count_by_hash(params.parse::<NamedArgs>()?.block_hash)
                .await
        },
    )?;
    module.register_async_method(
        "starknet_getBlockTransactionCountByNumber",
        |params, context| async move {
            #[derive(Debug, Deserialize)]
            pub struct NamedArgs {
                pub block_number: BlockNumberOrTag,
            }
            context
                .get_block_transaction_count_by_number(params.parse::<NamedArgs>()?.block_number)
                .await
        },
    )?;
    module.register_async_method("starknet_call", |params, context| async move {
        #[derive(Debug, Deserialize)]
        pub struct NamedArgs {
            pub request: Call,
            pub block_hash: BlockHashOrTag,
        }
        let params = params.parse::<NamedArgs>()?;
        context.call(params.request, params.block_hash).await
    })?;
    module.register_async_method("starknet_blockNumber", |_, context| async move {
        context.block_number().await
    })?;
    module.register_async_method("starknet_chainId", |_, context| async move {
        context.chain_id().await
    })?;
    // module.register_async_method("starknet_pendingTransactions", |_, context| async move {
    //     context.pending_transactions().await
    // })?;
    // module.register_async_method("starknet_protocolVersion", |_, context| async move {
    //     context.protocol_version().await
    // })?;
    module.register_async_method("starknet_syncing", |_, context| async move {
        context.syncing().await
    })?;
    module.register_async_method("starknet_getEvents", |params, context| async move {
        #[derive(Debug, Deserialize)]
        struct NamedArgs {
            pub filter: EventFilter,
        }
        let request = params.parse::<NamedArgs>()?.filter;
        context.get_events(request).await
    })?;
    let module = module.into_inner();
    server.start(module).map(|handle| (handle, local_addr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{
            ContractAddress, ContractHash, EventData, EventKey, GlobalRoot, StarknetBlockHash,
            StarknetBlockNumber, StarknetBlockTimestamp, StarknetProtocolVersion, StorageAddress,
        },
        ethereum::Chain,
        rpc::run_server,
        sequencer::{
            reply::transaction::{
                execution_resources::{BuiltinInstanceCounter, EmptyBuiltinInstanceCounter},
                ExecutionResources, Receipt, Transaction, Type,
            },
            test_utils::*,
            Client as SeqClient,
        },
        state::{state_tree::GlobalStateTree, SyncState},
        storage::{
            ContractCodeTable, ContractsTable, StarknetBlock, StarknetBlocksTable,
            StarknetTransactionsTable, Storage,
        },
    };
    use assert_matches::assert_matches;
    use jsonrpsee::{
        core::client::ClientT as Client,
        http_client::{HttpClient, HttpClientBuilder},
        rpc_params,
        types::ParamsSer,
    };
    use pedersen::StarkHash;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        net::{Ipv4Addr, SocketAddrV4},
        sync::Arc,
        time::Duration,
    };

    /// Helper function: produces named rpc method args map.
    fn by_name<const N: usize>(params: [(&'_ str, serde_json::Value); N]) -> Option<ParamsSer<'_>> {
        Some(BTreeMap::from(params).into())
    }

    /// Helper rpc client
    fn client(addr: SocketAddr) -> HttpClient {
        HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(120))
            .build(format!("http://{}", addr))
            .expect("Failed to create HTTP-RPC client")
    }

    lazy_static::lazy_static! {
        static ref LOCALHOST: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    }

    mod error {
        lazy_static::lazy_static! {
            pub static ref CONTRACT_NOT_FOUND: (i64, String) = (20, "Contract not found".to_owned());
            pub static ref INVALID_SELECTOR: (i64, String) = (21, "Invalid message selector".to_owned());
            pub static ref INVALID_CALL_DATA: (i64, String) = (22, "Invalid call data".to_owned());
            pub static ref INVALID_KEY: (i64, String) = (23, "Invalid storage key".to_owned());
            pub static ref INVALID_BLOCK_HASH: (i64, String) = (24, "Invalid block hash".to_owned());
            pub static ref INVALID_TX_HASH: (i64, String) = (25, "Invalid transaction hash".to_owned());
            pub static ref INVALID_BLOCK_NUMBER: (i64, String) = (26, "Invalid block number".to_owned());
            pub static ref INVALID_TX_INDEX: (i64, String) = (27, "Invalid transaction index in a block".to_owned());
        }
    }

    fn get_err(json_str: &str) -> (i64, String) {
        let v: serde_json::Value = serde_json::from_str(json_str).unwrap();
        (
            v["error"]["code"].as_i64().unwrap(),
            v["error"]["message"].as_str().unwrap().to_owned(),
        )
    }

    // Local test helper
    fn setup_storage() -> Storage {
        use crate::{
            core::{Fee, StorageValue},
            ethereum::state_update::{ContractUpdate, StorageUpdate},
            state::{update_contract_state, CompressedContract},
        };
        use web3::types::H128;

        let storage = Storage::in_memory().unwrap();
        let mut connection = storage.connection().unwrap();
        let db_txn = connection.transaction().unwrap();

        let contract0_addr = ContractAddress(StarkHash::from_be_slice(b"contract 0").unwrap());
        let contract1_addr = ContractAddress(StarkHash::from_be_slice(b"contract 1").unwrap());

        let contract0_hash = ContractHash(StarkHash::from_be_slice(b"contract 0 hash").unwrap());
        let contract1_hash = ContractHash(StarkHash::from_be_slice(b"contract 1 hash").unwrap());

        let contract0_update = ContractUpdate {
            address: contract0_addr,
            storage_updates: vec![],
        };

        let storage_addr = StorageAddress(StarkHash::from_be_slice(b"storage addr 0").unwrap());
        let contract1_update0 = ContractUpdate {
            address: contract1_addr,
            storage_updates: vec![StorageUpdate {
                address: storage_addr,
                value: StorageValue(StarkHash::from_be_slice(b"storage value 0").unwrap()),
            }],
        };
        let mut contract1_update1 = contract1_update0.clone();
        contract1_update1.storage_updates.get_mut(0).unwrap().value =
            StorageValue(StarkHash::from_be_slice(b"storage value 1").unwrap());
        let mut contract1_update2 = contract1_update0.clone();
        contract1_update2.storage_updates.get_mut(0).unwrap().value =
            StorageValue(StarkHash::from_be_slice(b"storage value 2").unwrap());

        // We need to set the magic bytes for zstd compression to simulate a compressed
        // contract definition, as this is asserted for internally
        let zstd_magic = vec![0x28, 0xb5, 0x2f, 0xfd];
        let contract0_code = CompressedContract {
            abi: zstd_magic.clone(),
            bytecode: zstd_magic.clone(),
            definition: zstd_magic,
            hash: contract0_hash,
        };
        let mut contract1_code = contract0_code.clone();
        contract1_code.hash = contract1_hash;

        ContractCodeTable::insert_compressed(&db_txn, &contract0_code).unwrap();
        ContractCodeTable::insert_compressed(&db_txn, &contract1_code).unwrap();

        ContractsTable::upsert(&db_txn, contract0_addr, contract0_hash).unwrap();
        ContractsTable::upsert(&db_txn, contract1_addr, contract1_hash).unwrap();

        let mut global_tree = GlobalStateTree::load(&db_txn, GlobalRoot(StarkHash::ZERO)).unwrap();
        let contract_state_hash =
            update_contract_state(&contract0_update, &global_tree, &db_txn).unwrap();
        global_tree
            .set(contract0_addr, contract_state_hash)
            .unwrap();
        let global_root0 = global_tree.apply().unwrap();

        let mut global_tree = GlobalStateTree::load(&db_txn, global_root0).unwrap();
        let contract_state_hash =
            update_contract_state(&contract1_update0, &global_tree, &db_txn).unwrap();
        global_tree
            .set(contract1_addr, contract_state_hash)
            .unwrap();
        let contract_state_hash =
            update_contract_state(&contract1_update1, &global_tree, &db_txn).unwrap();
        global_tree
            .set(contract1_addr, contract_state_hash)
            .unwrap();
        let global_root1 = global_tree.apply().unwrap();

        let mut global_tree = GlobalStateTree::load(&db_txn, global_root1).unwrap();
        let contract_state_hash =
            update_contract_state(&contract1_update2, &global_tree, &db_txn).unwrap();
        global_tree
            .set(contract1_addr, contract_state_hash)
            .unwrap();
        let global_root2 = global_tree.apply().unwrap();

        let genesis_hash = StarknetBlockHash(StarkHash::from_be_slice(b"genesis").unwrap());
        let block0 = StarknetBlock {
            number: StarknetBlockNumber(0),
            hash: genesis_hash,
            root: global_root0,
            timestamp: StarknetBlockTimestamp(0),
        };
        let block1_hash = StarknetBlockHash(StarkHash::from_be_slice(b"block 1").unwrap());
        let block1 = StarknetBlock {
            number: StarknetBlockNumber(1),
            hash: block1_hash,
            root: global_root1,
            timestamp: StarknetBlockTimestamp(0),
        };
        let latest_hash = StarknetBlockHash(StarkHash::from_be_slice(b"latest").unwrap());
        let block2 = StarknetBlock {
            number: StarknetBlockNumber(2),
            hash: latest_hash,
            root: global_root2,
            timestamp: StarknetBlockTimestamp(0),
        };
        StarknetBlocksTable::insert(&db_txn, &block0).unwrap();
        StarknetBlocksTable::insert(&db_txn, &block1).unwrap();
        StarknetBlocksTable::insert(&db_txn, &block2).unwrap();

        let txn0_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap());
        let txn0 = Transaction {
            calldata: None,
            class_hash: None,
            constructor_calldata: None,
            contract_address: contract0_addr,
            contract_address_salt: None,
            entry_point_type: None,
            entry_point_selector: None,
            max_fee: Some(Fee(H128::zero())),
            signature: None,
            transaction_hash: txn0_hash,
            r#type: Type::Deploy,
        };
        let receipt0 = Receipt {
            actual_fee: None,
            events: vec![],
            execution_resources: ExecutionResources {
                builtin_instance_counter: BuiltinInstanceCounter::Empty(
                    EmptyBuiltinInstanceCounter {},
                ),
                n_memory_holes: 0,
                n_steps: 0,
            },
            l1_to_l2_consumed_message: None,
            l2_to_l1_messages: vec![],
            transaction_hash: txn0_hash,
            transaction_index: StarknetTransactionIndex(0),
        };
        let txn1_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 1").unwrap());
        let txn2_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 2").unwrap());
        let txn3_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 3").unwrap());
        let txn4_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 4 ").unwrap());
        let txn5_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 5").unwrap());
        let mut txn1 = txn0.clone();
        let mut txn2 = txn0.clone();
        let mut txn3 = txn0.clone();
        let mut txn4 = txn0.clone();
        txn1.transaction_hash = txn1_hash;
        txn1.contract_address = contract1_addr;
        txn2.transaction_hash = txn2_hash;
        txn2.contract_address = contract1_addr;
        txn3.transaction_hash = txn3_hash;
        txn3.contract_address = contract1_addr;
        txn4.transaction_hash = txn4_hash;

        txn4.contract_address = ContractAddress(StarkHash::ZERO);
        let mut txn5 = txn4.clone();
        txn5.transaction_hash = txn5_hash;
        let mut receipt1 = receipt0.clone();
        let mut receipt2 = receipt0.clone();
        let mut receipt3 = receipt0.clone();
        let mut receipt4 = receipt0.clone();
        let mut receipt5 = receipt0.clone();
        receipt1.transaction_hash = txn1_hash;
        receipt2.transaction_hash = txn2_hash;
        receipt3.transaction_hash = txn3_hash;
        receipt4.transaction_hash = txn4_hash;
        receipt5.transaction_hash = txn5_hash;
        let transaction_data0 = [(txn0, receipt0)];
        let transaction_data1 = [(txn1, receipt1), (txn2, receipt2)];
        let transaction_data2 = [(txn3, receipt3), (txn4, receipt4), (txn5, receipt5)];
        StarknetTransactionsTable::upsert(&db_txn, block0.hash, block0.number, &transaction_data0)
            .unwrap();
        StarknetTransactionsTable::upsert(&db_txn, block1.hash, block1.number, &transaction_data1)
            .unwrap();
        StarknetTransactionsTable::upsert(&db_txn, block2.hash, block2.number, &transaction_data2)
            .unwrap();

        db_txn.commit().unwrap();
        storage
    }

    mod get_block_by_hash {
        use super::*;
        use crate::core::{StarknetBlockHash, StarknetBlockNumber};
        use crate::rpc::types::{
            reply::{Block, Transactions},
            request::BlockResponseScope,
            BlockHashOrTag, Tag,
        };
        use pedersen::StarkHash;
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn genesis() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let genesis_hash = StarknetBlockHash(StarkHash::from_be_slice(b"genesis").unwrap());
            let params = rpc_params!(genesis_hash);
            let block = client(addr)
                .request::<Block>("starknet_getBlockByHash", params)
                .await
                .unwrap();
            assert_eq!(block.block_hash, Some(genesis_hash));
            assert_eq!(block.block_number, Some(StarknetBlockNumber(0)));
            assert_matches!(
                block.transactions,
                Transactions::HashesOnly(t) => assert_eq!(t.len(), 1)
            );
        }

        mod latest {
            use super::*;

            mod positional_args {
                use super::*;
                use pretty_assertions::assert_eq;

                #[tokio::test]
                async fn all() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let latest_hash =
                        StarknetBlockHash(StarkHash::from_be_slice(b"latest").unwrap());
                    let params = rpc_params!(
                        BlockHashOrTag::Tag(Tag::Latest),
                        BlockResponseScope::FullTransactions
                    );
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByHash", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_hash, Some(latest_hash));
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_matches!(
                        block.transactions,
                        Transactions::Full(t) => assert_eq!(t.len(), 3)
                    );
                }

                #[tokio::test]
                async fn only_mandatory() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let latest_hash =
                        StarknetBlockHash(StarkHash::from_be_slice(b"latest").unwrap());
                    let params = rpc_params!(BlockHashOrTag::Tag(Tag::Latest));
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByHash", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_hash, Some(latest_hash));
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_matches!(
                        block.transactions,
                        Transactions::HashesOnly(t) => assert_eq!(t.len(), 3)
                    );
                }
            }

            mod named_args {
                use super::*;
                use pretty_assertions::assert_eq;
                use serde_json::json;

                #[tokio::test]
                async fn all() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let latest_hash =
                        StarknetBlockHash(StarkHash::from_be_slice(b"latest").unwrap());
                    let params = by_name([
                        ("block_hash", json!("latest")),
                        ("requested_scope", json!("FULL_TXN_AND_RECEIPTS")),
                    ]);
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByHash", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_hash, Some(latest_hash));
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_matches!(
                        block.transactions,
                        Transactions::FullWithReceipts(t) => assert_eq!(t.len(), 3)
                    );
                }

                #[tokio::test]
                async fn only_mandatory() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let latest_hash =
                        StarknetBlockHash(StarkHash::from_be_slice(b"latest").unwrap());
                    let params = by_name([("block_hash", json!("latest"))]);
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByHash", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_hash, Some(latest_hash));
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_matches!(
                        block.transactions,
                        Transactions::HashesOnly(t) => assert_eq!(t.len(), 3)
                    );
                }
            }
        }

        #[tokio::test]
        async fn pending() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                BlockHashOrTag::Tag(Tag::Pending),
                BlockResponseScope::FullTransactions
            );
            let block = client(addr)
                .request::<Block>("starknet_getBlockByHash", params)
                .await
                .unwrap();
            assert_matches!(
                block.transactions,
                Transactions::Full(_) => ()
            );
        }

        #[tokio::test]
        async fn invalid_block_hash() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(StarknetBlockHash(StarkHash::ZERO));
            let error = client(addr)
                .request::<Block>("starknet_getBlockByHash", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(&s.error().to_string(), *error::INVALID_BLOCK_HASH)
            );
        }
    }

    mod get_block_by_number {
        use super::*;
        use crate::rpc::types::{
            reply::{Block, Transactions},
            request::BlockResponseScope,
            BlockNumberOrTag, Tag,
        };
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn genesis() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(StarknetBlockNumber(0));
            let block = client(addr)
                .request::<Block>("starknet_getBlockByNumber", params)
                .await
                .unwrap();
            assert_eq!(block.block_number, Some(StarknetBlockNumber(0)));
            assert_matches!(
                block.transactions,
                Transactions::HashesOnly(t) => assert_eq!(t.len(), 1)
            );
        }

        mod latest {
            use super::*;

            mod positional_args {
                use super::*;
                use pretty_assertions::assert_eq;

                #[tokio::test]
                async fn all() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let params = rpc_params!(
                        BlockNumberOrTag::Tag(Tag::Latest),
                        BlockResponseScope::FullTransactions
                    );
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByNumber", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_matches!(
                        block.transactions,
                        Transactions::Full(t) => assert_eq!(t.len(), 3)
                    );
                }

                #[tokio::test]
                async fn only_mandatory() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let params = rpc_params!(BlockNumberOrTag::Tag(Tag::Latest));
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByNumber", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_matches!(
                        block.transactions,
                        Transactions::HashesOnly(t) => assert_eq!(t.len(), 3)
                    );
                }
            }

            mod named_args {
                use super::*;
                use pretty_assertions::assert_eq;
                use serde_json::json;

                #[tokio::test]
                async fn all() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let params = by_name([
                        ("block_number", json!("latest")),
                        ("requested_scope", json!("FULL_TXN_AND_RECEIPTS")),
                    ]);
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByNumber", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_eq!(
                        block.block_hash,
                        Some(StarknetBlockHash(
                            StarkHash::from_be_slice(b"latest").unwrap()
                        ))
                    );
                    assert_matches!(
                        block.transactions,
                        Transactions::FullWithReceipts(t) => assert_eq!(t.len(), 3)
                    );
                }

                #[tokio::test]
                async fn only_mandatory() {
                    let storage = setup_storage();
                    let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let params = by_name([("block_number", json!("latest"))]);
                    let block = client(addr)
                        .request::<Block>("starknet_getBlockByNumber", params)
                        .await
                        .unwrap();
                    assert_eq!(block.block_number, Some(StarknetBlockNumber(2)));
                    assert_eq!(
                        block.block_hash,
                        Some(StarknetBlockHash(
                            StarkHash::from_be_slice(b"latest").unwrap()
                        ))
                    );
                    assert_matches!(
                        block.transactions,
                        Transactions::HashesOnly(t) => assert_eq!(t.len(), 3)
                    );
                }
            }
        }

        #[tokio::test]
        async fn pending() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                BlockNumberOrTag::Tag(Tag::Pending),
                BlockResponseScope::FullTransactions
            );
            let block = client(addr)
                .request::<Block>("starknet_getBlockByNumber", params)
                .await
                .unwrap();
            assert_matches!(
                block.transactions,
                Transactions::Full(_) => ()
            );
        }

        #[tokio::test]
        async fn invalid_number() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(StarknetBlockNumber(123));
            let error = client(addr)
                .request::<Block>("starknet_getBlockByNumber", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_NUMBER)
            );
        }
    }

    mod get_state_update_by_hash {
        use super::*;
        use crate::rpc::types::{reply::StateUpdate, BlockHashOrTag, Tag};

        #[tokio::test]
        #[should_panic]
        async fn genesis() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(*GENESIS_BLOCK_HASH);
            client(addr)
                .request::<StateUpdate>("starknet_getStateUpdateByHash", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn latest() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(BlockHashOrTag::Tag(Tag::Latest));
            client(addr)
                .request::<StateUpdate>("starknet_getStateUpdateByHash", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        #[should_panic]
        async fn pending() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(BlockHashOrTag::Tag(Tag::Pending));
            client(addr)
                .request::<StateUpdate>("starknet_getStateUpdateByHash", params)
                .await
                .unwrap();
        }
    }

    mod get_storage_at {
        use super::*;
        use crate::{
            core::StorageValue,
            rpc::types::{BlockHashOrTag, Tag},
        };
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn key_is_field_modulus() {
            use std::str::FromStr;

            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                ContractAddress(StarkHash::from_be_slice(b"contract 0").unwrap()),
                web3::types::H256::from_str(
                    "0x0800000000000011000000000000000000000000000000000000000000000001"
                )
                .unwrap(),
                BlockHashOrTag::Tag(Tag::Latest)
            );
            let error = client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_KEY)
            );
        }

        #[tokio::test]
        async fn key_is_less_than_modulus_but_252_bits() {
            use std::str::FromStr;

            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                ContractAddress(StarkHash::from_be_slice(b"contract 0").unwrap()),
                web3::types::H256::from_str(
                    "0x0800000000000000000000000000000000000000000000000000000000000000"
                )
                .unwrap(),
                BlockHashOrTag::Tag(Tag::Latest)
            );
            let error = client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_KEY)
            );
        }

        #[tokio::test]
        async fn non_existent_contract_address() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                ContractAddress(StarkHash::from_be_slice(b"nonexistent").unwrap()),
                StorageAddress(StarkHash::from_be_slice(b"storage addr 0").unwrap()),
                BlockHashOrTag::Tag(Tag::Latest)
            );
            let error = client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::CONTRACT_NOT_FOUND)
            );
        }

        #[tokio::test]
        async fn pre_deploy_block_hash() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                ContractAddress(StarkHash::from_be_slice(b"contract 1").unwrap()),
                StorageAddress(StarkHash::from_be_slice(b"storage addr 0").unwrap()),
                BlockHashOrTag::Hash(StarknetBlockHash(
                    StarkHash::from_be_slice(b"genesis").unwrap()
                ))
            );
            let error = client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::CONTRACT_NOT_FOUND)
            );
        }

        #[tokio::test]
        async fn non_existent_block_hash() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                ContractAddress(StarkHash::from_be_slice(b"contract 1").unwrap()),
                StorageAddress(StarkHash::from_be_slice(b"storage addr 0").unwrap()),
                BlockHashOrTag::Hash(StarknetBlockHash(
                    StarkHash::from_be_slice(b"nonexistent").unwrap()
                ))
            );
            let error = client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_HASH)
            );
        }

        #[tokio::test]
        async fn deployment_block() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                ContractAddress(StarkHash::from_be_slice(b"contract 1").unwrap()),
                StorageAddress(StarkHash::from_be_slice(b"storage addr 0").unwrap()),
                BlockHashOrTag::Hash(StarknetBlockHash(
                    StarkHash::from_be_slice(b"block 1").unwrap()
                ))
            );
            let value = client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap();
            assert_eq!(
                value.0,
                StarkHash::from_be_slice(b"storage value 1").unwrap()
            );
        }

        mod latest_block {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(
                    ContractAddress(StarkHash::from_be_slice(b"contract 1").unwrap()),
                    StorageAddress(StarkHash::from_be_slice(b"storage addr 0").unwrap()),
                    BlockHashOrTag::Tag(Tag::Latest)
                );
                let value = client(addr)
                    .request::<StorageValue>("starknet_getStorageAt", params)
                    .await
                    .unwrap();
                assert_eq!(
                    value.0,
                    StarkHash::from_be_slice(b"storage value 2").unwrap()
                );
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([
                    (
                        "contract_address",
                        json! {StarkHash::from_be_slice(b"contract 1").unwrap()},
                    ),
                    (
                        "key",
                        json! {StarkHash::from_be_slice(b"storage addr 0").unwrap()},
                    ),
                    ("block_hash", json! {"latest"}),
                ]);
                let value = client(addr)
                    .request::<StorageValue>("starknet_getStorageAt", params)
                    .await
                    .unwrap();
                assert_eq!(
                    value.0,
                    StarkHash::from_be_slice(b"storage value 2").unwrap()
                );
            }
        }

        #[tokio::test]
        async fn pending_block() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                *VALID_CONTRACT_ADDR,
                *VALID_KEY,
                BlockHashOrTag::Tag(Tag::Pending)
            );
            client(addr)
                .request::<StorageValue>("starknet_getStorageAt", params)
                .await
                .unwrap();
        }
    }

    mod get_transaction_by_hash {
        use super::*;
        use crate::rpc::types::reply::Transaction;
        use pretty_assertions::assert_eq;

        mod accepted {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap());
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(hash);
                let transaction = client(addr)
                    .request::<Transaction>("starknet_getTransactionByHash", params)
                    .await
                    .unwrap();
                assert_eq!(transaction.txn_hash, hash);
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap());
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([("transaction_hash", json!(hash))]);
                let transaction = client(addr)
                    .request::<Transaction>("starknet_getTransactionByHash", params)
                    .await
                    .unwrap();
                assert_eq!(transaction.txn_hash, hash);
            }
        }

        #[tokio::test]
        async fn invalid_hash() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(*INVALID_TX_HASH);
            let error = client(addr)
                .request::<Transaction>("starknet_getTransactionByHash", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_TX_HASH)
            );
        }
    }

    mod get_transaction_by_block_hash_and_index {
        use super::*;
        use crate::rpc::types::{reply::Transaction, BlockHashOrTag, Tag};
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn genesis() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let genesis_hash = StarknetBlockHash(StarkHash::from_be_slice(b"genesis").unwrap());
            let params = rpc_params!(genesis_hash, 0);
            let txn = client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockHashAndIndex", params)
                .await
                .unwrap();
            assert_eq!(
                txn.txn_hash,
                StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap())
            )
        }

        mod latest {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(BlockHashOrTag::Tag(Tag::Latest), 0);
                let txn = client(addr)
                    .request::<Transaction>("starknet_getTransactionByBlockHashAndIndex", params)
                    .await
                    .unwrap();
                assert_eq!(
                    txn.txn_hash,
                    StarknetTransactionHash(StarkHash::from_be_slice(b"txn 3").unwrap())
                );
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([("block_hash", json!("latest")), ("index", json!(0))]);
                let txn = client(addr)
                    .request::<Transaction>("starknet_getTransactionByBlockHashAndIndex", params)
                    .await
                    .unwrap();
                assert_eq!(
                    txn.txn_hash,
                    StarknetTransactionHash(StarkHash::from_be_slice(b"txn 3").unwrap())
                );
            }
        }

        #[tokio::test]
        async fn pending() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(BlockHashOrTag::Tag(Tag::Pending), 0);
            client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockHashAndIndex", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn invalid_block() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(StarknetBlockHash(StarkHash::ZERO), 0);
            let error = client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockHashAndIndex", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_HASH)
            );
        }

        #[tokio::test]
        async fn invalid_transaction_index() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let genesis_hash = StarknetBlockHash(StarkHash::from_be_slice(b"genesis").unwrap());
            let params = rpc_params!(genesis_hash, 123);
            let error = client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockHashAndIndex", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_TX_INDEX)
            );
        }
    }

    mod get_transaction_by_block_number_and_index {
        use super::*;
        use crate::rpc::types::{reply::Transaction, BlockNumberOrTag, Tag};
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn genesis() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(0, 0);
            let txn = client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockNumberAndIndex", params)
                .await
                .unwrap();
            assert_eq!(
                txn.txn_hash,
                StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap())
            );
        }

        mod latest {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(BlockNumberOrTag::Tag(Tag::Latest), 0);
                let txn = client(addr)
                    .request::<Transaction>("starknet_getTransactionByBlockNumberAndIndex", params)
                    .await
                    .unwrap();
                assert_eq!(
                    txn.txn_hash,
                    StarknetTransactionHash(StarkHash::from_be_slice(b"txn 3").unwrap())
                );
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([("block_number", json!("latest")), ("index", json!(0))]);
                let txn = client(addr)
                    .request::<Transaction>("starknet_getTransactionByBlockNumberAndIndex", params)
                    .await
                    .unwrap();
                assert_eq!(
                    txn.txn_hash,
                    StarknetTransactionHash(StarkHash::from_be_slice(b"txn 3").unwrap())
                );
            }
        }

        #[tokio::test]
        async fn pending() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(BlockNumberOrTag::Tag(Tag::Pending), 0);
            client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockNumberAndIndex", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn invalid_block() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(123, 0);
            let error = client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockNumberAndIndex", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_NUMBER)
            );
        }

        #[tokio::test]
        async fn invalid_transaction_index() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(0, 123);
            let error = client(addr)
                .request::<Transaction>("starknet_getTransactionByBlockNumberAndIndex", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_TX_INDEX)
            );
        }
    }

    mod get_transaction_receipt {
        use super::*;
        use crate::rpc::types::reply::TransactionReceipt;
        use pretty_assertions::assert_eq;

        mod accepted {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let txn_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap());
                let params = rpc_params!(txn_hash);
                let receipt = client(addr)
                    .request::<TransactionReceipt>("starknet_getTransactionReceipt", params)
                    .await
                    .unwrap();
                assert_eq!(receipt.txn_hash, txn_hash);
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let txn_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"txn 0").unwrap());
                let params = by_name([("transaction_hash", json!(txn_hash))]);
                let receipt = client(addr)
                    .request::<TransactionReceipt>("starknet_getTransactionReceipt", params)
                    .await
                    .unwrap();
                assert_eq!(receipt.txn_hash, txn_hash);
            }
        }

        #[tokio::test]
        async fn invalid() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let txn_hash = StarknetTransactionHash(StarkHash::from_be_slice(b"not found").unwrap());
            let params = rpc_params!(txn_hash);
            let error = client(addr)
                .request::<TransactionReceipt>("starknet_getTransactionReceipt", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_TX_HASH)
            );
        }
    }

    mod get_code {
        use super::*;
        use crate::core::ContractCode;
        use crate::rpc::types::reply::ErrorCode;
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn invalid_contract_address() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(*INVALID_CONTRACT_ADDR);
            let error = client(addr)
                .request::<ContractCode>("starknet_getCode", params)
                .await
                .unwrap_err();
            assert_eq!(ErrorCode::ContractNotFound, error);
        }

        #[tokio::test]
        async fn returns_not_found_if_we_dont_know_about_the_contract() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

            let not_found = client(addr)
                .request::<ContractCode>(
                    "starknet_getCode",
                    rpc_params!(
                        "0x4ae0618c330c59559a59a27d143dd1c07cd74cf4e5e5a7cd85d53c6bf0e89dc"
                    ),
                )
                .await
                .unwrap_err();

            assert_eq!(ErrorCode::ContractNotFound, not_found);
        }

        #[tokio::test]
        async fn returns_abi_and_code_for_known() {
            use crate::core::ContractCode;
            use anyhow::Context;
            use bytes::Bytes;
            use futures::stream::TryStreamExt;
            use pedersen::StarkHash;

            let storage = Storage::in_memory().unwrap();

            let contract_definition = include_bytes!("../fixtures/contract_definition.json.zst");
            let buffer = zstd::decode_all(std::io::Cursor::new(contract_definition)).unwrap();
            let contract_definition = Bytes::from(buffer);

            {
                let mut conn = storage.connection().unwrap();
                let tx = conn.transaction().unwrap();

                let address = StarkHash::from_hex_str(
                    "057dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e374",
                )
                .unwrap();
                let expected_hash = StarkHash::from_hex_str(
                    "050b2148c0d782914e0b12a1a32abe5e398930b7e914f82c65cb7afce0a0ab9b",
                )
                .unwrap();

                let (abi, bytecode, hash) =
                    crate::state::contract_hash::extract_abi_code_hash(&*contract_definition)
                        .unwrap();

                assert_eq!(hash.0, expected_hash);

                crate::storage::ContractCodeTable::insert(
                    &tx,
                    hash,
                    &abi,
                    &bytecode,
                    &contract_definition,
                )
                .context("Deploy testing contract")
                .unwrap();

                crate::storage::ContractsTable::upsert(
                    &tx,
                    crate::core::ContractAddress(address),
                    hash,
                )
                .unwrap();

                tx.commit().unwrap();
            }

            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

            let client = client(addr);

            // both parameters, these used to be separate tests
            let rets = [
                rpc_params!("0x057dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e374"),
                by_name([(
                    "contract_address",
                    json!("0x057dde83c18c0efe7123c36a52d704cf27d5c38cdf0b1e1edc3b0dae3ee4e374"),
                )]),
            ]
            .into_iter()
            .map(|arg| client.request::<ContractCode>("starknet_getCode", arg))
            .collect::<futures::stream::FuturesOrdered<_>>()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

            assert_eq!(rets.len(), 2);

            assert_eq!(rets[0], rets[1]);
            let abi = rets[0].abi.to_string();
            assert_eq!(
                abi,
                // this should not have the quotes because that'd be in json:
                // `"abi":"\"[{....}]\""`
                r#"[{"inputs":[{"name":"address","type":"felt"},{"name":"value","type":"felt"}],"name":"increase_value","outputs":[],"type":"function"},{"inputs":[{"name":"contract_address","type":"felt"},{"name":"address","type":"felt"},{"name":"value","type":"felt"}],"name":"call_increase_value","outputs":[],"type":"function"},{"inputs":[{"name":"address","type":"felt"}],"name":"get_value","outputs":[{"name":"res","type":"felt"}],"type":"function"}]"#
            );
            assert_eq!(rets[0].bytecode.len(), 132);
        }
    }

    mod get_block_transaction_count_by_hash {
        use super::*;
        use crate::rpc::types::{BlockHashOrTag, Tag};
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn genesis() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(StarknetBlockHash(
                StarkHash::from_be_slice(b"genesis").unwrap()
            ));
            let count = client(addr)
                .request::<u64>("starknet_getBlockTransactionCountByHash", params)
                .await
                .unwrap();
            assert_eq!(count, 1);
        }

        mod latest {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(BlockHashOrTag::Tag(Tag::Latest));
                let count = client(addr)
                    .request::<u64>("starknet_getBlockTransactionCountByHash", params)
                    .await
                    .unwrap();
                assert_eq!(count, 3);
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([("block_hash", json!("latest"))]);
                let count = client(addr)
                    .request::<u64>("starknet_getBlockTransactionCountByHash", params)
                    .await
                    .unwrap();
                assert_eq!(count, 3);
            }
        }

        #[tokio::test]
        async fn pending() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(BlockHashOrTag::Tag(Tag::Pending));
            client(addr)
                .request::<u64>("starknet_getBlockTransactionCountByHash", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn invalid() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(StarknetBlockHash(StarkHash::ZERO));
            let error = client(addr)
                .request::<u64>("starknet_getBlockTransactionCountByHash", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_HASH)
            );
        }
    }

    mod get_block_transaction_count_by_number {
        use super::*;
        use crate::rpc::types::{BlockNumberOrTag, Tag};
        use pretty_assertions::assert_eq;

        #[tokio::test]
        async fn genesis() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(0);
            let count = client(addr)
                .request::<u64>("starknet_getBlockTransactionCountByNumber", params)
                .await
                .unwrap();
            assert_eq!(count, 1);
        }

        mod latest {
            use super::*;
            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn positional_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(BlockNumberOrTag::Tag(Tag::Latest));
                let count = client(addr)
                    .request::<u64>("starknet_getBlockTransactionCountByNumber", params)
                    .await
                    .unwrap();
                assert_eq!(count, 3);
            }

            #[tokio::test]
            async fn named_args() {
                let storage = setup_storage();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([("block_number", json!("latest"))]);
                let count = client(addr)
                    .request::<u64>("starknet_getBlockTransactionCountByNumber", params)
                    .await
                    .unwrap();
                assert_eq!(count, 3);
            }
        }

        #[tokio::test]
        async fn pending() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(BlockNumberOrTag::Tag(Tag::Pending));
            client(addr)
                .request::<u64>("starknet_getBlockTransactionCountByNumber", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn invalid() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(123);
            let error = client(addr)
                .request::<u64>("starknet_getBlockTransactionCountByNumber", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_NUMBER)
            );
        }
    }

    mod call {
        use super::*;
        use crate::{
            core::{CallParam, CallResultValue},
            rpc::types::{request::Call, BlockHashOrTag, Tag},
        };
        use pretty_assertions::assert_eq;

        lazy_static::lazy_static! {
            static ref CALL_DATA: Vec<CallParam> = vec![CallParam::from_hex_str("1234").unwrap()];
        }

        #[tokio::test]
        async fn latest_invoked_block() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: CALL_DATA.clone(),
                    contract_address: *VALID_CONTRACT_ADDR,
                    entry_point_selector: *VALID_ENTRY_POINT,
                },
                *INVOKE_CONTRACT_BLOCK_HASH
            );
            client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap();
        }

        mod latest_block {
            use super::*;

            #[tokio::test]
            async fn positional_args() {
                let storage = Storage::in_memory().unwrap();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = rpc_params!(
                    Call {
                        calldata: CALL_DATA.clone(),
                        contract_address: *VALID_CONTRACT_ADDR,
                        entry_point_selector: *VALID_ENTRY_POINT,
                    },
                    BlockHashOrTag::Tag(Tag::Latest)
                );
                client(addr)
                    .request::<Vec<CallResultValue>>("starknet_call", params)
                    .await
                    .unwrap();
            }

            #[tokio::test]
            async fn named_args() {
                let storage = Storage::in_memory().unwrap();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                let params = by_name([
                    (
                        "request",
                        json!({
                            "calldata": CALL_DATA.clone(),
                            "contract_address": *VALID_CONTRACT_ADDR,
                            "entry_point_selector": *VALID_ENTRY_POINT,
                        }),
                    ),
                    ("block_hash", json!("latest")),
                ]);
                client(addr)
                    .request::<Vec<CallResultValue>>("starknet_call", params)
                    .await
                    .unwrap();
            }
        }

        #[tokio::test]
        async fn pending_block() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: CALL_DATA.clone(),
                    contract_address: *VALID_CONTRACT_ADDR,
                    entry_point_selector: *VALID_ENTRY_POINT,
                },
                BlockHashOrTag::Tag(Tag::Pending)
            );
            client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn invalid_entry_point() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: CALL_DATA.clone(),
                    contract_address: *VALID_CONTRACT_ADDR,
                    entry_point_selector: *INVALID_ENTRY_POINT,
                },
                BlockHashOrTag::Tag(Tag::Latest)
            );
            let error = client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_SELECTOR)
            );
        }

        #[tokio::test]
        async fn invalid_contract_address() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: CALL_DATA.clone(),
                    contract_address: *INVALID_CONTRACT_ADDR,
                    entry_point_selector: *VALID_ENTRY_POINT,
                },
                BlockHashOrTag::Tag(Tag::Latest)
            );
            let error = client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::CONTRACT_NOT_FOUND)
            );
        }

        #[tokio::test]
        async fn invalid_call_data() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: vec![],
                    contract_address: *VALID_CONTRACT_ADDR,
                    entry_point_selector: *VALID_ENTRY_POINT,
                },
                BlockHashOrTag::Tag(Tag::Latest)
            );
            let error = client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_CALL_DATA)
            );
        }

        #[tokio::test]
        async fn uninitialized_contract() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: CALL_DATA.clone(),
                    contract_address: *VALID_CONTRACT_ADDR,
                    entry_point_selector: *VALID_ENTRY_POINT,
                },
                *PRE_DEPLOY_CONTRACT_BLOCK_HASH
            );
            let error = client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(s.error.to_call_error(), *error::CONTRACT_NOT_FOUND)
            );
        }

        #[tokio::test]
        async fn invalid_block_hash() {
            let storage = Storage::in_memory().unwrap();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let params = rpc_params!(
                Call {
                    calldata: CALL_DATA.clone(),
                    contract_address: *VALID_CONTRACT_ADDR,
                    entry_point_selector: *VALID_ENTRY_POINT,
                },
                *INVALID_BLOCK_HASH
            );
            let error = client(addr)
                .request::<Vec<CallResultValue>>("starknet_call", params)
                .await
                .unwrap_err();
            assert_matches!(
                error,
                Error::Call(s) => assert_eq!(get_err(&s), *error::INVALID_BLOCK_HASH)
            );
        }
    }

    #[tokio::test]
    async fn block_number() {
        let storage = setup_storage();
        let sequencer = SeqClient::new(Chain::Goerli).unwrap();
        let sync_state = Arc::new(SyncState::default());
        let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
        let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
        let number = client(addr)
            .request::<u64>("starknet_blockNumber", rpc_params!())
            .await
            .unwrap();
        assert_eq!(number, 2);
    }

    #[tokio::test]
    async fn chain_id() {
        use futures::stream::StreamExt;

        assert_eq!(
            [Chain::Goerli, Chain::Mainnet]
                .iter()
                .map(|set_chain| async {
                    let storage = Storage::in_memory().unwrap();
                    let sequencer = SeqClient::new(*set_chain).unwrap();
                    let sync_state = Arc::new(SyncState::default());
                    let api = RpcApi::new(storage, sequencer, *set_chain, sync_state);
                    let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
                    let params = rpc_params!();
                    client(addr)
                        .request::<String>("starknet_chainId", params)
                        .await
                        .unwrap()
                })
                .collect::<futures::stream::FuturesOrdered<_>>()
                .collect::<Vec<_>>()
                .await,
            vec![
                format!("0x{}", hex::encode("SN_GOERLI")),
                format!("0x{}", hex::encode("SN_MAIN")),
            ]
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn pending_transactions() {
        let storage = Storage::in_memory().unwrap();
        let sequencer = SeqClient::new(Chain::Goerli).unwrap();
        let sync_state = Arc::new(SyncState::default());
        let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
        let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
        client(addr)
            .request::<()>("starknet_pendingTransactions", rpc_params!())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn protocol_version() {
        let storage = Storage::in_memory().unwrap();
        let sequencer = SeqClient::new(Chain::Goerli).unwrap();
        let sync_state = Arc::new(SyncState::default());
        let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
        let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
        client(addr)
            .request::<StarknetProtocolVersion>("starknet_protocolVersion", rpc_params!())
            .await
            .unwrap();
    }

    mod syncing {
        use crate::rpc::types::reply::{syncing, Syncing};
        use pretty_assertions::assert_eq;

        use super::*;

        #[tokio::test]
        async fn not_syncing() {
            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let syncing = client(addr)
                .request::<Syncing>("starknet_syncing", rpc_params!())
                .await
                .unwrap();

            assert_eq!(syncing, Syncing::False(false));
        }

        #[tokio::test]
        async fn syncing() {
            let expected = Syncing::Status(syncing::Status {
                starting_block: StarknetBlockHash(StarkHash::from_be_slice(b"starting").unwrap()),
                current_block: StarknetBlockHash(StarkHash::from_be_slice(b"current").unwrap()),
                highest_block: StarknetBlockHash(StarkHash::from_be_slice(b"highest").unwrap()),
            });

            let storage = setup_storage();
            let sequencer = SeqClient::new(Chain::Goerli).unwrap();
            let sync_state = Arc::new(SyncState::default());
            *sync_state.status.write().await = expected.clone();
            let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
            let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();
            let syncing = client(addr)
                .request::<Syncing>("starknet_syncing", rpc_params!())
                .await
                .unwrap();

            assert_eq!(syncing, expected);
        }
    }

    mod events {
        use super::*;

        use super::types::reply::{EmittedEvent, GetEventsResult};
        use crate::sequencer::reply::transaction;

        const NUM_BLOCKS: usize = 4;

        fn create_blocks() -> [StarknetBlock; NUM_BLOCKS] {
            (0..NUM_BLOCKS as u64)
                .map(|i| StarknetBlock {
                    number: StarknetBlockNumber::GENESIS + i,
                    hash: StarknetBlockHash(
                        StarkHash::from_hex_str(&"a".repeat(i as usize + 3)).unwrap(),
                    ),
                    root: GlobalRoot(StarkHash::from_hex_str(&"f".repeat(i as usize + 3)).unwrap()),
                    timestamp: StarknetBlockTimestamp(i + 500),
                })
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        }

        const TRANSACTIONS_PER_BLOCK: usize = 10;
        const EVENTS_PER_BLOCK: usize = TRANSACTIONS_PER_BLOCK;
        const NUM_TRANSACTIONS: usize = NUM_BLOCKS * TRANSACTIONS_PER_BLOCK;
        const NUM_EVENTS: usize = NUM_BLOCKS * EVENTS_PER_BLOCK;

        fn create_transactions_and_receipts(
        ) -> [(transaction::Transaction, transaction::Receipt); NUM_TRANSACTIONS] {
            let transactions = (0..NUM_TRANSACTIONS).map(|i| transaction::Transaction {
                calldata: None,
                class_hash: None,
                constructor_calldata: None,
                contract_address: ContractAddress(
                    StarkHash::from_hex_str(&"2".repeat(i + 3)).unwrap(),
                ),
                contract_address_salt: None,
                entry_point_type: None,
                entry_point_selector: None,
                signature: None,
                transaction_hash: StarknetTransactionHash(
                    StarkHash::from_hex_str(&"f".repeat(i + 3)).unwrap(),
                ),
                r#type: transaction::Type::InvokeFunction,
                max_fee: None,
            });
            let receipts = (0..NUM_TRANSACTIONS).map(|i| transaction::Receipt {
                actual_fee: None,
                events: vec![transaction::Event {
                    from_address: ContractAddress(
                        StarkHash::from_hex_str(&"2".repeat(i + 3)).unwrap(),
                    ),
                    data: vec![EventData(
                        StarkHash::from_hex_str(&"c".repeat(i + 3)).unwrap(),
                    )],
                    keys: vec![
                        EventKey(StarkHash::from_hex_str(&"d".repeat(i + 3)).unwrap()),
                        EventKey(StarkHash::from_hex_str("deadbeef").unwrap()),
                    ],
                }],
                execution_resources: transaction::ExecutionResources {
                    builtin_instance_counter:
                        transaction::execution_resources::BuiltinInstanceCounter::Empty(
                            transaction::execution_resources::EmptyBuiltinInstanceCounter {},
                        ),
                    n_steps: i as u64 + 987,
                    n_memory_holes: i as u64 + 1177,
                },
                l1_to_l2_consumed_message: None,
                l2_to_l1_messages: Vec::new(),
                transaction_hash: StarknetTransactionHash(
                    StarkHash::from_hex_str(&"e".repeat(i + 3)).unwrap(),
                ),
                transaction_index: StarknetTransactionIndex(i as u64 + 2311),
            });

            transactions
                .into_iter()
                .zip(receipts)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        }

        fn setup() -> (Storage, Vec<EmittedEvent>) {
            let storage = Storage::in_memory().unwrap();
            let connection = storage.connection().unwrap();

            let blocks = create_blocks();
            let transactions_and_receipts = create_transactions_and_receipts();

            for (i, block) in blocks.iter().enumerate() {
                StarknetBlocksTable::insert(&connection, block).unwrap();
                StarknetTransactionsTable::upsert(
                    &connection,
                    block.hash,
                    block.number,
                    &transactions_and_receipts
                        [i * TRANSACTIONS_PER_BLOCK..(i + 1) * TRANSACTIONS_PER_BLOCK],
                )
                .unwrap();
            }

            let events = transactions_and_receipts
                .iter()
                .enumerate()
                .map(|(i, (txn, receipt))| {
                    let event = &receipt.events[0];
                    let block = &blocks[i / TRANSACTIONS_PER_BLOCK];

                    EmittedEvent {
                        data: event.data.clone(),
                        from_address: event.from_address,
                        keys: event.keys.clone(),
                        block_hash: block.hash,
                        block_number: block.number,
                        transaction_hash: txn.transaction_hash,
                    }
                })
                .collect();

            (storage, events)
        }

        mod positional_args {
            use super::*;

            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn get_events_with_empty_filter() {
                let (storage, events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                let params = rpc_params!(EventFilter {
                    from_block: None,
                    to_block: None,
                    address: None,
                    keys: vec![],
                    page_size: NUM_EVENTS,
                    page_number: 0,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();

                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events,
                        page_number: 0,
                        is_last_page: true,
                    }
                );
            }

            #[tokio::test]
            async fn get_events_with_fully_specified_filter() {
                let (storage, events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                let expected_event = &events[1];
                let params = rpc_params!(EventFilter {
                    from_block: Some(expected_event.block_number),
                    to_block: Some(expected_event.block_number),
                    address: Some(expected_event.from_address),
                    // we're using a key which is present in _all_ events
                    keys: vec![EventKey(StarkHash::from_hex_str("deadbeef").unwrap())],
                    page_size: NUM_EVENTS,
                    page_number: 0,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();

                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: vec![expected_event.clone()],
                        page_number: 0,
                        is_last_page: true,
                    }
                );
            }

            #[tokio::test]
            async fn get_events_by_block() {
                let (storage, events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                const BLOCK_NUMBER: usize = 2;
                let params = rpc_params!(EventFilter {
                    from_block: Some(StarknetBlockNumber(BLOCK_NUMBER as u64)),
                    to_block: Some(StarknetBlockNumber(BLOCK_NUMBER as u64)),
                    address: None,
                    keys: vec![],
                    page_size: NUM_EVENTS,
                    page_number: 0,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();

                let expected_events =
                    &events[EVENTS_PER_BLOCK * BLOCK_NUMBER..EVENTS_PER_BLOCK * (BLOCK_NUMBER + 1)];
                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: expected_events.to_vec(),
                        page_number: 0,
                        is_last_page: true,
                    }
                );
            }

            #[tokio::test]
            async fn get_events_with_invalid_page_size() {
                let (storage, _events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                let params = rpc_params!(EventFilter {
                    from_block: None,
                    to_block: None,
                    address: None,
                    keys: vec![],
                    page_size: crate::storage::StarknetEventsTable::PAGE_SIZE_LIMIT + 1,
                    page_number: 0,
                });
                let error = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap_err();
                assert_matches!(
                    error,
                    Error::Call(s) => assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&s).unwrap()["error"],
                        json!({
                            "code": 31,
                            "message": "Requested page size is too big",
                            "data": {
                                "max_page_size": crate::storage::StarknetEventsTable::PAGE_SIZE_LIMIT
                            }
                        })
                    )
                );
            }

            #[tokio::test]
            async fn get_events_by_key_with_paging() {
                let (storage, events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                let expected_events = &events[27..32];
                let keys_for_expected_events: Vec<_> =
                    expected_events.iter().map(|e| e.keys[0]).collect();

                let params = rpc_params!(EventFilter {
                    from_block: None,
                    to_block: None,
                    address: None,
                    keys: keys_for_expected_events.clone(),
                    page_size: 2,
                    page_number: 0,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();
                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: expected_events[..2].to_vec(),
                        page_number: 0,
                        is_last_page: false,
                    }
                );

                let params = rpc_params!(EventFilter {
                    from_block: None,
                    to_block: None,
                    address: None,
                    keys: keys_for_expected_events.clone(),
                    page_size: 2,
                    page_number: 1,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();
                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: expected_events[2..4].to_vec(),
                        page_number: 1,
                        is_last_page: false,
                    }
                );

                let params = rpc_params!(EventFilter {
                    from_block: None,
                    to_block: None,
                    address: None,
                    keys: keys_for_expected_events.clone(),
                    page_size: 2,
                    page_number: 2,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();
                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: expected_events[4..].to_vec(),
                        page_number: 2,
                        is_last_page: true,
                    }
                );

                // nonexistent page
                let params = rpc_params!(EventFilter {
                    from_block: None,
                    to_block: None,
                    address: None,
                    keys: keys_for_expected_events.clone(),
                    page_size: 2,
                    page_number: 3,
                });
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();
                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: vec![],
                        page_number: 3,
                        is_last_page: true,
                    }
                );
            }
        }

        mod named_args {
            use super::*;

            use pretty_assertions::assert_eq;

            #[tokio::test]
            async fn get_events_with_empty_filter() {
                let (storage, events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                let params =
                    by_name([("filter", json!({"page_size": NUM_EVENTS, "page_number": 0}))]);
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();

                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events,
                        page_number: 0,
                        is_last_page: true,
                    }
                );
            }

            #[tokio::test]
            async fn get_events_with_fully_specified_filter() {
                let (storage, events) = setup();
                let sequencer = SeqClient::new(Chain::Goerli).unwrap();
                let sync_state = Arc::new(SyncState::default());
                let api = RpcApi::new(storage, sequencer, Chain::Goerli, sync_state);
                let (__handle, addr) = run_server(*LOCALHOST, api).await.unwrap();

                let expected_event = &events[1];
                let params = by_name([(
                    "filter",
                    json!({
                        "fromBlock": expected_event.block_number.0,
                        "toBlock": expected_event.block_number.0,
                        "address": expected_event.from_address,
                        "keys": [expected_event.keys[0]],
                        "page_size": NUM_EVENTS,
                        "page_number": 0,
                    }),
                )]);
                let rpc_result = client(addr)
                    .request::<GetEventsResult>("starknet_getEvents", params)
                    .await
                    .unwrap();

                assert_eq!(
                    rpc_result,
                    GetEventsResult {
                        events: vec![expected_event.clone()],
                        page_number: 0,
                        is_last_page: true,
                    }
                );
            }
        }
    }
}
