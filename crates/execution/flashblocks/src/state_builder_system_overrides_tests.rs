use super::PendingStateBuilder;
use alloy_consensus::Block;
use alloy_consensus::Header;
use alloy_eips::eip2935::HISTORY_STORAGE_ADDRESS;
use alloy_eips::eip2935::HISTORY_STORAGE_CODE;
use alloy_eips::eip4788::BEACON_ROOTS_ADDRESS;
use alloy_eips::eip4788::BEACON_ROOTS_CODE;
use alloy_evm::overrides::apply_state_overrides;
use alloy_primitives::Address;
use alloy_primitives::B256;
use alloy_primitives::Bytes;
use alloy_primitives::TxKind;
use alloy_primitives::U256;
use alloy_primitives::address;
use alloy_rpc_types_eth::state::StateOverride;
use base_common_evm::L1BlockInfo;
use base_execution_chainspec::BaseChainSpecBuilder;
use base_execution_evm::BaseEvmConfig;
use reth_evm::ConfigureEvm;
use revm::Context;
use revm::Database;
use revm::DatabaseCommit;
use revm::ExecuteEvm;
use revm::MainBuilder;
use revm::MainContext;
use revm::context::TxEnv;
use revm::database::InMemoryDB;
use revm::database::State;
use revm::primitives::hardfork::SpecId;
use revm::state::AccountInfo;
use revm::state::Bytecode;
use revm::state::EvmState;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

type TestResult = Result<(), Box<dyn Error>>;

// Real Base mainnet fork times: Canyon installation is historical, while both system calls
// are active at this post-Isthmus timestamp.
const CANYON_TIMESTAMP: u64 = 1_704_992_401;
const SYSTEM_TIMESTAMP: u64 = 1_780_000_000;
const CREATE2_DEPLOYER: Address = address!("13b0D85CcB8bf860b6b79AF3029fCA081AE9beF2");

fn prepare_system_block(
    database: State<InMemoryDB>,
    overrides: StateOverride,
    header: Header,
    beacon_root: Option<B256>,
) -> Result<(State<InMemoryDB>, StateOverride), Box<dyn Error>> {
    let chain_spec = Arc::new(BaseChainSpecBuilder::base_mainnet().build());
    let configuration = BaseEvmConfig::base(Arc::clone(&chain_spec));
    let environment = configuration.evm_env(&header)?;
    let evm = configuration.evm_with_env(database, environment);
    let parent_hash = header.parent_hash;
    let mut builder = PendingStateBuilder::new(
        chain_spec,
        evm,
        Block { header, body: Default::default() },
        None,
        L1BlockInfo::default(),
        overrides,
    );
    builder.apply_pre_execution_changes(parent_hash, beacon_root)?;
    Ok(builder.into_db_and_state_overrides())
}

// Execute original system bytecode after the same override application used by pending RPC.
fn call_from_canonical(
    mut canonical: InMemoryDB,
    overrides: StateOverride,
    header: &Header,
    target: Address,
    input: Bytes,
) -> Result<Bytes, Box<dyn Error>> {
    apply_state_overrides(overrides, &mut canonical)?;
    let caller = Address::repeat_byte(0x99);
    canonical.insert_account_info(
        caller,
        AccountInfo { balance: U256::from(1_000_000_000_u64), ..Default::default() },
    );
    let mut evm = Context::mainnet()
        .modify_cfg_chained(|configuration| {
            configuration.set_spec_and_mainnet_gas_params(SpecId::PRAGUE);
        })
        .modify_block_chained(|block| {
            block.number = U256::from(header.number);
            block.timestamp = U256::from(header.timestamp);
        })
        .with_db(canonical)
        .build_mainnet();
    let result = evm.transact(
        TxEnv::builder()
            .caller(caller)
            .kind(TxKind::Call(target))
            .data(input)
            .gas_limit(100_000)
            .build()?,
    )?;
    if !result.result.is_success() {
        return Err(format!("system contract RPC failed: {:?}", result.result).into());
    }
    Ok(result.result.output().ok_or("system contract RPC returned no output")?.clone())
}

#[test]
fn active_block_system_writes_reach_pending_rpc_across_multiple_blocks() -> TestResult {
    let mut canonical = InMemoryDB::default();
    for (address, bytes) in [
        (HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE.clone()),
        (BEACON_ROOTS_ADDRESS, BEACON_ROOTS_CODE.clone()),
    ] {
        let code = Bytecode::new_raw(bytes);
        canonical.insert_account_info(
            address,
            AccountInfo {
                code_hash: code.hash_slow(),
                code: Some(code),
                nonce: 1,
                ..Default::default()
            },
        );
    }
    let mut database = State::builder().with_database(canonical.clone()).build();
    let commits = Arc::new(AtomicUsize::new(0));
    let hook_commits = Arc::clone(&commits);
    database.set_state_hook(Some(Box::new(move |state: &EvmState| {
        assert!(!state.is_empty());
        hook_commits.fetch_add(1, Ordering::SeqCst);
    })));
    let mut overrides = StateOverride::default();
    for index in 0..2_u64 {
        let root = B256::repeat_byte(u8::try_from(index + 0xa0)?);
        let header = Header {
            number: 50_000_000 + index,
            timestamp: SYSTEM_TIMESTAMP + 2 * index,
            parent_hash: B256::repeat_byte(u8::try_from(index + 0xb0)?),
            ..Default::default()
        };
        (database, overrides) =
            prepare_system_block(database, overrides, header.clone(), Some(root))?;
        assert_eq!(commits.load(Ordering::SeqCst), usize::try_from((index + 1) * 2)?);
        for prior in 0..=index {
            let prior_root = B256::repeat_byte(u8::try_from(prior + 0xa0)?);
            let prior_hash = B256::repeat_byte(u8::try_from(prior + 0xb0)?);
            let root_input = U256::from(SYSTEM_TIMESTAMP + 2 * prior).to_be_bytes::<32>();
            let hash_input = U256::from(49_999_999 + prior).to_be_bytes::<32>();
            assert_eq!(
                call_from_canonical(
                    canonical.clone(),
                    overrides.clone(),
                    &header,
                    BEACON_ROOTS_ADDRESS,
                    Bytes::copy_from_slice(&root_input)
                )?,
                Bytes::copy_from_slice(prior_root.as_slice()),
            );
            assert_eq!(
                call_from_canonical(
                    canonical.clone(),
                    overrides.clone(),
                    &header,
                    HISTORY_STORAGE_ADDRESS,
                    Bytes::copy_from_slice(&hash_input)
                )?,
                Bytes::copy_from_slice(prior_hash.as_slice()),
            );
        }
        // Without pending overrides canonical storage still cannot answer the new root.
        assert!(
            call_from_canonical(
                canonical.clone(),
                StateOverride::default(),
                &header,
                BEACON_ROOTS_ADDRESS,
                Bytes::copy_from_slice(&U256::from(header.timestamp).to_be_bytes::<32>()),
            )
            .is_err()
        );
    }
    assert!(database.state_hook.is_some(), "the original hook must be restored");
    let account = database.basic(HISTORY_STORAGE_ADDRESS)?.ok_or("system account missing")?;
    let mut account = revm::state::Account::from(account);
    account.mark_touch();
    database.commit(EvmState::from_iter([(HISTORY_STORAGE_ADDRESS, account)]));
    assert_eq!(commits.load(Ordering::SeqCst), 5);
    Ok(())
}

#[test]
fn canyon_installation_reaches_rpc_from_canonical_without_deployer_code() -> TestResult {
    let inspector = Address::repeat_byte(0x77);
    let mut bytes = vec![0x73];
    bytes.extend_from_slice(CREATE2_DEPLOYER.as_slice());
    bytes.extend_from_slice(&[0x3b, 0x60, 0x00, 0x52, 0x73]);
    bytes.extend_from_slice(CREATE2_DEPLOYER.as_slice());
    bytes.extend_from_slice(&[0x3f, 0x60, 0x20, 0x52, 0x60, 0x40, 0x60, 0x00, 0xf3]);
    let code = Bytecode::new_raw(bytes.into());
    let mut canonical = InMemoryDB::default();
    canonical.insert_account_info(
        inspector,
        AccountInfo { code_hash: code.hash_slow(), code: Some(code), ..Default::default() },
    );
    assert!(canonical.basic(CREATE2_DEPLOYER)?.is_none());
    let header = Header { number: 1, timestamp: CANYON_TIMESTAMP, ..Default::default() };
    let (mut database, overrides) = prepare_system_block(
        State::builder().with_database(canonical.clone()).build(),
        StateOverride::default(),
        header.clone(),
        None,
    )?;
    let installed = database.basic(CREATE2_DEPLOYER)?.ok_or("Canyon deployer missing")?;
    let installed_code = installed.code.ok_or("Canyon deployer code missing")?;
    assert_eq!(installed_code.original_bytes().len(), 1584);
    assert_eq!(
        overrides.get(&CREATE2_DEPLOYER).and_then(|account| account.code.clone()),
        Some(installed_code.original_bytes())
    );
    let mut expected = Vec::new();
    expected.extend_from_slice(&U256::from(1584).to_be_bytes::<32>());
    expected.extend_from_slice(installed.code_hash.as_slice());
    assert_eq!(
        call_from_canonical(canonical.clone(), overrides, &header, inspector, Bytes::new())?,
        Bytes::from(expected)
    );
    assert_eq!(
        call_from_canonical(canonical, StateOverride::default(), &header, inspector, Bytes::new())?,
        Bytes::from(vec![0; 64])
    );
    Ok(())
}

#[test]
fn rejected_system_block_restores_existing_hook_without_publishing_partial_overrides() -> TestResult
{
    let chain_spec = Arc::new(BaseChainSpecBuilder::base_mainnet().build());
    let configuration = BaseEvmConfig::base(Arc::clone(&chain_spec));
    let header = Header { number: 1, timestamp: SYSTEM_TIMESTAMP, ..Default::default() };
    let mut database = State::builder().with_database(InMemoryDB::default()).build();
    let commits = Arc::new(AtomicUsize::new(0));
    let hook_commits = Arc::clone(&commits);
    database.set_state_hook(Some(Box::new(move |state: &EvmState| {
        hook_commits.fetch_add(state.len(), Ordering::SeqCst);
    })));
    let evm = configuration.evm_with_env(database, configuration.evm_env(&header)?);
    let mut builder = PendingStateBuilder::new(
        chain_spec,
        evm,
        Block { header, body: Default::default() },
        None,
        L1BlockInfo::default(),
        StateOverride::default(),
    );
    assert!(builder.apply_pre_execution_changes(B256::ZERO, None).is_err());
    let (mut database, overrides) = builder.into_db_and_state_overrides();
    assert!(overrides.is_empty());
    assert!(database.state_hook.is_some());
    let previous = commits.load(Ordering::SeqCst);
    let mut account = revm::state::Account::from(AccountInfo::default());
    account.mark_touch();
    database.commit(EvmState::from_iter([(Address::repeat_byte(0x11), account)]));
    assert_eq!(commits.load(Ordering::SeqCst), previous + 1);
    Ok(())
}
