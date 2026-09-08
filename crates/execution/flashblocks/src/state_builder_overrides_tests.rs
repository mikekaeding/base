use super::accumulate_pending_state_overrides;
use alloy_evm::overrides::apply_state_overrides;
use alloy_primitives::Address;
use alloy_primitives::B256;
use alloy_primitives::Bytes;
use alloy_primitives::KECCAK256_EMPTY;
use alloy_primitives::TxKind;
use alloy_primitives::U256;
use alloy_rpc_types_eth::state::StateOverride;
use revm::Context;
use revm::Database;
use revm::DatabaseCommit;
use revm::ExecuteEvm;
use revm::MainBuilder;
use revm::MainContext;
use revm::context::TxEnv;
use revm::database::InMemoryDB;
use revm::primitives::hardfork::SpecId;
use revm::state::Account;
use revm::state::AccountInfo;
use revm::state::Bytecode;
use revm::state::EvmState;
use revm::state::EvmStorageSlot;
use std::error::Error;
use std::time::Instant;

type TestResult = Result<(), Box<dyn Error>>;

// Returns slot zero, EXTCODESIZE(address(this)), and EXTCODEHASH(address(this)).
fn introspecting_code() -> Bytecode {
    Bytecode::new_legacy(Bytes::from_static(&[
        0x60, 0x00, 0x54, 0x60, 0x00, 0x52, 0x30, 0x3b, 0x60, 0x20, 0x52, 0x30, 0x3f, 0x60, 0x40,
        0x52, 0x60, 0x60, 0x60, 0x00, 0xf3,
    ]))
}

fn changed_account(code: Bytecode, slot: u64) -> Account {
    let mut account = Account::from(AccountInfo {
        nonce: 1,
        balance: U256::from(100),
        code_hash: code.hash_slow(),
        code: Some(code),
        ..Default::default()
    });
    account.mark_touch();
    account.storage.insert(
        U256::ZERO,
        EvmStorageSlot::new_changed(U256::ZERO, U256::from(slot), Default::default()),
    );
    account
}

// Uses the same override application as eth_call/estimateGas, then executes actual EVM opcodes.
fn call_with_overrides(
    mut database: InMemoryDB,
    overrides: StateOverride,
    target: Address,
) -> Result<Bytes, Box<dyn Error>> {
    apply_state_overrides(overrides, &mut database)?;
    let caller = Address::repeat_byte(0x99);
    database.insert_account_info(
        caller,
        AccountInfo { balance: U256::from(1_000_000_000_u64), ..Default::default() },
    );
    let mut evm = Context::mainnet()
        .modify_cfg_chained(|configuration| {
            configuration.set_spec_and_mainnet_gas_params(SpecId::PRAGUE);
        })
        .with_db(database)
        .build_mainnet();
    let result = evm.transact(
        TxEnv::builder().caller(caller).kind(TxKind::Call(target)).gas_limit(100_000).build()?,
    )?;
    Ok(result.result.output().ok_or("pending RPC EVM call failed")?.clone())
}

fn expected_output(code: &Bytecode, slot: u64) -> Bytes {
    let mut bytes = Vec::with_capacity(96);
    bytes.extend_from_slice(&U256::from(slot).to_be_bytes::<32>());
    bytes.extend_from_slice(&U256::from(code.len()).to_be_bytes::<32>());
    bytes.extend_from_slice(code.hash_slow().as_slice());
    bytes.into()
}

#[test]
fn unchanged_code_omitted_without_changing_pending_rpc_state_or_code_identity() -> TestResult {
    let address = Address::repeat_byte(0x11);
    let code = introspecting_code();
    let account = changed_account(code.clone(), 42);
    let mut canonical = InMemoryDB::default();
    canonical.insert_account_info(address, account.info.clone());
    let state = EvmState::from_iter([(address, account)]);
    let mut pending = StateOverride::default();
    accumulate_pending_state_overrides(&mut canonical.clone(), &mut pending, &state)?;
    let account = pending.get(&address).ok_or("pending account missing")?;
    assert!(account.code.is_none());
    assert_eq!(account.balance, Some(U256::from(100)));
    assert_eq!(account.nonce, Some(1));
    let actual = call_with_overrides(canonical.clone(), pending.clone(), address)?;
    assert_eq!(actual, expected_output(&code, 42));

    let mut full = pending.clone();
    full.get_mut(&address).ok_or("full account missing")?.code = Some(code.original_bytes());
    assert_eq!(call_with_overrides(canonical.clone(), full, address)?, actual);

    // The former code.bytes() path included internal padding and corrupted both opcodes.
    assert!(code.bytes().len() > code.original_bytes().len());
    pending.get_mut(&address).ok_or("legacy account missing")?.code = Some(code.bytes());
    assert_ne!(call_with_overrides(canonical, pending, address)?, actual);
    Ok(())
}

#[test]
fn pending_creation_survives_later_flashblocks_with_unloaded_and_loaded_code() -> TestResult {
    let address = Address::repeat_byte(0x22);
    let code = introspecting_code();
    let canonical = InMemoryDB::default();
    let mut database = canonical.clone();
    let mut pending = StateOverride::default();
    let mut creation = changed_account(code.clone(), 1);
    creation.mark_created();
    let state = EvmState::from_iter([(address, creation)]);
    accumulate_pending_state_overrides(&mut database, &mut pending, &state)?;
    database.commit(state);
    for slot in [2, 3] {
        let mut account = changed_account(code.clone(), slot);
        if slot == 2 {
            account.info.code = None;
        }
        let state = EvmState::from_iter([(address, account)]);
        accumulate_pending_state_overrides(&mut database, &mut pending, &state)?;
        database.commit(state);
        let account = pending.get(&address).ok_or("created account missing")?;
        assert_eq!(account.code, Some(code.original_bytes()));
        assert!(account.state_diff.is_none());
        assert_eq!(
            call_with_overrides(canonical.clone(), pending.clone(), address)?,
            expected_output(&code, slot)
        );
    }
    Ok(())
}

#[test]
fn delegation_change_and_clear_are_explicit_even_when_bytecode_is_unloaded() -> TestResult {
    let address = Address::repeat_byte(0x33);
    let first_target = Address::repeat_byte(0x44);
    let second_target = Address::repeat_byte(0x55);
    let first_code = Bytecode::new_eip7702(first_target);
    let second_code = Bytecode::new_eip7702(second_target);
    let mut canonical = InMemoryDB::default();
    canonical.insert_account_info(address, changed_account(first_code, 0).info);
    canonical.insert_account_info(second_target, changed_account(introspecting_code(), 0).info);
    let mut database = canonical.clone();
    // A cached result can contain only a code hash; its code must resolve in the database.
    database.cache.contracts.insert(second_code.hash_slow(), second_code.clone());
    let mut changed = changed_account(second_code.clone(), 0);
    changed.info.code = None;
    let state = EvmState::from_iter([(address, changed)]);
    let mut pending = StateOverride::default();
    accumulate_pending_state_overrides(&mut database, &mut pending, &state)?;
    database.commit(state);
    assert_eq!(
        pending.get(&address).and_then(|account| account.code.clone()),
        Some(second_code.original_bytes())
    );
    assert_eq!(
        call_with_overrides(canonical.clone(), pending.clone(), address)?,
        expected_output(&second_code, 0)
    );
    let mut rpc_database = canonical.clone();
    apply_state_overrides(pending.clone(), &mut rpc_database)?;
    assert_eq!(
        rpc_database.basic(address)?.ok_or("delegation missing")?.code_hash,
        second_code.hash_slow()
    );

    let mut cleared = changed_account(Bytecode::default(), 0);
    cleared.info.code = None;
    let state = EvmState::from_iter([(address, cleared)]);
    accumulate_pending_state_overrides(&mut database, &mut pending, &state)?;
    assert_eq!(pending.get(&address).and_then(|account| account.code.clone()), Some(Bytes::new()));
    assert_eq!(call_with_overrides(canonical.clone(), pending.clone(), address)?, Bytes::new());
    apply_state_overrides(pending, &mut canonical)?;
    assert_eq!(
        canonical.basic(address)?.ok_or("cleared account missing")?.code_hash,
        KECCAK256_EMPTY
    );
    Ok(())
}

#[test]
fn unresolved_changed_code_fails_closed() -> TestResult {
    let address = Address::repeat_byte(0x66);
    let mut account = changed_account(introspecting_code(), 0);
    account.info.code = None;
    let state = EvmState::from_iter([(address, account)]);
    let result = accumulate_pending_state_overrides(
        &mut InMemoryDB::default(),
        &mut StateOverride::default(),
        &state,
    );
    assert!(result.is_err_and(|error| error.to_string().contains("code hash mismatch")));
    Ok(())
}

#[test]
fn selfdestruct_clears_pending_code_storage_balance_and_nonce() -> TestResult {
    let address = Address::repeat_byte(0x77);
    let code = introspecting_code();
    let canonical = InMemoryDB::default();
    let mut database = canonical.clone();
    let mut pending = StateOverride::default();
    let mut created = changed_account(code.clone(), 42);
    created.mark_created();
    let state = EvmState::from_iter([(address, created)]);
    accumulate_pending_state_overrides(&mut database, &mut pending, &state)?;
    database.commit(state);
    let mut destroyed = changed_account(code, 99);
    destroyed.mark_created();
    destroyed.mark_selfdestruct();
    let state = EvmState::from_iter([(address, destroyed)]);
    accumulate_pending_state_overrides(&mut database, &mut pending, &state)?;
    database.commit(state);
    let mut rpc_database = canonical;
    apply_state_overrides(pending, &mut rpc_database)?;
    assert_eq!(
        rpc_database.basic(address)?.unwrap_or_default(),
        database.basic(address)?.unwrap_or_default()
    );
    assert_eq!(rpc_database.storage(address, U256::ZERO)?, U256::ZERO);
    assert_eq!(call_with_overrides(rpc_database, StateOverride::default(), address)?, Bytes::new());
    Ok(())
}

#[test]
fn untouched_accounts_do_not_override_canonical_or_prior_pending_state() -> TestResult {
    let address = Address::repeat_byte(0x88);
    let mut account = changed_account(introspecting_code(), 42);
    account.unmark_touch();
    let mut pending = StateOverride::default();
    pending.entry(address).or_default().code = Some(Bytes::from_static(&[0x00]));
    let previous = pending.clone();
    accumulate_pending_state_overrides(
        &mut InMemoryDB::default(),
        &mut pending,
        &EvmState::from_iter([(address, account)]),
    )?;
    assert_eq!(pending, previous);
    Ok(())
}

// Run separately to report the cost of the exact alloy RPC preparation operation. Timing is
// diagnostic, never a pass/fail threshold; this synthetic workload is not an end-to-end node SLO.
#[test]
#[ignore = "bounded pending RPC preparation benchmark"]
fn benchmark_repeated_pending_rpc_override_preparation() -> TestResult {
    const ACCOUNT_COUNT: u64 = 128;
    const CODE_BYTES: usize = 16_384;
    const REQUEST_COUNT: u32 = 100;
    let code = Bytecode::new_legacy(Bytes::from(vec![0x5b; CODE_BYTES]));
    let mut canonical = InMemoryDB::default();
    let mut state = EvmState::default();
    for index in 0..ACCOUNT_COUNT {
        let address = Address::from_word(B256::from(U256::from(index + 1)));
        let account = changed_account(code.clone(), index);
        canonical.insert_account_info(address, account.info.clone());
        state.insert(address, account);
    }
    let mut optimized = StateOverride::default();
    accumulate_pending_state_overrides(&mut canonical.clone(), &mut optimized, &state)?;
    let mut previous = optimized.clone();
    for account in previous.values_mut() {
        account.code = Some(code.bytes());
    }
    let previous_bytes: usize =
        previous.values().filter_map(|account| account.code.as_ref()).map(|code| code.len()).sum();
    let optimized_bytes: usize =
        optimized.values().filter_map(|account| account.code.as_ref()).map(|code| code.len()).sum();
    assert_eq!(optimized_bytes, 0);
    assert!(previous_bytes >= usize::try_from(ACCOUNT_COUNT)? * CODE_BYTES);
    for (name, overrides) in [("previous", previous), ("optimized", optimized)] {
        let start = Instant::now();
        for _request in 0..REQUEST_COUNT {
            let mut database = canonical.clone();
            apply_state_overrides(overrides.clone(), &mut database)?;
            std::hint::black_box(database);
        }
        eprintln!(
            "{name}: requests={REQUEST_COUNT} accounts={ACCOUNT_COUNT} code_bytes_per_request={} elapsed_us={}",
            if name == "previous" { previous_bytes } else { optimized_bytes },
            start.elapsed().as_micros()
        );
    }
    Ok(())
}
