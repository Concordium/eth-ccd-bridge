use anyhow::Context;
use ccdeth_relayer::{
    concordium_contracts::{self, BridgeManagerClient},
    db::{self, Database},
    ethereum,
    merkle::{self, MerkleSetterClient},
    state_sender::StateSender,
};
use clap::Parser;
use concordium::{
    types::{AbsoluteBlockHeight, ContractAddress, WalletAccount},
    v2,
};
use concordium_rust_sdk as concordium;
use ethabi::ethereum_types::U256;
use ethers::prelude::{
    Http, HttpRateLimitRetryPolicy, LocalWallet, Middleware, Provider, RetryClient, Signer,
};
use futures::StreamExt;
use std::{path::PathBuf, sync::Arc};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Relayer {
    #[clap(
        long = "log-level",
        default_value = "info",
        help = "Maximum log level.",
        env = "ETHCCD_RELAYER_LOG_LEVEL"
    )]
    log_level: log::LevelFilter,
    #[clap(
        long = "state-sender-address",
        help = "Address of the StateSender proxy instance.",
        env = "ETHCCD_RELAYER_STATE_SENDER_PROXY"
    )]
    state_sender: ethers::core::types::Address,
    #[clap(
        long = "root-chain-manager-address",
        help = "Address of the RootChainManager proxy instance.",
        env = "ETHCCD_RELAYER_ROOT_CHAIN_MANAGER_PROXY"
    )]
    root_chain_manager: ethers::core::types::Address,
    #[clap(
        long = "state-sender-creation-height",
        help = "Block number when the state sender instance was created.",
        env = "ETHCCD_RELAYER_STATE_SENDER_CREATION_BLOCK_NUMBER"
    )]
    state_sender_creation_block_number: u64,
    #[clap(
        long = "start-block",
        help = "Where to start monitoring if no activity is yet detected on the state manager.",
        env = "ETHCCD_RELAYER_ETH_START_BLOCK"
    )]
    start_block: u64,
    #[clap(
        long = "bridge-manager-address",
        help = "Address of the BridgeManger contract on Concordium.",
        env = "ETHCCD_RELAYER_BRIDGE_MANAGER"
    )]
    bridge_manager: ContractAddress,
    #[clap(
        long = "concordium-wallet-file",
        help = "File with the Concordium wallet.",
        env = "ETHCCD_RELAYER_CONCORDIUM_WALLET_FILE"
    )]
    concordium_wallet: PathBuf,
    #[clap(
        long = "concordium-api",
        help = "GRPC V2 interface of the Concordium node.",
        env = "ETHCCD_RELAYER_CONCORDIUM_API",
        default_value = "http://localhost:20000"
    )]
    concordium_api: v2::Endpoint,
    #[clap(
        long = "ethereum-api",
        help = "JSON-RPC interface.",
        env = "ETHCCD_RELAYER_ETHEREUM_API"
    )]
    ethereum_api: url::Url,
    #[clap(
        long = "db",
        default_value = "host=localhost dbname=relayer user=postgres password=password port=5432",
        help = "Database connection string.",
        env = "ETHCCD_RELAYER_DB_STRING"
    )]
    db_config: tokio_postgres::Config,
    #[clap(long, env = "ETH_KEY")]
    eth_private_key: LocalWallet,
    #[clap(
        long,
        env = "ETHCCD_RELAYER_MAX_PARALLEL_QUERIES_CONCORDIUM",
        default_value = "8"
    )]
    max_parallel: u32,
    // Maximum number of seconds a concordium node can be behind before it is deemed "behind".
    #[clap(
        long,
        env = "ETHCCD_RELAYER_CONCORDIUM_MAX_BEHIND",
        default_value = "240"
    )]
    max_behind: u32,
    // Maximum gas price.
    #[clap(
        long,
        env = "ETHCCD_RELAYER_MAX_GAS_PRICE",
        value_parser = U256::from_dec_str,
        default_value = "1000000000",
    )]
    max_gas_price: U256,
    // Maximum gas for setting merkle roots.
    #[clap(long, env = "ETHCCD_RELAYER_MAX_GAS", value_parser = U256::from_dec_str, default_value = "100000")]
    max_gas: U256,
    // Maximum gas for setting merkle roots.
    #[clap(
        long,
        env = "ETHCCD_RELAYER_MERKLE_UPDATE_INTERVAL",
        default_value = "600"
    )]
    merkle_update_interval: u64,
    /// Chain ID for the Ethereum network.
    #[clap(
        long,
        help = "Chain ID. Goerli is 5, mainnet is 1.",
        env = "ETHCCD_RELAYER_CHAIN_ID",
        default_value_t = 5
    )]
    chain_id: u64,
    /// Number of confirmations required on Ethereum before considering
    /// the transaction as "final".
    #[clap(
        long,
        help = "Number of confirmations required on Ethereum before considering the transaction \
                as final.",
        env = "ETHCCD_RELAYER_NUM_CONFIRMATIONS",
        default_value = "10"
    )]
    num_confirmations: u64,
    /// Request timeout for Concordium node requests.
    #[clap(
        long,
        help = "Timeout for requests to the Concordium node.",
        env = "ETHCCD_RELAYER_CONCORDIUM_REQUEST_TIMEOUT",
        default_value = "10"
    )]
    concordium_request_timeout: u64,
    /// Request timeout for Ethereum node requests.
    #[clap(
        long,
        help = "Timeout for requests to the Ethereum node.",
        env = "ETHCCD_RELAYER_ETHEREUM_REQUEST_TIMEOUT",
        default_value = "10"
    )]
    ethereum_request_timeout: u64,
}

async fn find_start_ethereum_config<M: Middleware>(
    client: M,
    last_processed: Option<u64>,
    creation_height: u64,
    num_confirmations: u64,
) -> anyhow::Result<(u64, u64)>
where
    M::Error: 'static, {
    let last_finalized: u64 = client
        .get_block_number()
        .await?
        .as_u64()
        .saturating_sub(num_confirmations);
    if let Some(last_processed) = last_processed {
        Ok((
            last_processed + 1,
            std::cmp::max(last_processed + 1, last_finalized),
        ))
    } else {
        Ok((
            creation_height,
            std::cmp::max(last_finalized, creation_height),
        ))
    }
}

async fn find_concordium_start_height(
    mut client: v2::Client,
    last_processed: Option<AbsoluteBlockHeight>,
    manager_address: ContractAddress,
) -> anyhow::Result<AbsoluteBlockHeight> {
    if let Some(h) = last_processed {
        Ok(h.next())
    } else {
        let (height, _, _) = client.find_instance_creation(.., manager_address).await?;
        Ok(height)
    }
}

fn spawn_cancel<T>(
    died_sender: tokio::sync::broadcast::Sender<()>,
    future: T,
) -> tokio::task::JoinHandle<T::Output>
where
    T: futures::Future + Send + 'static,
    T::Output: Send + 'static, {
    tokio::spawn(async move {
        let res = future.await;
        // We ignore errors here since this always happens at the end of a task.
        // Since we keep one receiver alive until the end of the `main` function
        // the error should not happen anyhow.
        let _ = died_sender.send(());
        res
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app: Relayer = Relayer::parse();

    let mut log_builder = env_logger::Builder::from_env("ETHCCD_RELAYER_LOG");
    // only log the current module (main).
    log_builder.filter_module(module_path!(), app.log_level);
    log_builder.init();

    let inner_ethereum_client = {
        let network_client = reqwest::ClientBuilder::new()
            .timeout(std::time::Duration::from_secs(app.ethereum_request_timeout))
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()?;
        Http::new_with_client(app.ethereum_api, network_client)
    };
    let ethereum_client = RetryClient::new(
        inner_ethereum_client,
        Box::new(HttpRateLimitRetryPolicy::default()),
        5,
        3000,
    );

    let provider = Provider::new(ethereum_client);
    let ethereum_client = Arc::new(provider);

    // Transactions will be signed with the private key below and will be broadcast
    // via the eth_sendRawTransaction API)
    let wallet: LocalWallet = app.eth_private_key.with_chain_id(app.chain_id);
    let sender = wallet.address();

    let balance = ethereum_client.get_balance(sender, None).await?;
    log::info!("Balance of the Ethereum sender account is {balance}.");
    let ethereum_nonce = ethereum_client.get_transaction_count(sender, None).await?;
    log::info!("Nonce of the Ethereum sender account is {ethereum_nonce}.");
    log::info!("Using max gas price bound = {}.", app.max_gas_price);
    log::info!("Using max gas bound = {}.", app.max_gas);
    log::info!("Using chain id = {}.", app.chain_id);

    // Set up signal handlers before doing anything non-trivial so we have some sort
    // of graceful shut down during initial database lookups and pending
    // transaction sends.
    let (stop_sender, mut stop_receiver) = tokio::sync::watch::channel(());
    let (died_sender, died_receiver) = tokio::sync::broadcast::channel(10);
    let shutdown_handler_handle = spawn_cancel(
        died_sender.clone(),
        set_shutdown(stop_sender, died_receiver),
    );

    // ethers::prelude::Abigen::new("BridgeManager", "abis/root-chain-manager.json")
    //     .unwrap()
    //     .generate()
    //     .unwrap()
    //     .write_to_file(format!("src/root_chain_manager.rs"))
    //     .unwrap();

    // ethers::prelude::Abigen::new("StateSender", "abis/state-sender.json")
    //     .unwrap()
    //     .generate()
    //     .unwrap()
    //     .write_to_file(format!("src/state_sender.rs"))
    //     .unwrap();

    // ethers::prelude::Abigen::new("Erc20", "abis/erc20.json")
    //     .unwrap()
    //     .generate()
    //     .unwrap()
    //     .write_to_file(format!("src/erc20.rs"))
    //     .unwrap();

    // return Ok(());

    // let erc20 = ccdeth_relayer::erc20::Erc20::new(app.state_sender,
    // ethereum_client.clone()); let name = erc20.name().call().await;
    // let decimals = erc20.decimals().call().await?;

    let state_sender_contract = StateSender::new(app.state_sender, ethereum_client.clone());

    let root_chain_manager_contract = ccdeth_relayer::root_chain_manager::BridgeManager::new(
        app.root_chain_manager,
        Arc::new(ethereum_client.clone()),
    );

    let mut concordium_client = {
        let ep = app
            .concordium_api
            .timeout(std::time::Duration::from_secs(
                app.concordium_request_timeout,
            ))
            .connect_timeout(std::time::Duration::from_secs(10));
        v2::Client::new(ep).await?
    };
    {
        let lfb = concordium_client
            .get_consensus_info()
            .await?
            .last_finalized_block;
        let bi = concordium_client.get_block_info(lfb).await?.response;
        if chrono::Utc::now().signed_duration_since(bi.block_slot_time)
            > chrono::Duration::seconds(app.max_behind.into())
        {
            anyhow::bail!(
                "Unable to start. The last finalized time of the Concordium node is more than {}s \
                 in the past.",
                app.max_behind,
            );
        }
    }

    let (last_ethereum, last_concordium, db) = Database::new(&app.db_config).await?;
    let start_nonce = db.submit_missing_txs(concordium_client.clone()).await?;

    let concordium_wallet = WalletAccount::from_json_file(app.concordium_wallet)?;

    let bridge_manager_client = BridgeManagerClient::new(
        concordium_client.clone(),
        concordium_wallet.address,
        app.bridge_manager,
    );

    let bridge_manager = concordium_contracts::BridgeManager::new(
        bridge_manager_client.clone(),
        concordium_wallet,
        start_nonce,
    )
    .await
    .context("Unable to connect to Concordium API.")?;

    let (start_number, upper_number) = find_start_ethereum_config(
        ethereum_client.clone(),
        last_ethereum,
        app.state_sender_creation_block_number,
        app.num_confirmations,
    )
    .await?;
    log::info!(
        "Found starting point on Ethereum chain at start = {start_number}, end ={upper_number})"
    );
    let concordium_start_height = find_concordium_start_height(
        concordium_client.clone(),
        last_concordium,
        app.bridge_manager,
    )
    .await?;

    log::info!("Starting at {concordium_start_height} on the Concordium chain.");

    let (max_marked_event_index, leaves) = db
        .pending_withdrawals(bridge_manager_client.clone())
        .await?;

    // To spawn

    let (db_sender, db_receiver) = tokio::sync::mpsc::channel(50);
    let (ccd_transaction_sender, ccd_transaction_receiver) = tokio::sync::mpsc::channel(50);
    let (merkle_setter_sender, merkle_setter_receiver) = tokio::sync::mpsc::channel(50);

    let pending_merkle_set = db.pending_ethereum_tx().await?;

    // Now we set up the main service, after we have established a baseline.
    // The different tasks communicate using channels established above.
    // The shutdown plan is as follows.
    // - Tasks which only query are just aborted.
    // - Tasks which send transactions or write to the database are given an
    //   opportunity to shut down gracefully by sending a signal on the
    //   stop_sender/stop_receiver channel. The same broadcast channel is shared by
    //   all tasks, and the only sender is the signal handler.
    let tx_sender_handle = spawn_cancel(
        died_sender.clone(),
        concordium_contracts::concordium_tx_sender(
            concordium_client.clone(),
            ccd_transaction_receiver,
            stop_receiver.clone(),
        ),
    );
    let db_task_handle = spawn_cancel(
        died_sender.clone(),
        db::handle_database(
            app.db_config,
            db,
            db_receiver,
            bridge_manager,
            ccd_transaction_sender,
            merkle_setter_sender,
            stop_receiver.clone(),
        ),
    );
    let merkle_updater_handle = {
        let merkle_client = MerkleSetterClient::new(
            root_chain_manager_contract,
            wallet,
            app.max_gas_price.into(),
            app.max_gas.into(),
            ethereum_nonce,
            &pending_merkle_set,
            std::time::Duration::from_secs(app.merkle_update_interval),
            leaves,
            max_marked_event_index,
        )?;

        spawn_cancel(
            died_sender.clone(),
            merkle::send_merkle_root_updates(
                merkle_client,
                pending_merkle_set,
                merkle_setter_receiver,
                db_sender.clone(),
                app.num_confirmations,
                stop_receiver.clone(),
            ),
        )
    };

    // The remaining tasks only watch so they are aborted on on signal received.
    let watch_concordium_handle = spawn_cancel(
        died_sender.clone(),
        concordium_contracts::listen_concordium(
            bridge_manager_client.clone(),
            db_sender.clone(),
            concordium_start_height,
            app.max_parallel,
            app.max_behind,
        ),
    );
    let watch_ethereum_handle = spawn_cancel(
        died_sender.clone(),
        ethereum::watch_eth_blocks(
            state_sender_contract,
            db_sender.clone(),
            start_number,
            upper_number,
            app.num_confirmations,
        ),
    );

    // Wait for signal to be received.
    if let Err(e) = stop_receiver.changed().await {
        log::error!("The signal handler unexpectedly died with {e}. Shutting off the service.");
    }

    // Stop watcher tasks.
    watch_concordium_handle.abort();
    watch_ethereum_handle.abort();
    // And wait for all of them to terminate.
    let shutdown = [
        await_and_report("merkle updater", merkle_updater_handle),
        await_and_report("watch Ethereum", watch_ethereum_handle),
        await_and_report("watch Concordium", watch_concordium_handle),
        await_and_report("database handler", db_task_handle),
        await_and_report("concordium transaction sender", tx_sender_handle),
    ];
    shutdown
        .into_iter()
        .collect::<futures::stream::FuturesUnordered<_>>()
        .collect::<()>()
        .await;
    await_and_report("shutdown handler", shutdown_handler_handle).await;
    drop(died_sender); // keep the sender alive until here explicitly so that we don't have spurious
                       // errors when the last task is dying.
    Ok(())
}

async fn await_and_report<E: std::fmt::Display>(
    descr: &str,
    handle: tokio::task::JoinHandle<Result<(), E>>,
) {
    match handle.await {
        Ok(Ok(())) => {
            log::info!("Task {descr} terminated.");
        }
        Ok(Err(e)) => {
            log::error!("Task {descr} unexpectedly stopped due to {e:#}.");
        }
        Err(e) => {
            if e.is_panic() {
                log::error!("Task panicked.");
            } else if e.is_cancelled() {
                log::info!("Task {descr} was cancelled.");
            } else {
                log::error!("Task {descr} unexpectedly closed.");
            }
        }
    }
}

/// Construct a future for shutdown signals (for unix: SIGINT and SIGTERM) (for
/// windows: ctrl c and ctrl break). The signal handler is set when the future
/// is polled and until then the default signal handler.
async fn set_shutdown(
    stop_sender: tokio::sync::watch::Sender<()>,
    mut task_died: tokio::sync::broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix as unix_signal;
        let mut terminate_stream = unix_signal::signal(unix_signal::SignalKind::terminate())?;
        let mut interrupt_stream = unix_signal::signal(unix_signal::SignalKind::interrupt())?;
        let terminate = Box::pin(terminate_stream.recv());
        let interrupt = Box::pin(interrupt_stream.recv());
        let task_died = Box::pin(task_died.recv());
        futures::future::select(task_died, futures::future::select(terminate, interrupt)).await;
        if stop_sender.send(()).is_err() {
            log::error!("Unable to send stop signal.");
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows as windows_signal;
        let mut ctrl_break_stream = windows_signal::ctrl_break()?;
        let mut ctrl_c_stream = windows_signal::ctrl_c()?;
        let ctrl_break = Box::pin(ctrl_break_stream.recv());
        let ctrl_c = Box::pin(ctrl_c_stream.recv());
        let task_died = Box::pin(task_died.recv());
        futures::future::select(task_died, futures::future::select(ctrl_break, ctrl_c)).await;
        if stop_sender.send(()).is_err() {
            log::error!("Unable to send stop signal.");
        }
    }
    Ok(())
}
