use anyhow::Context;
use ccdeth_relayer::state_sender::{
    InitializedFilter, LockedTokenFilter, MerkleRootFilter, RoleAdminChangedFilter,
    RoleGrantedFilter, RoleRevokedFilter, StateSenderEvents, TokenMapAddedFilter,
    TokenMapRemovedFilter, VaultRegisteredFilter, WithdrawEventFilter,
};
use clap::Parser;
use concordium_rust_sdk::{id::types::AccountAddress, types::ContractAddress};
use ethabi::{ethereum_types::U256, RawLog};
use ethers::{
    abi::AbiDecode,
    prelude::{EthLogDecode, Http, HttpRateLimitRetryPolicy, Middleware, Provider, RetryClient},
};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct EthereumInspector {
    #[clap(long = "tx", help = "Transaction to query.")]
    tx: ethers::core::types::H256,
    #[clap(long = "address", help = "Address of the state sender.")]
    state_sender: ethers::core::types::Address,
    #[clap(long = "api", help = "JSON-RPC interface.")]
    ethereum_api: url::Url,
    /// Request timeout for Ethereum node requests.
    #[clap(
        long,
        help = "Timeout for requests to the Ethereum node.",
        env = "ETHCCD_RELAYER_ETHEREUM_REQUEST_TIMEOUT",
        default_value = "10"
    )]
    ethereum_request_timeout: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app: EthereumInspector = EthereumInspector::parse();

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

    let receipt = provider
        .get_transaction_receipt(app.tx)
        .await?
        .context("No transaction receipt.")?;
    for log in receipt.logs {
        if log.address == app.state_sender {
            let e = StateSenderEvents::decode_log(&RawLog {
                topics: log.topics,
                data:   log.data.0.into(),
            })?;
            match e {
                StateSenderEvents::InitializedFilter(InitializedFilter { version }) => {
                    println!("Initialized version {version}");
                }
                StateSenderEvents::LockedTokenFilter(LockedTokenFilter {
                    id,
                    depositor,
                    deposit_receiver,
                    root_token,
                    vault,
                    deposit_data,
                }) => {
                    println!("Deposited");
                    println!("  id = {id}");
                    println!("  depositor = {depositor:#x}");
                    println!("  receiver = {}", AccountAddress(deposit_receiver));
                    println!("  root token = {root_token:#x}");
                    println!("  vault = {vault:#x}");
                    let amount = U256::decode(deposit_data)?;
                    println!("  amount = {amount}");
                }
                StateSenderEvents::MerkleRootFilter(MerkleRootFilter { id, root }) => {
                    println!("Set Merkle root");
                    println!("  id = {id}");
                    println!("  new root = {}", hex::encode(&root[..]));
                }
                StateSenderEvents::RoleAdminChangedFilter(RoleAdminChangedFilter {
                    role,
                    previous_admin_role,
                    new_admin_role,
                }) => {
                    println!("Role admin changed");
                    println!("  role = {}", hex::encode(role));
                    println!(
                        "  previous admin role = {}",
                        hex::encode(previous_admin_role)
                    );
                    println!("  new admin role = {}", hex::encode(new_admin_role));
                }
                StateSenderEvents::RoleGrantedFilter(RoleGrantedFilter {
                    role,
                    account,
                    sender,
                }) => {
                    println!("Role granted");
                    println!("  role = {}", hex::encode(role));
                    println!("  account = {account:#x}");
                    println!("  sender = {sender:#x}");
                }
                StateSenderEvents::RoleRevokedFilter(RoleRevokedFilter {
                    role,
                    account,
                    sender,
                }) => {
                    println!("Role revoked");
                    println!("  role = {}", hex::encode(role));
                    println!("  account = {account:#x}");
                    println!("  sender = {sender:#x}");
                }
                StateSenderEvents::TokenMapAddedFilter(TokenMapAddedFilter {
                    id,
                    root_token,
                    child_token_index,
                    child_token_sub_index,
                    token_type,
                }) => {
                    println!("Token map added");
                    println!("  id = {id}");
                    println!("  root token = {root_token:#x}");
                    println!(
                        "  child contract = {}",
                        ContractAddress::new(child_token_index, child_token_sub_index)
                    );
                    println!("  token type = {}", hex::encode(token_type));
                }
                StateSenderEvents::TokenMapRemovedFilter(TokenMapRemovedFilter {
                    id,
                    root_token,
                    child_token_index,
                    child_token_sub_index,
                    token_type,
                }) => {
                    println!("Token map removed");
                    println!("  id = {id}");
                    println!("  root token = {root_token:#x}");
                    println!(
                        "  child contract = {}",
                        ContractAddress::new(child_token_index, child_token_sub_index)
                    );
                    println!("  token type = {}", hex::encode(token_type));
                }
                StateSenderEvents::VaultRegisteredFilter(VaultRegisteredFilter {
                    id,
                    token_type,
                    vault_address,
                }) => {
                    println!("Vault registered");
                    println!("  id = {id}");
                    println!("  vault address = {vault_address:#x}");
                    println!("  token type = {}", hex::encode(token_type));
                }
                StateSenderEvents::WithdrawEventFilter(WithdrawEventFilter {
                    id,
                    ccd_index,
                    ccd_sub_index,
                    amount,
                    user_wallet,
                    ccd_tx_hash,
                    ccd_event_index,
                    token_id,
                }) => {
                    println!("Withdraw");
                    println!("  id = {id}");
                    println!(
                        "  CCD contract = {}",
                        ContractAddress::new(ccd_index, ccd_sub_index)
                    );
                    println!("  amount = {amount}");
                    println!("  receiver = {user_wallet:#x}");
                    println!("  CCD tx hash = {}", hex::encode(ccd_tx_hash));
                    println!("  CCD event index = {ccd_event_index}");
                    println!("  token ID = {}", hex::encode(token_id.to_le_bytes()));
                }
            }
        }
    }

    Ok(())
}
