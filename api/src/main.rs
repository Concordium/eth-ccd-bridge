use anyhow::Context;
use axum::{http::StatusCode, Json};
use clap::Parser;
use concordium::{
    cis2::TokenId,
    types::{hashes::TransactionHash, ContractAddress},
};
use concordium_rust_sdk as concordium;
use postgres_types::FromSql;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse};
use utoipa::{openapi::ObjectBuilder, OpenApi};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Api {
    #[clap(
        long = "log-level",
        default_value = "info",
        help = "Maximum log level.",
        env = "ETHCCD_API_LOG_LEVEL"
    )]
    log_level:       tracing_subscriber::filter::LevelFilter,
    #[clap(
        long = "db",
        default_value = "host=localhost dbname=relayer user=postgres password=password port=5432",
        help = "Database connection string.",
        env = "ETHCCD_API_DB_STRING"
    )]
    db_config:       tokio_postgres::Config,
    #[clap(
        long = "listen-address",
        default_value = "0.0.0.0:8080",
        help = "Listen address for the server.",
        env = "ETHCCD_API_LISTEN_ADDRESS"
    )]
    listen_address:  std::net::SocketAddr,
    #[clap(
        long = "max-pool-size",
        default_value = "16",
        help = "Maximum size of a database connection pool.",
        env = "ETHCCD_API_MAX_DB_CONNECTION_POOL_SIZE"
    )]
    max_pool_size:   usize,
    #[clap(
        long = "request-timeout",
        default_value = "1000",
        help = "Request timeout in millisecons.",
        env = "ETHCCD_API_REQUEST_TIMEOUT"
    )]
    request_timeout: u64,
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        watch_deposit,
        watch_withdraw,
        list_tokens,
        wallet_transactions,
        get_merkle_proof,
        expected_merkle_root_update,
    ),
    components(schemas(
        WatchTxResponse,
        WatchWithdrawalResponse,
        TokenMapItem,
        WalletTx,
        TransactionStatus,
        EthMerkleProofResponse,
        WithdrawParams,
        WalletDepositTx,
        WalletWithdrawTx,
        WithdrawalStatus
    ))
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app: Api = Api::parse();
    {
        use tracing_subscriber::prelude::*;
        let log_filter = tracing_subscriber::filter::Targets::new()
            .with_target(module_path!(), app.log_level)
            .with_target("tower_http", app.log_level)
            .with_target("tokio_postgres", app.log_level);
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(log_filter)
            .init();
    }

    let db = Database::new(app.db_config, app.max_pool_size).await?;

    let openapi = ApiDoc::openapi();

    // build our application with a route
    let api = axum::Router::new()
        .route(
            "/api/v1/deposit/:tx_hash",
            axum::routing::get(watch_deposit),
        )
        .route(
            "/api/v1/withdraw/:tx_hash",
            axum::routing::get(watch_withdraw),
        )
        .route(
            "/api/v1/ethereum/proof/:tx_hash/:event_id",
            axum::routing::get(get_merkle_proof),
        )
        .route("/api/v1/tokens", axum::routing::get(list_tokens))
        .route("/api/v1/expectedMerkleRootUpdate", axum::routing::get(expected_merkle_root_update))
        .route(
            "/api/v1/wallet/:wallet",
            axum::routing::get(wallet_transactions),
        )
        .route(
            "/openapi.json",
            axum::routing::get(|| async move { Json(openapi) }),
        )
        .with_state(db)
        .layer(tower_http::trace::TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::new().include_headers(true)).on_response(DefaultOnResponse::new().include_headers(true)))
        .layer(tower_http::timeout::TimeoutLayer::new(
            std::time::Duration::from_millis(app.request_timeout),
        ))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(0)) // no bodies, we only have GET requests.
        .layer(tower_http::cors::CorsLayer::permissive()); // TODO: Remove for production.

    // run our app with hyper
    tracing::debug!("listening on {}", app.listen_address);
    axum::Server::bind(&app.listen_address)
        .serve(api.into_make_service())
        .await
        .context("Unable to start server.")?;
    Ok(())
}

fn optional_hash() -> utoipa::openapi::Object {
    ObjectBuilder::new()
        .schema_type(utoipa::openapi::SchemaType::String)
        .nullable(true)
        .description(Some("Optional transaction hash"))
        .build()
}

fn hex_string() -> utoipa::openapi::Object {
    ObjectBuilder::new()
        .schema_type(utoipa::openapi::SchemaType::String)
        .description(Some("Hex string"))
        .build()
}

#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct WatchTxResponse {
    status:             TransactionStatus,
    #[schema(schema_with = optional_hash)]
    concordium_tx_hash: Option<TransactionHash>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Pool error")]
    PoolError(#[from] deadpool_postgres::PoolError),
    #[error("Database error")]
    DBError(#[from] tokio_postgres::Error),
    #[error("Invalid request")]
    Invalid,
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Not found")]
    NotFound,
    #[error("Internal invariant violation")]
    Internal,
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let r = match self {
            Error::PoolError(_) => (StatusCode::REQUEST_TIMEOUT, Json("Server busy.".into())),
            Error::DBError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(e.to_string().into()),
            ),
            Error::Invalid => (StatusCode::BAD_REQUEST, Json("Invalid request.".into())),
            Error::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, Json(msg)),
            Error::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("Invalid request.".into()),
            ),
            Error::NotFound => (
                StatusCode::NOT_FOUND,
                Json("Requested value not found.".into()),
            ),
        };
        r.into_response()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unexpected data size.")]
struct IncorrectLength;

struct Fixed<const N: usize>(pub [u8; N]);

impl<'a, const N: usize> FromSql<'a> for Fixed<N> {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let v = <&[u8] as FromSql>::from_sql(ty, raw)?;
        Ok(Fixed(v.try_into().map_err(|_| Box::new(IncorrectLength))?))
    }

    fn accepts(ty: &postgres_types::Type) -> bool { <&[u8] as FromSql>::accepts(ty) }
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct WalletDepositTx {
    #[schema(schema_with = hex_string)]
    root_token:         ethers::prelude::Address,
    status:             TransactionStatus,
    #[schema(schema_with = optional_hash)]
    tx_hash:            Option<TransactionHash>,
    #[schema(schema_with = hex_string)]
    origin_tx_hash:     TransactionHash,
    origin_event_index: u64,
    amount:             String,
    timestamp:          i64,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct WalletWithdrawTx {
    #[schema(schema_with = contract_address)]
    child_token:        ContractAddress,
    #[schema(schema_with = optional_hash)]
    tx_hash:            Option<TransactionHash>,
    #[schema(schema_with = hex_string)]
    origin_tx_hash:     TransactionHash,
    origin_event_index: u64,
    amount:             String,
    status:             WithdrawalStatus,
    timestamp:          i64,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
enum WithdrawalStatus {
    #[serde(rename = "pending")]
    #[schema(rename = "pending")]
    Pending,
    #[serde(rename = "processed")]
    #[schema(rename = "processed")]
    Processed,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
enum WalletTx {
    Withdraw(WalletWithdrawTx),
    Deposit(WalletDepositTx),
}

#[utoipa::path(
        get,
        path = "api/v1/wallet/{wallet}",
        operation_id = "wallet_txs",
        params(
            ("wallet" = String,
            Path,
            description = "Ethereum Wallet address.")
        ),
        responses(
            (status = 200, description = "List wallet transactions.", body = [WalletTx]),
            (status = 400, description = "Invalid request.", body = inline(String), content_type = "application/json"),
            (status = 500, description = "Internal server error.", body = inline(String), content_type = "application/json"),
        )
    )]
async fn wallet_transactions(
    axum::extract::Path(wallet): axum::extract::Path<ethers::types::Address>,
    axum::extract::State(db): axum::extract::State<Database>,
) -> Result<axum::Json<Vec<WalletTx>>, Error> {
    let span = tracing::debug_span!(
        "wallet_transactions",
        time = chrono::Utc::now().timestamp_millis()
    );
    let _enter = span.enter();
    let client = db.pool.get().await?;
    let (statement, param) = &db.prepared_statements.get_withdrawals_for_address;
    let statement = client
        .prepare_typed_cached(statement, std::slice::from_ref(param))
        .await?;
    let withdraws = client.query(&statement, &[&wallet.as_bytes()]).await?;
    let (statement, param) = &db.prepared_statements.get_deposits_for_address;
    let statement = client
        .prepare_typed_cached(statement, std::slice::from_ref(param))
        .await?;
    let deposits = client.query(&statement, &[&wallet.as_bytes()]).await?;
    let mut out = Vec::new();
    for withdraw in withdraws {
        let tx_hash = withdraw
            .try_get::<_, Option<Fixed<32>>>("processed")?
            .map(|x| TransactionHash::new(x.0));
        let origin_tx_hash = TransactionHash::new(withdraw.try_get::<_, Fixed<32>>("tx_hash")?.0);
        let origin_event_index = withdraw.try_get::<_, i64>("event_index")? as u64;
        let amount = withdraw.try_get::<_, String>("amount")?;
        let index = withdraw.try_get::<_, i64>("child_index")? as u64;
        let subindex = withdraw.try_get::<_, i64>("child_subindex")? as u64;
        let timestamp = withdraw
            .try_get::<_, chrono::DateTime<chrono::Utc>>("insert_time")?
            .timestamp();
        out.push(WalletTx::Withdraw(WalletWithdrawTx {
            tx_hash,
            origin_tx_hash,
            origin_event_index,
            amount,
            timestamp,
            status: if tx_hash.is_some() {
                WithdrawalStatus::Processed
            } else {
                WithdrawalStatus::Pending
            },
            child_token: ContractAddress::new(index, subindex),
        }))
    }
    for deposit in deposits {
        let tx_hash = deposit.try_get::<_, Option<Fixed<32>>>("tx_hash")?;
        let origin_tx_hash =
            TransactionHash::new(deposit.try_get::<_, Fixed<32>>("origin_tx_hash")?.0);
        let origin_event_index = deposit.try_get::<_, i64>("origin_event_index")? as u64;
        let amount = deposit.try_get::<_, String>("amount")?;
        let root_token = deposit.try_get::<_, Fixed<20>>("root_token")?;
        let timestamp = deposit
            .try_get::<_, chrono::DateTime<chrono::Utc>>("insert_time")?
            .timestamp();
        out.push(WalletTx::Deposit(WalletDepositTx {
            status: if tx_hash.is_some() {
                TransactionStatus::Finalized
            } else {
                TransactionStatus::Pending
            },
            tx_hash: tx_hash.map(|x| TransactionHash::new(x.0)),
            origin_tx_hash,
            origin_event_index,
            amount,
            timestamp,
            root_token: root_token.0.into(),
        }))
    }
    Ok(out.into())
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct WithdrawParams {
    ccd_index:       u64,
    ccd_sub_index:   u64,
    amount:          String,
    #[schema(schema_with = hex_string)]
    user_wallet:     ethers::types::Address,
    #[schema(schema_with = hex_string)]
    ccd_tx_hash:     TransactionHash,
    ccd_event_index: u64,
    #[schema(schema_with = hex_string)]
    token_id:        TokenId,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct EthMerkleProofResponse {
    params: WithdrawParams,
    // hex string
    proof:  String,
}

#[utoipa::path(
    get,
    path = "/api/v1/ethereum/proof/{tx_hash}/{event_id}",
    operation_id = "eth_merkle_proof",
    params(
        ("tx_hash" = String,
         Path,
         description = "Withdrawal transaction hash."),
        ("event_id" = u64,
         Path,
         description = "Event id.")
    ),
    responses(
        (status = 200, description = "Proof.", body = Option<EthMerkleProofResponse>),
        (status = 400, description = "Invalid request.", body = inline(String), content_type = "application/json"),
        (status = 404, description = "Transaction hash and event ID not found.", body = inline(String), content_type = "application/json"),
        (status = 500, description = "Internal server error.", body = inline(String), content_type = "application/json"),
    )
)]
async fn get_merkle_proof(
    axum::extract::Path((tx_hash, event_id)): axum::extract::Path<(TransactionHash, u64)>,
    axum::extract::State(db): axum::extract::State<Database>,
) -> Result<axum::Json<EthMerkleProofResponse>, Error> {
    let span = tracing::debug_span!(
        "get_merkle_proof",
        time = chrono::Utc::now().timestamp_millis()
    );
    let _enter = span.enter();
    let client = db.pool.get().await?;
    let (statement, params) = &db.prepared_statements.get_event;
    let statement = client.prepare_typed_cached(statement, &params[..]).await?;
    let rows = client
        .query_opt(&statement, &[&&tx_hash.as_ref()[..], &(event_id as i64)])
        .await?;
    if let Some(row) = rows {
        let processed = row.try_get::<_, Option<Fixed<32>>>("processed")?;
        if processed.is_some() {
            return Err(Error::InvalidRequest("Event already processed".into()));
        }
        let data = row.try_get::<_, Vec<u8>>("event_data")?;
        let we: ccdeth_relayer::concordium_contracts::WithdrawEvent =
            concordium::smart_contracts::common::from_bytes(&data).map_err(|_| Error::Internal)?;
        let statement = &db.prepared_statements.get_merkle_leafs;
        let statement = client.prepare_typed_cached(statement, &[]).await?;
        let rows = client.query(&statement, &[]).await?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let tx_hash = row.try_get::<_, Fixed<32>>("tx_hash")?;
                let event_merkle_hash = row.try_get::<_, Fixed<32>>("event_merkle_hash")?.0;
                let tx_hash = TransactionHash::new(tx_hash.0);
                Ok((tx_hash, event_merkle_hash))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // TODO: avoid collecting above. Just iterate over results once.
        let proof = ccdeth_relayer::merkle::make_proof(rows, tx_hash);
        if let Some(proof) = proof {
            Ok(EthMerkleProofResponse {
                params: WithdrawParams {
                    ccd_index:       we.contract.index,
                    ccd_sub_index:   we.contract.subindex,
                    amount:          we.amount.to_string(),
                    user_wallet:     we.eth_address.into(),
                    ccd_tx_hash:     tx_hash,
                    ccd_event_index: we.event_index,
                    token_id:        we.token_id,
                },
                proof:  hex::encode(&proof.to_bytes()),
            }
            .into())
        } else {
            return Err(Error::InvalidRequest(
                "Event not in Merkle root at present.".into(),
            ));
        }
    } else {
        Err(Error::NotFound)
    }
}

#[utoipa::path(
        get,
        path = "api/v1/expectedMerkleRootUpdate",
        operation_id = "expected_merkle_root_update",
        responses(
            (status = 200, description = "Unix timestamp (in seconds) of the next scheduled update..", body = Option<i64>, content_type = "application/json"),
            (status = 500, description = "Internal server error.", body = inline(String), content_type = "application/json")
        )
    )]
/// Queried by Ethereum transaction hash, respond with the status of the
/// corresponding transaction on Concordium that handles the deposit.
pub async fn expected_merkle_root_update(
    axum::extract::State(db): axum::extract::State<Database>,
) -> Result<axum::Json<Option<i64>>, Error> {
    let client = db.pool.get().await?;
    let statement = &db.prepared_statements.get_next_merkle_root;
    let statement = client.prepare_typed_cached(statement, &[]).await?;
    let row = client.query_opt(&statement, &[]).await?;
    match row {
        None => Ok(None.into()),
        Some(v) => {
            let time = v.try_get::<_, chrono::DateTime<chrono::Utc>>("expected_time")?;
            Ok(Some(time.timestamp()).into())
        }
    }
}

#[utoipa::path(
        get,
        path = "api/v1/deposit/{tx_hash}",
        operation_id = "watch_deposit_tx",
        params(
            ("tx_hash" = String,
            Path,
            description = "Hash of the transaction to query, in hex.")
        ),
        responses(
            (status = 200, description = "Follow a deposit transaction.", body = WatchTxResponse),
            (status = 400, description = "Invalid request.", body = inline(String), content_type = "application/json"),
            (status = 500, description = "Internal server error.", body = inline(String), content_type = "application/json")
        )
    )]
/// Queried by Ethereum transaction hash, respond with the status of the
/// corresponding transaction on Concordium that handles the deposit.
pub async fn watch_deposit(
    path: Result<axum::extract::Path<ethers::types::H256>, axum::extract::rejection::PathRejection>,
    axum::extract::State(db): axum::extract::State<Database>,
) -> Result<axum::Json<WatchTxResponse>, Error> {
    let path = match path {
        Ok(p) => p,
        Err(e) => {
            return Err(Error::InvalidRequest(e.to_string()));
        }
    };
    let span = tracing::debug_span!(
        "watch_deposit",
        time = chrono::Utc::now().timestamp_millis()
    );
    let _enter = span.enter();
    let client = db.pool.get().await?;
    let (statement, params) = &db.prepared_statements.concordium_tx_status;
    let statement = client
        .prepare_typed_cached(statement, std::slice::from_ref(params))
        .await?;
    let row = client.query(&statement, &[&path.0.as_ref()]).await?;
    // TODO: This is how it is now, but it would be better to
    // not assume there can only be one deposit for one transaction.
    if let Some((first, rest)) = row.split_first() {
        if rest.is_empty() {
            let concordium_tx_hash = first.try_get::<_, Option<Fixed<32>>>("tx_hash")?;
            Ok(axum::Json(WatchTxResponse {
                status:             if concordium_tx_hash.is_some() {
                    TransactionStatus::Finalized
                } else {
                    TransactionStatus::Pending
                },
                concordium_tx_hash: concordium_tx_hash.map(|x| TransactionHash::new(x.0)),
            }))
        } else {
            tracing::warn!("Multiple deposit events for the same transaction.");
            Err(Error::Invalid)
        }
    } else {
        Ok(axum::Json(WatchTxResponse {
            status:             TransactionStatus::Missing,
            concordium_tx_hash: None,
        }))
    }
}

#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
struct WatchWithdrawalResponse {
    status:              &'static str,
    concordium_event_id: Option<u64>,
}

/// Queried by Concordium transaction hash, respond with the status of
/// withdrawal on Ethereum.
#[utoipa::path(
        get,
        path = "api/v1/withdraw/{tx_hash}",
        operation_id = "watch_withdraw_tx",
        params(
            ("tx_hash" = String,
            Path,
            description = "Hash of the transaction to query, in hex.")
        ),
        responses(
            (status = 200, description = "Follow a withdraw transaction.", body = WatchWithdrawalResponse),
            (status = 400, description = "Invalid request.", body = inline(String), content_type = "application/json"),
            (status = 500, description = "Internal server error.", body = inline(String), content_type = "application/json")
        )
    )]
async fn watch_withdraw(
    path: Result<axum::extract::Path<TransactionHash>, axum::extract::rejection::PathRejection>,
    axum::extract::State(db): axum::extract::State<Database>,
) -> Result<axum::Json<WatchWithdrawalResponse>, Error> {
    let path = match path {
        Ok(p) => p,
        Err(e) => {
            return Err(Error::InvalidRequest(e.to_string()));
        }
    };
    let span = tracing::debug_span!(
        "watch_withdraw",
        time = chrono::Utc::now().timestamp_millis()
    );
    let _enter = span.enter();
    let client = db.pool.get().await?;
    let (statement, params) = &db.prepared_statements.withdrawal_status;
    let statement = client
        .prepare_typed_cached(statement, std::slice::from_ref(params))
        .await?;
    let row = client.query(&statement, &[&path.0.as_ref()]).await?;
    // TODO: This is how it is now, but it would be better to
    // not assume there can only be one deposit for one transaction.
    if let Some((first, rest)) = row.split_first() {
        if rest.is_empty() {
            let processed = first.try_get::<_, Option<Fixed<32>>>("processed")?;
            let event_index = first.try_get::<_, Option<i64>>("event_index")?;
            Ok(axum::Json(WatchWithdrawalResponse {
                status:              if processed.is_some() {
                    "processed"
                } else {
                    "pending"
                },
                concordium_event_id: event_index.map(|x| x as u64),
            }))
        } else {
            tracing::warn!("Multiple deposit events for the same transaction.");
            Err(Error::Invalid)
        }
    } else {
        Ok(axum::Json(WatchWithdrawalResponse {
            status:              "missing",
            concordium_event_id: None,
        }))
    }
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct TokenMapItem {
    #[schema(schema_with = hex_string)]
    eth_address:  ethers::types::Address,
    eth_name:     String,
    decimals:     u8,
    #[schema(schema_with = contract_address)]
    ccd_contract: ContractAddress,
    ccd_name:     String,
}

#[derive(utoipa::ToSchema)]
#[schema(as = u64)]
struct Index(u64);

fn contract_address() -> utoipa::openapi::Object {
    ObjectBuilder::new()
        .schema_type(utoipa::openapi::SchemaType::Object)
        .property("index", <Index as utoipa::ToSchema>::schema().1)
        .property("subindex", <Index as utoipa::ToSchema>::schema().1)
        .description(Some("Smart contract instance address."))
        .build()
}

/// List all tokens that are mapped.
#[utoipa::path(
        get,
        path = "api/v1/tokens",
        operation_id = "list_tokens",
        responses(
            (status = 200, description = "List mapped tokens.", body = [TokenMapItem]),
            (status = 500, description = "Internal server error.", body = inline(String), content_type = "application/json")
        )
    )]
async fn list_tokens(
    axum::extract::State(db): axum::extract::State<Database>,
) -> Result<axum::Json<Vec<TokenMapItem>>, Error> {
    let span = tracing::debug_span!("list_tokens", time = chrono::Utc::now().timestamp_millis());
    let _enter = span.enter();
    let client = db.pool.get().await?;
    let statement = &db.prepared_statements.list_tokens;
    let statement = client.prepare_typed_cached(statement, &[]).await?;
    let rows = client.query(&statement, &[]).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let root = row.try_get::<_, Vec<u8>>("root")?;
        let child_index = row.try_get::<_, i64>("child_index")?;
        let child_subindex = row.try_get::<_, i64>("child_subindex")?;
        let eth_name = row.try_get::<_, String>("eth_name")?;
        let decimals = row.try_get::<_, i16>("decimals")? as u8;
        let eth_address = ethers::types::Address::from_slice(&root);
        out.push(TokenMapItem {
            eth_address,
            eth_name: eth_name.clone(),
            decimals,
            ccd_contract: ContractAddress::new(child_index as u64, child_subindex as u64),
            ccd_name: eth_name + ".eth",
        })
    }
    Ok(out.into())
}

#[derive(Clone)]
pub struct Database {
    pool:                deadpool_postgres::Pool,
    prepared_statements: Arc<QueryStatements>,
}

impl Database {
    pub async fn new(config: tokio_postgres::Config, pool_size: usize) -> anyhow::Result<Self> {
        let manager_config = deadpool_postgres::ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Verified,
        };
        let manager = deadpool_postgres::Manager::from_config(config, NoTls, manager_config);
        let pool = deadpool_postgres::Pool::builder(manager)
            .create_timeout(Some(std::time::Duration::from_secs(5)))
            .recycle_timeout(Some(std::time::Duration::from_secs(5)))
            .wait_timeout(Some(std::time::Duration::from_secs(5)))
            .max_size(pool_size)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .build()?;
        Ok(Self {
            pool,
            prepared_statements: Arc::new(QueryStatements::new()),
        })
    }
}

struct QueryStatements {
    concordium_tx_status:        (String, tokio_postgres::types::Type),
    withdrawal_status:           (String, tokio_postgres::types::Type),
    get_event:                   (String, [tokio_postgres::types::Type; 2]),
    get_merkle_leafs:            String,
    get_withdrawals_for_address: (String, tokio_postgres::types::Type),
    get_deposits_for_address:    (String, tokio_postgres::types::Type),
    list_tokens:                 String,
    get_next_merkle_root:        String,
}

impl QueryStatements {
    pub fn new() -> Self {
        let concordium_tx_status = (
            "SELECT tx_hash FROM ethereum_deposit_events WHERE origin_tx_hash = $1".into(),
            tokio_postgres::types::Type::BYTEA,
        );
        let withdrawal_status = (
            "SELECT processed, root, event_index FROM concordium_events WHERE tx_hash = $1".into(),
            tokio_postgres::types::Type::BYTEA,
        );
        let get_event = (
            "SELECT event_data, processed FROM concordium_events WHERE tx_hash = $1 AND \
             event_index = $2"
                .into(),
            [
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::INT8,
            ],
        );
        let get_merkle_leafs = "SELECT tx_hash, event_merkle_hash FROM concordium_events WHERE \
                                root IN (SELECT root FROM merkle_roots ORDER BY id DESC LIMIT 1) \
                                ORDER BY event_index ASC"
            .into();
        let get_withdrawals_for_address = (
            "SELECT insert_time, processed, tx_hash, child_index, child_subindex, amount, \
             event_index FROM concordium_events WHERE event_type = 'withdraw' AND receiver = $1"
                .into(),
            tokio_postgres::types::Type::BYTEA,
        );
        let get_deposits_for_address = (
            "SELECT insert_time, tx_hash, root_token, tx_hash, amount, origin_tx_hash, \
             origin_event_index FROM ethereum_deposit_events WHERE depositor = $1"
                .into(),
            tokio_postgres::types::Type::BYTEA,
        );
        let list_tokens = "SELECT root, child_index, child_subindex, eth_name, decimals FROM \
                           token_maps ORDER BY id ASC"
            .into();
        let get_next_merkle_root =
            "SELECT expected_time FROM expected_merkle_update WHERE tag = ''".into();
        Self {
            concordium_tx_status,
            withdrawal_status,
            get_event,
            get_merkle_leafs,
            get_withdrawals_for_address,
            get_deposits_for_address,
            list_tokens,
            get_next_merkle_root,
        }
    }
}

// FIXME: Unify into a library shared by relayer and API.
#[derive(
    Debug,
    Copy,
    Clone,
    tokio_postgres::types::ToSql,
    tokio_postgres::types::FromSql,
    utoipa::ToSchema,
)]
#[postgres(name = "concordium_transaction_status")]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum TransactionStatus {
    /// Transaction was added to the database and not yet finalized.
    #[postgres(name = "pending")]
    #[serde(rename = "pending")]
    #[schema(rename = "pending")]
    Pending,
    /// Transaction was finalized.
    #[postgres(name = "failed")]
    #[serde(rename = "failed")]
    #[schema(rename = "failed")]
    Failed,
    /// Transaction was finalized.
    #[postgres(name = "finalized")]
    #[serde(rename = "processed")]
    #[schema(rename = "processed")]
    Finalized,
    #[postgres(name = "missing")]
    #[schema(rename = "missing")]
    #[serde(rename = "missing")]
    Missing,
}
