use crate::{
    concordium_contracts::{self, BridgeEvent, BridgeManager, BridgeManagerClient, WithdrawEvent},
    ethereum,
};
use anyhow::Context;
use concordium_rust_sdk::{
    cis2,
    common::{self, to_bytes},
    smart_contracts::common as contracts_common,
    types::{
        hashes::TransactionHash,
        queries::BlockInfo,
        transactions::{BlockItem, EncodedPayload, PayloadLike},
        AbsoluteBlockHeight, ContractAddress, Nonce,
    },
    v2,
};
use ethabi::ethereum_types::{H160, H256, U256};
use num_bigint::BigUint;
use tokio::task::JoinHandle;
use tokio_postgres::{NoTls, Statement, Transaction};

const SCHEMA: &str = include_str!("../resources/db_schema.sql");

#[derive(Debug, Copy, Clone, tokio_postgres::types::ToSql, tokio_postgres::types::FromSql)]
#[postgres(name = "network")]
pub enum Network {
    #[postgres(name = "ethereum")]
    Ethereum,
    #[postgres(name = "concordium")]
    Concordium,
}

#[derive(Debug, Copy, Clone, tokio_postgres::types::ToSql, tokio_postgres::types::FromSql)]
#[postgres(name = "ethereum_transaction_status")]
pub enum EthTransactionStatus {
    /// Transaction was added to the database and not yet finalized.
    #[postgres(name = "pending")]
    Pending,
    /// Transaction was finalized.
    #[postgres(name = "confirmed")]
    Confirmed,
    #[postgres(name = "missing")]
    Missing,
}

#[derive(
    Debug,
    Copy,
    Clone,
    tokio_postgres::types::ToSql,
    tokio_postgres::types::FromSql,
    serde::Serialize,
)]
#[postgres(name = "concordium_transaction_status")]
pub enum TransactionStatus {
    /// Transaction was added to the database and not yet finalized.
    #[postgres(name = "pending")]
    #[serde(rename = "pending")]
    Pending,
    /// Transaction was finalized, but failed.
    #[postgres(name = "failed")]
    #[serde(rename = "failed")]
    Failed,
    /// Transaction was finalized.
    #[postgres(name = "finalized")]
    #[serde(rename = "finalized")]
    Finalized,
    #[postgres(name = "missing")]
    #[serde(rename = "missing")]
    Missing,
}

struct PreparedStatements {
    insert_concordium_tx:         Statement,
    get_pending_concordium_txs:   Statement,
    get_pending_ethereum_txs:     Statement,
    mark_concordium_tx:           Statement,
    insert_ethereum_tx:           Statement,
    insert_concordium_event:      Statement,
    mark_withdrawal_as_completed: Statement,
    get_pending_withdrawals:      Statement,
    get_max_event_index:          Statement,
}

impl PreparedStatements {
    pub async fn insert_concordium_tx<'a, 'b, Payload: PayloadLike>(
        &'a self,
        db_tx: &Transaction<'b>,
        origin_tx_hash: &H256,
        bi: &BlockItem<Payload>,
    ) -> anyhow::Result<i64> {
        let hash = bi.hash();
        let timestamp = chrono::Utc::now().timestamp();
        let tx_bytes = to_bytes(bi);
        let res = db_tx
            .query_one(&self.insert_concordium_tx, &[
                &&hash.as_ref()[..],
                &tx_bytes,
                &origin_tx_hash.as_bytes(),
                &timestamp,
                &TransactionStatus::Pending,
            ])
            .await?;
        Ok(res.get::<_, i64>(0))
    }

    /// Insert the event. If the event is a Withdraw event
    /// return whether it has already been processed or not.
    pub async fn insert_concordium_event<'a, 'b>(
        &'a self,
        db_tx: &Transaction<'b>,
        tx_hash: &TransactionHash,
        event: &BridgeEvent,
        merkle_hash: Option<[u8; 32]>,
    ) -> anyhow::Result<bool> {
        let (event_type, origin_event_index, data) = match event {
            BridgeEvent::TokenMap(tm) => {
                let rows = db_tx
                    .query(&self.mark_concordium_tx, &[
                        &tx_hash.as_ref(),
                        &TransactionStatus::Finalized,
                    ])
                    .await?;
                if rows.len() != 1 {
                    log::warn!(
                        "A TokenMap event was emitted by a transaction not submitted by the \
                         relayer."
                    );
                }
                (
                    ConcordiumEventType::TokenMap,
                    Some(tm.id as i64),
                    contracts_common::to_bytes(tm),
                )
            }
            BridgeEvent::Deposit(de) => {
                log::debug!("Marking a deposit with event index {} as completed.", de.id);
                let rows = db_tx
                    .query(
                        "UPDATE ethereum_deposit_events SET tx_hash = $2 WHERE origin_event_index \
                         = $1 RETURNING id",
                        &[&(de.id as i64), &&tx_hash.as_ref()[..]],
                    )
                    .await?;
                if rows.len() != 1 {
                    log::warn!("Deposited an event that was not emitted on Ethereum.");
                }
                let rows = db_tx
                    .query(&self.mark_concordium_tx, &[
                        &tx_hash.as_ref(),
                        &TransactionStatus::Finalized,
                    ])
                    .await?;
                if rows.len() != 1 {
                    log::warn!(
                        "A deposit event was emitted by a transaction not submitted by the \
                         relayer."
                    );
                }
                (
                    ConcordiumEventType::Deposit,
                    Some(de.id as i64),
                    contracts_common::to_bytes(de),
                )
            }
            BridgeEvent::Withdraw(we) => {
                let res = db_tx
                    .query_one(&self.insert_concordium_event, &[
                        &&tx_hash.as_ref()[..],
                        &Some(we.event_index as i64),
                        &None::<i64>,
                        &ConcordiumEventType::Withdraw,
                        &Some(we.contract.index as i64),
                        &Some(we.contract.subindex as i64),
                        &Some(&we.eth_address[..]),
                        &Some(&we.amount.to_string()),
                        &contracts_common::to_bytes(we),
                        &merkle_hash.as_ref().map(|x| &x[..]),
                    ])
                    .await?;
                return Ok(res.get::<_, bool>(0));
            }
            BridgeEvent::GrantRole(gr) => (
                ConcordiumEventType::GrantRole,
                None,
                contracts_common::to_bytes(gr),
            ),
            BridgeEvent::RevokeRole(rr) => (
                ConcordiumEventType::RevokeRole,
                None,
                contracts_common::to_bytes(rr),
            ),
        };
        let res = db_tx
            .query_one(&self.insert_concordium_event, &[
                &&tx_hash.as_ref()[..],
                &event.event_index().map(|x| x as i64),
                &origin_event_index,
                &event_type,
                &None::<i64>,
                &None::<i64>,
                &None::<Vec<u8>>,
                &None::<String>,
                &data,
                &merkle_hash.as_ref().map(|x| &x[..]),
            ])
            .await?;
        Ok(res.get::<_, bool>(0))
    }
}

pub struct Database {
    pub client:          tokio_postgres::Client,
    connection_handle:   JoinHandle<Result<(), tokio_postgres::Error>>,
    prepared_statements: PreparedStatements,
}

impl Database {
    pub(crate) async fn stop(self) {
        self.connection_handle.abort();
        match self.connection_handle.await {
            Ok(v) => {
                if let Err(e) = v {
                    log::error!("Database connection task terminated with an error {e:#}.");
                }
            }
            Err(e) => {
                if !e.is_cancelled() {
                    log::error!("Error {e:#} shutting down database connection task.");
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum DatabaseOperation {
    ConcordiumEvents {
        /// Events are from this block.
        block:              BlockInfo,
        /// Events for the given transactions.
        transaction_events: Vec<(TransactionHash, Vec<BridgeEvent>)>,
    },
    EthereumEvents {
        events: ethereum::EthBlockEvents,
    },
    MarkConcordiumTransaction {
        tx_hash: TransactionHash,
        state:   TransactionStatus,
    },
    GetPendingConcordiumTransactions {
        response: tokio::sync::oneshot::Sender<Vec<(TransactionHash, BlockItem<EncodedPayload>)>>,
    },
    StoreEthereumTransaction {
        tx_hash:  H256,
        tx:       ethers::prelude::Bytes,
        response: tokio::sync::oneshot::Sender<(ethers::prelude::Bytes, Vec<u64>)>,
        root:     [u8; 32],
        ids:      Vec<u64>,
    },
    MarkSetMerkleCompleted {
        root:     [u8; 32],
        ids:      Vec<u64>,
        response: tokio::sync::oneshot::Sender<()>,
        success:  bool,
        tx_hash:  H256,
    },
}

#[derive(Debug, tokio_postgres::types::ToSql, tokio_postgres::types::FromSql)]
#[postgres(name = "concordium_event_type")]
pub enum ConcordiumEventType {
    #[postgres(name = "token_map")]
    TokenMap,
    #[postgres(name = "deposit")]
    Deposit,
    #[postgres(name = "withdraw")]
    Withdraw,
    #[postgres(name = "grant_role")]
    GrantRole,
    #[postgres(name = "revoke_role")]
    RevokeRole,
}

impl Database {
    pub async fn new(
        config: &tokio_postgres::Config,
    ) -> anyhow::Result<(Option<u64>, Option<AbsoluteBlockHeight>, Self)> {
        let (client, connection) = config.connect(NoTls).await?;
        let connection_handle = tokio::spawn(connection);
        client.batch_execute(SCHEMA).await?;
        let insert_concordium_tx = client
            .prepare(
                "INSERT INTO concordium_transactions (tx_hash, tx, origin_tx_hash, timestamp, \
                 status) VALUES ($1, $2, $3, $4, $5) RETURNING id",
            )
            .await?;
        let get_pending_concordium_txs = client
            .prepare(
                "SELECT tx_hash, tx FROM concordium_transactions
WHERE status = 'pending' ORDER BY id ASC;",
            )
            .await?;
        let get_pending_ethereum_txs = client
            .prepare(
                "SELECT tx_hash, tx, timestamp FROM ethereum_transactions WHERE status = \
                 'pending' ORDER BY id ASC;",
            )
            .await?;
        let mark_concordium_tx = client
            .prepare(
                "UPDATE concordium_transactions SET status = $2 WHERE tx_hash = $1 RETURNING id;",
            )
            .await?;
        let insert_ethereum_tx = client
            .prepare(
                "INSERT INTO ethereum_transactions (tx_hash, tx, timestamp, status)
VALUES ($1, $2, $3, $4) RETURNING id",
            )
            .await?;
        let insert_concordium_event = client
            .prepare(
                "INSERT INTO concordium_events (tx_hash, event_index, origin_event_index, \
                 event_type, child_index, child_subindex, receiver, amount, event_data, \
                 event_merkle_hash, processed)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        (CASE WHEN $4 = ('withdraw' :: concordium_event_type)
              THEN (SELECT tx_hash FROM ethereum_withdraw_events
                    WHERE ethereum_withdraw_events.origin_event_index = $2
                    LIMIT 1)
              ELSE NULL END))
RETURNING (CASE WHEN processed IS NULL THEN FALSE ELSE TRUE END)",
            )
            .await?;

        let mark_withdrawal_as_completed = client
            .prepare(
                "UPDATE concordium_events SET processed = $1 WHERE event_index = $2 RETURNING id;",
            )
            .await?;

        let get_pending_withdrawals = client
            .prepare(
                "SELECT tx_hash, event_index, event_data FROM concordium_events
WHERE (processed IS NULL) AND event_type = 'withdraw' ORDER BY id ASC;",
            )
            .await?;
        let get_max_event_index = client
            .prepare("SELECT MAX(event_index) FROM concordium_events WHERE (root IS NOT NULL);")
            .await?;
        let ethereum_checkpoint = client
            .query_opt(
                "SELECT last_processed_height FROM checkpoints WHERE network = 'ethereum'",
                &[],
            )
            .await?;
        let ethereum_last_height =
            ethereum_checkpoint.map(|row| row.get::<_, i64>("last_processed_height") as u64);
        let concordium_checkpoint = client
            .query_opt(
                "SELECT last_processed_height FROM checkpoints WHERE network = 'concordium'",
                &[],
            )
            .await?;
        let concordium_last_height =
            concordium_checkpoint.map(|row| row.get::<_, i64>("last_processed_height") as u64);

        let db = Database {
            client,
            connection_handle,
            prepared_statements: PreparedStatements {
                insert_concordium_tx,
                get_pending_concordium_txs,
                get_pending_ethereum_txs,
                mark_concordium_tx,
                insert_ethereum_tx,
                insert_concordium_event,
                get_pending_withdrawals,
                mark_withdrawal_as_completed,
                get_max_event_index,
            },
        };
        Ok((
            ethereum_last_height,
            concordium_last_height.map(AbsoluteBlockHeight::from),
            db,
        ))
    }

    pub async fn insert_ethereum_tx(
        &mut self,
        tx_hash: H256,
        tx: &ethers::prelude::Bytes,
        root: [u8; 32],
        ids: &[u64],
    ) -> anyhow::Result<u64> {
        let timestamp = chrono::Utc::now().timestamp();
        log::debug!("Inserting Ethereum transaction {:#x}.", tx_hash);
        let statements = &self.prepared_statements;
        let db_tx = self.client.transaction().await?;
        let row = db_tx
            .query_one(&statements.insert_ethereum_tx, &[
                &tx_hash.as_bytes(),
                &tx.as_ref(),
                &timestamp,
                &EthTransactionStatus::Pending,
            ])
            .await
            .context("Unable to insert transaction.")?;
        for &id in ids {
            // TODO: Make prepared statement for this.
            db_tx
                .query_opt(
                    "UPDATE concordium_events SET pending_root = $1 WHERE event_index = $2 \
                     RETURNING id",
                    &[&&root[..], &(id as i64)],
                )
                .await?;
        }
        db_tx.commit().await?;
        Ok(row.get::<_, i64>("id") as u64)
    }

    pub async fn mark_merkle_root_set(
        &mut self,
        root: [u8; 32],
        ids: &[u64],
        success: bool,
        tx_hash: H256,
    ) -> anyhow::Result<()> {
        let db_tx = self.client.transaction().await?;
        if success {
            for &id in ids {
                // TODO: Make prepared statement for this.
                db_tx
                    .query_opt(
                        "UPDATE concordium_events SET pending_root = NULL, root = $1 WHERE \
                         event_index = $2 RETURNING id",
                        &[&&root[..], &(id as i64)],
                    )
                    .await?;
            }
            db_tx
                .query_one(
                    "INSERT INTO merkle_roots (root) VALUES ($1) RETURNING id;",
                    &[&&root[..]],
                )
                .await?;
        } else {
            for &id in ids {
                // TODO: Make prepared statement for this.
                db_tx
                    .query_opt(
                        "UPDATE concordium_events SET pending_root = NULL WHERE event_index = $1 \
                         RETURNING id",
                        &[&(id as i64)],
                    )
                    .await?;
            }
        }
        db_tx
            .query_one(
                "UPDATE ethereum_transactions SET status = 'confirmed' WHERE tx_hash = $1 \
                 RETURNING id;",
                &[&tx_hash.as_bytes()],
            )
            .await?;
        db_tx.commit().await?;
        Ok(())
    }

    pub async fn pending_concordium_txs(
        &self,
    ) -> anyhow::Result<Vec<(TransactionHash, BlockItem<EncodedPayload>)>> {
        let rows = self
            .client
            .query(&self.prepared_statements.get_pending_concordium_txs, &[])
            .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let tx_hash: Vec<u8> = row.try_get("tx_hash")?;
            let tx: Vec<u8> = row.try_get("tx")?;
            let tx_hash = tx_hash[..].try_into()?;
            let tx = common::from_bytes(&mut &tx[..])?;
            result.push((tx_hash, tx))
        }
        Ok(result)
    }

    /// Get the transaction hash, the data, and the timestamp when
    /// the transaction was inserted to the database, in case
    /// a pending transaction exists.
    pub async fn pending_ethereum_tx(
        &self,
    ) -> anyhow::Result<Option<(H256, ethers::prelude::Bytes, u64, [u8; 32], Vec<u64>)>> {
        let rows = self
            .client
            .query_opt(&self.prepared_statements.get_pending_ethereum_txs, &[])
            .await?;
        if let Some(row) = rows {
            let tx_hash: Vec<u8> = row.try_get("tx_hash")?;
            let tx: Vec<u8> = row.try_get("tx")?;
            let timestamp = row.try_get::<_, i64>("timestamp")? as u64;
            let pending_idxs = self
                .client
                .query(
                    "SELECT pending_root, event_index FROM concordium_events WHERE pending_root \
                     IS NOT NULL ORDER BY event_index ASC;",
                    &[],
                )
                .await?;
            let mut root = None;
            let mut idxs = Vec::with_capacity(pending_idxs.len());
            for row in pending_idxs {
                let pending_root: [u8; 32] = row
                    .try_get::<_, Vec<u8>>("pending_root")?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Stored value is not a Merkle root hash"))?;
                anyhow::ensure!(
                    root.is_none() || root == Some(pending_root),
                    "Multiple pending Merkle roots. Database invariant violation"
                );
                root = Some(pending_root);
                let event_index = row.try_get::<_, i64>("event_index")? as u64;
                idxs.push(event_index);
            }
            let root = root.context(
                "A pending transaction without any indices. This is a database invariant \
                 violation.",
            )?;
            Ok(Some((
                H256(tx_hash.try_into().map_err(|_| {
                    anyhow::anyhow!("Database invariant violation. Hash not 32 bytes")
                })?),
                tx.into(),
                timestamp,
                root,
                idxs,
            )))
        } else {
            Ok(None)
        }
    }

    /// Get pending withdrawals and check that they indeed exist on the chain.
    /// Additionally return the maximum event index of an event that has been
    /// part of a merkle root.
    pub async fn pending_withdrawals(
        &self,
        mut client: BridgeManagerClient,
    ) -> anyhow::Result<(Option<u64>, Vec<(TransactionHash, WithdrawEvent)>)> {
        let rows = self
            .client
            .query(&self.prepared_statements.get_pending_withdrawals, &[])
            .await?;
        let max_sent = self
            .client
            .query_one(&self.prepared_statements.get_max_event_index, &[])
            .await?;
        let max_sent_event_index = max_sent.try_get::<_, Option<i64>>(0)?.map(|x| x as u64);
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let tx_hash: Vec<u8> = row.try_get("tx_hash")?;
            let tx_hash = tx_hash[..].try_into()?;
            let event_index: i64 = row.try_get("event_index")?;
            let event_index = event_index as u64;
            let data: Vec<u8> = row.try_get("event_data")?;
            let we: WithdrawEvent = contracts_common::from_bytes(&mut &data[..])?;
            let status = client.client.get_block_item_status(&tx_hash).await?;
            if let Some((_block, summary)) = status.is_finalized() {
                let chain_events = client.extract_events(summary)?;
                // Find the event with the given event index. We trust the contract
                // to only have one event for each event index, so using find is safe.
                if let Some(crate::concordium_contracts::BridgeEvent::Withdraw(chain_we)) =
                    chain_events
                        .into_iter()
                        .find(|e| e.event_index() == Some(event_index))
                {
                    anyhow::ensure!(
                        chain_we == we,
                        "Mismatching withdraw event. The database was tampered with. Aborting."
                    );
                    result.push((tx_hash, we))
                } else {
                    anyhow::bail!("Mismatching event. The database was tampered. Aborting.")
                }
            } else {
                anyhow::bail!(
                    "Events for a non-finalized transaction. This should not happen. Aborting."
                )
            }
        }
        Ok((max_sent_event_index, result))
    }

    pub async fn mark_concordium_tx(
        &self,
        tx_hash: TransactionHash,
        state: TransactionStatus,
    ) -> anyhow::Result<bool> {
        let rows = self
            .client
            .query_opt(&self.prepared_statements.mark_concordium_tx, &[
                &tx_hash.as_ref(),
                &state,
            ])
            .await?;
        Ok(rows.is_some())
    }

    pub async fn insert_transactions<P: PayloadLike>(
        &mut self,
        last_block_number: u64,
        txs: &[(H256, BlockItem<P>)],
        // List of event indexes to mark as "done"
        wes: &[(H256, u64, U256, TransactionHash, u64, H160, u64)],
        deposits: &[(H256, u64, U256, H160, H160)],
        // New token maps.
        maps: &[(H160, ContractAddress, String, u8)],
        // Removed token maps.
        unmaps: &[(H160, ContractAddress)],
    ) -> anyhow::Result<()> {
        let statements = &self.prepared_statements;
        let db_tx = self.client.transaction().await?;
        for (origin_tx_hash, tx) in txs {
            statements
                .insert_concordium_tx(&db_tx, origin_tx_hash, tx)
                .await?;
        }
        for (origin_tx_hash, origin_event_index, amount, depositor, root_token) in deposits {
            db_tx
                .query(
                    "INSERT INTO ethereum_deposit_events (origin_tx_hash, origin_event_index, \
                     amount, depositor, root_token, tx_hash)
VALUES ($1, $2, $3, $4, $5, (SELECT tx_hash FROM concordium_events
                    WHERE concordium_events.origin_event_index = $2
                    LIMIT 1));",
                    &[
                        &origin_tx_hash.as_bytes(),
                        &(*origin_event_index as i64),
                        &(amount.to_string()),
                        &depositor.as_bytes(),
                        &root_token.as_bytes(),
                    ],
                )
                .await?;
        }
        for (tx_hash, id, amount, origin_tx_hash, origin_event_id, receiver, event_index) in wes {
            let rv = db_tx
                .query_opt(&statements.mark_withdrawal_as_completed, &[
                    &tx_hash.as_bytes(),
                    &(*event_index as i64),
                ])
                .await?;
            if rv.is_none() {
                log::error!(
                    "Event index {} not in the database. This is a database invariant violation.",
                    event_index
                );
            }
            db_tx
                .query_opt(
                    "INSERT INTO ethereum_withdraw_events (tx_hash, event_index, amount, \
                     receiver, origin_tx_hash, origin_event_index) VALUES ($1, $2, $3, $4, $5, \
                     $6);",
                    &[
                        &&tx_hash.as_ref()[..],
                        &(*id as i64),
                        &amount.to_string(),
                        &receiver.as_bytes(),
                        &&origin_tx_hash.as_ref()[..],
                        &(*origin_event_id as i64),
                    ],
                )
                .await?;
        }
        for (root, child, eth_name, decimals) in maps {
            db_tx
                .query(
                    "INSERT INTO token_maps (root, child_index, child_subindex, eth_name, \
                     decimals) VALUES ($1 , $2, $3, $4, $5);",
                    &[
                        &root.as_bytes(),
                        &(child.index as i64),
                        &(child.subindex as i64),
                        &eth_name,
                        &(*decimals as i16),
                    ],
                )
                .await?;
        }
        for (root, child) in unmaps {
            db_tx
                .query(
                    "DELETE FROM token_maps WHERE root = $1 AND child_index = $2 AND \
                     child_subindex = $3;",
                    &[
                        &root.as_bytes(),
                        &(child.index as i64),
                        &(child.subindex as i64),
                    ],
                )
                .await?;
        }
        db_tx
            .query_opt(
                "INSERT INTO checkpoints VALUES ('ethereum', $1) ON CONFLICT (network) DO UPDATE \
                 SET last_processed_height = $1;",
                &[&(last_block_number as i64)],
            )
            .await
            .context("Unable to insert processed block.")?;
        db_tx.commit().await?;
        Ok(())
    }

    pub async fn insert_concordium_events(
        &mut self,
        block: &BlockInfo,
        events: &[(TransactionHash, Vec<BridgeEvent>)],
    ) -> anyhow::Result<Vec<(u64, [u8; 32])>> {
        let statements = &self.prepared_statements;
        let db_tx = self.client.transaction().await?;
        let mut withdraws = Vec::new();
        for (tx_hash, events) in events {
            for event in events {
                let mh = if let BridgeEvent::Withdraw(we) = &event {
                    Some((
                        we.event_index,
                        crate::merkle::make_event_leaf_hash(*tx_hash, we)?,
                    ))
                } else {
                    None
                };
                let processed = statements
                    .insert_concordium_event(&db_tx, &tx_hash, &event, mh.map(|x| x.1))
                    .await?;
                if !processed {
                    if let Some(p) = mh {
                        withdraws.push(p);
                    };
                }
            }
        }
        db_tx
            .query_opt(
                "INSERT INTO checkpoints VALUES ('concordium', $1) ON CONFLICT (network) DO \
                 UPDATE SET last_processed_height = $1;",
                &[&(block.block_height.height as i64)],
            )
            .await
            .context("Unable to set checkpoint for Concordium events.")?;
        db_tx.commit().await?;
        Ok(withdraws)
    }

    /// Return the maximum nonce of a pending transaction.
    pub async fn submit_missing_txs(
        &self,
        mut client: v2::Client,
    ) -> anyhow::Result<Option<Nonce>> {
        let txs = self.pending_concordium_txs().await?;
        let mut next_nonce = None;
        for (tx_hash, tx) in txs {
            match &tx {
                BlockItem::AccountTransaction(at) => next_nonce = Some(at.header.nonce.next()),
                BlockItem::CredentialDeployment(_) => anyhow::bail!(
                    "Database invariant violation. Credential deployment in the database."
                ),
                BlockItem::UpdateInstruction(_) => anyhow::bail!(
                    "Database invariant violation. Update instruction in the database."
                ),
            }
            let status = client.get_block_item_status(&tx_hash).await;
            match status {
                Ok(_) => (),
                Err(e) if e.is_not_found() => {
                    log::debug!("Submitting missing transaction {}.", tx_hash);
                    client.send_block_item(&tx).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(next_nonce)
    }
}

// // TODO: Revise this to do cleanup instead.
// async fn mark_concordium_txs(
//     db_actions: tokio::sync::mpsc::Sender<DatabaseOperation>,
//     mut client: v2::Client,
//     stop: tokio::sync::watch::Receiver<()>,
// ) -> anyhow::Result<()> {
//     // TODO: We could just be listening for blocks. But if there are no
// pending     // transactions that is not efficient.
//     let mut interval =
// tokio::time::interval(std::time::Duration::from_millis(10000));     interval.
// set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);     while !
// stop.has_changed().unwrap_or(true) {         interval.tick().await;
//         let (response, receiver) = tokio::sync::oneshot::channel();
//         db_actions
//             .send(DatabaseOperation::GetPendingConcordiumTransactions {
// response })             .await?;
//         let txs = receiver.await?;
//         if !txs.is_empty() {
//             log::debug!(
//                 "There are {} pending transactions. Checking them.",
//                 txs.len()
//             );
//         }
//         for (tx_hash, _) in txs {
//             let status = client.get_block_item_status(&tx_hash).await;
//             match status {
//                 Ok(s) => {
//                     if let Some((block, outcome)) = s.is_finalized() {
//                         if outcome.is_success() {
//                             log::debug!("Transaction {} finalized in block
// {}.", tx_hash, block);                             db_actions
//
// .send(DatabaseOperation::MarkConcordiumTransaction {
// tx_hash,                                     state:
// TransactionStatus::Finalized,                                 })
//                                 .await?;
//                         } else {
//                             // TODO: Handle failure in some way.
//                             // Transactions should generally not fail.
//                             log::error!(
//                                 "Transaction {} finalized in block {} but
// failed.",                                 tx_hash,
//                                 block
//                             );
//                             db_actions
//
// .send(DatabaseOperation::MarkConcordiumTransaction {
// tx_hash,                                     state:
// TransactionStatus::Failed,                                 })
//                                 .await?;
//                         }
//                     } // else nothing to do, wait until next time.
//                 }
//                 Err(e) if e.is_not_found() => {
//                     log::error!("A transaction has gone missing {}.",
// tx_hash);                     // TODO: Figure out how to resume. Missing
// transactions will mean failure.                     db_actions
//                         .send(DatabaseOperation::MarkConcordiumTransaction {
//                             tx_hash,
//                             state: TransactionStatus::Missing,
//                         })
//                         .await?;
//                 }
//                 Err(e) => return Err(e.into()),
//             }
//         }
//     }
//     db_actions.closed().await;
//     Ok(())
// }

fn convert_to_token_amount(a: U256) -> cis2::TokenAmount {
    let mut buf = [0u8; 32];
    a.to_little_endian(&mut buf);
    cis2::TokenAmount(BigUint::from_bytes_le(&buf))
}

#[derive(Debug)]
pub enum MerkleUpdate {
    NewWithdraws {
        withdraws: Vec<(u64, [u8; 32])>,
    },
    WithdrawalCompleted {
        receiver:             H160,
        original_event_index: u64,
    },
}

const MAX_CONNECT_ATTEMPTS: u32 = 5;

async fn try_reconnect(
    config: &tokio_postgres::Config,
    stop_flag: &tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<(Option<u64>, Option<AbsoluteBlockHeight>, Database)> {
    let mut i = 1;
    while !stop_flag.has_changed().unwrap_or(true) {
        match Database::new(config).await {
            Ok(db) => return Ok(db),
            Err(e) if i < MAX_CONNECT_ATTEMPTS => {
                let delay = std::time::Duration::from_millis(500 * (1 << i));
                log::error!(
                    "Could not connect to the database due to {:#}. Reconnecting in {}ms.",
                    e,
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                i += 1;
            }
            Err(e) => {
                log::error!(
                    "Could not connect to the database in {} attempts. Last attempt failed with \
                     reason {:#}.",
                    MAX_CONNECT_ATTEMPTS,
                    e
                );
                return Err(e);
            }
        }
    }
    anyhow::bail!("The service was asked to stop.");
}

pub async fn handle_database(
    config: tokio_postgres::Config,
    mut db: Database,
    mut blocks: tokio::sync::mpsc::Receiver<DatabaseOperation>,
    mut bridge_manager: BridgeManager,
    ccd_transaction_sender: tokio::sync::mpsc::Sender<BlockItem<EncodedPayload>>,
    merkle_setter_sender: tokio::sync::mpsc::Sender<MerkleUpdate>,
    mut stop_flag: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    let mut retry = None;

    // Loop until told to stop. If the stop sender has been dropped
    // treat that as if we need to stop as well.
    loop {
        let next_item = if let Some(v) = retry.take() {
            Some(v)
        } else {
            tokio::select! {
                // Make sure to process all events that are in the queue before shutting down.
                // Thus prioritize getting things from the channel.
                // This only works in combination with the fact that we shut down senders
                // upon receving a kill signal, so the receiver will be drained eventually.
                biased;
                x = blocks.recv() => x,
                _ = stop_flag.changed() => None,
            }
        };
        if let Some(action) = next_item {
            match insert_into_db(
                &mut db,
                action,
                &merkle_setter_sender,
                &ccd_transaction_sender,
                &mut bridge_manager,
            )
            .await
            {
                Ok(()) => {
                    log::trace!("Processed database operation.");
                }
                Err(InsertError::Retry(action)) => {
                    let delay = std::time::Duration::from_millis(2000);
                    log::error!(
                        "Could not insert into the database. Reconnecting in {}ms.",
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                    let new_db = match try_reconnect(&config, &stop_flag).await {
                        Ok(db) => db.2,
                        Err(e) => {
                            blocks.close();
                            db.stop().await;
                            return Err(e);
                        }
                    };
                    let old_db = std::mem::replace(&mut db, new_db);
                    old_db.connection_handle.abort();
                    match old_db.connection_handle.await {
                        Ok(v) => {
                            if let Err(e) = v {
                                log::warn!(
                                    "Could not correctly stop the old database connection due to: \
                                     {}.",
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            if e.is_panic() {
                                log::warn!(
                                    "Could not correctly stop the old database connection. The \
                                     connection thread panicked: {e:#}."
                                );
                            } else if !e.is_cancelled() {
                                log::warn!("Could not correctly stop the old database connection.");
                            }
                        }
                    }
                    retry = Some(action);
                }
                Err(other) => {
                    log::debug!(
                        "One of the internal channels was closed ({:#}). Closing the database \
                         worker.",
                        other
                    );
                    // One of the channels closed. Terminate.
                    break;
                }
            }
        } else {
            break;
        }
    }
    blocks.close();
    db.stop().await;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum InsertError {
    #[error("Other error {0:#}")]
    Other(#[from] anyhow::Error),
    #[error("Retry.")]
    Retry(DatabaseOperation),
}

async fn insert_into_db(
    db: &mut Database,
    action: DatabaseOperation,
    merkle_setter_sender: &tokio::sync::mpsc::Sender<MerkleUpdate>,
    ccd_transaction_sender: &tokio::sync::mpsc::Sender<BlockItem<EncodedPayload>>,
    bridge_manager: &mut BridgeManager,
) -> Result<(), InsertError> {
    match action {
        DatabaseOperation::ConcordiumEvents {
            block,
            transaction_events,
        } => {
            match db
                .insert_concordium_events(&block, &transaction_events)
                .await
            {
                Ok(withdraws) => {
                    if !withdraws.is_empty() {
                        if merkle_setter_sender
                            .send(MerkleUpdate::NewWithdraws { withdraws })
                            .await
                            .is_err()
                        {
                            log::warn!(
                                "Unable to send new withdraw events to the Merkle updated since \
                                 the channel is closed."
                            )
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Database error when trying to insert Concordium events: {e}.");
                    return Err(InsertError::Retry(DatabaseOperation::ConcordiumEvents {
                        block,
                        transaction_events,
                    }));
                }
            }
        }
        DatabaseOperation::EthereumEvents { events } => {
            let mut wes = Vec::new();
            let mut txs = Vec::with_capacity(events.events.len());
            let mut maps = Vec::new();
            let mut unmaps = Vec::new();
            let mut deposits = Vec::new();
            for event in &events.events {
                match event.event {
                    ethereum::EthEvent::TokenLocked {
                        id,
                        depositor,
                        deposit_receiver,
                        root_token,
                        vault: _,
                        amount,
                    } => {
                        // Send transaction to Concordium.
                        let deposit = concordium_contracts::DepositOperation {
                            id:       id.low_u64(),
                            user:     deposit_receiver.into(),
                            root:     root_token.into(),
                            amount:   convert_to_token_amount(amount),
                            // TODO: Hardcoded token ID. Works with contracts as they are
                            // now, but is not ideal.
                            token_id: cis2::TokenId::new_unchecked(vec![0u8; 8]),
                        };
                        let update = concordium_contracts::StateUpdate::Deposit(deposit);
                        // TODO estimate execution energy.
                        let tx = bridge_manager.make_state_update_tx(100_000.into(), &update);
                        txs.push((event.tx_hash, tx));
                        deposits.push((event.tx_hash, id.low_u64(), amount, depositor, root_token));
                    }
                    ethereum::EthEvent::TokenMapped {
                        id,
                        root_token,
                        child_token,
                        token_type: _,
                        ref name,
                        decimals,
                    } => {
                        // Send transaction to Concordium.
                        let map = concordium_contracts::TokenMapOperation {
                            id:    id.low_u64(),
                            root:  root_token.into(),
                            child: child_token,
                        };
                        let update = concordium_contracts::StateUpdate::TokenMap(map);
                        let tx = bridge_manager.make_state_update_tx(100_000.into(), &update);
                        txs.push((event.tx_hash, tx));
                        maps.push((root_token, child_token, name.clone(), decimals));
                    }
                    ethereum::EthEvent::TokenUnmapped {
                        id,
                        root_token,
                        child_token,
                        token_type: _,
                    } => {
                        // Do nothing at present. Manual intervention needed.
                        log::warn!("Token {id} ({root_token} -> {child_token}) unmapped.");
                        unmaps.push((root_token, child_token));
                    }
                    ethereum::EthEvent::Withdraw {
                        id,
                        child_token: _,
                        amount,
                        receiver,
                        origin_tx_hash,
                        origin_event_index,
                        child_token_id: _,
                    } => {
                        wes.push((
                            event.tx_hash,
                            id.low_u64(),
                            amount,
                            origin_tx_hash,
                            origin_event_index,
                            receiver,
                            origin_event_index,
                        ));
                    }
                }
            }

            match db
                .insert_transactions(events.last_number, &txs, &wes, &deposits, &maps, &unmaps)
                .await
            {
                Ok(()) => {
                    for (_, _, _, _, _, receiver, we) in wes {
                        if merkle_setter_sender
                            .send(MerkleUpdate::WithdrawalCompleted {
                                original_event_index: we,
                                receiver,
                            })
                            .await
                            .is_err()
                        {
                            {
                                log::warn!(
                                    "Unable to send completed withdrawal to the Merkle updater. \
                                     The channel is closed."
                                )
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Database error when trying to insert transactions: {e}.");
                    return Err(InsertError::Retry(DatabaseOperation::EthereumEvents {
                        events,
                    }));
                }
            }

            // We have now written all the transactions to the database. Now send them to
            // the Concordium node.
            for (_, tx) in txs {
                let hash = tx.hash();
                if ccd_transaction_sender.send(tx).await.is_err() {
                    {
                        log::warn!(
                            "Unable to send transctions stored in the database to the node since \
                             the channel is closed."
                        )
                    }
                } else {
                    log::info!("Enqueued transaction {}.", hash);
                }
            }
        }
        DatabaseOperation::MarkConcordiumTransaction { tx_hash, state } => {
            log::debug!("Marking {} as {:?}.", tx_hash, state);
            if let Err(e) = db.mark_concordium_tx(tx_hash, state).await {
                log::warn!("Database error: {e}");
                return Err(InsertError::Retry(
                    DatabaseOperation::MarkConcordiumTransaction { tx_hash, state },
                ));
            }
        }
        DatabaseOperation::GetPendingConcordiumTransactions { response } => {
            match db.pending_concordium_txs().await {
                Ok(txs) => {
                    if response.send(txs).is_err() {
                        log::warn!(
                            "Unable to send response to the sender of \
                             GetPendingConcordiumTransactions, indicating they have stopped."
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Database error when trying to get pending Concordium transactions: {e}."
                    );
                    return Err(InsertError::Retry(
                        DatabaseOperation::GetPendingConcordiumTransactions { response },
                    ));
                }
            }
        }
        DatabaseOperation::StoreEthereumTransaction {
            tx_hash,
            tx,
            response,
            ids,
            root,
        } => {
            if db
                .insert_ethereum_tx(tx_hash, &tx, root, &ids)
                .await
                .is_ok()
            {
                if response.send((tx, ids)).is_err() {
                    log::warn!("Unable to send response StoreEthereumTransaction. Continuing.");
                }
            } else {
                return Err(InsertError::Retry(
                    DatabaseOperation::StoreEthereumTransaction {
                        tx_hash,
                        tx,
                        response,
                        root,
                        ids,
                    },
                ));
            }
        }
        DatabaseOperation::MarkSetMerkleCompleted {
            root,
            ids,
            response,
            success,
            tx_hash,
        } => {
            if db
                .mark_merkle_root_set(root, &ids, success, tx_hash)
                .await
                .is_ok()
            {
                if response.send(()).is_err() {
                    log::warn!("Unable to send response MarkSetMerkleCompleted. Continuing.")
                }
            } else {
                return Err(InsertError::Retry(
                    DatabaseOperation::MarkSetMerkleCompleted {
                        root,
                        ids,
                        response,
                        success,
                        tx_hash,
                    },
                ));
            }
        }
    }
    Ok(())
}
