use crate::{
    concordium_contracts::{
        self, BridgeEvent, BridgeManager, BridgeManagerClient, DatabaseOperation, WithdrawEvent,
    },
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

#[derive(Debug, Copy, Clone, tokio_postgres::types::ToSql, tokio_postgres::types::FromSql, serde::Serialize)]
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
}

impl PreparedStatements {
    pub async fn insert_concordium_tx<'a, 'b, Payload: PayloadLike>(
        &'a self,
        db_tx: &Transaction<'b>,
        origin_tx: &H256,
        origin_depositor: &Option<H160>,
        origin_event_index: u64,
        bi: &BlockItem<Payload>,
    ) -> anyhow::Result<i64> {
        let hash = bi.hash();
        let timestamp = chrono::Utc::now().timestamp();
        let tx_bytes = to_bytes(bi);
        let res = db_tx
            .query_one(&self.insert_concordium_tx, &[
                &&hash.as_ref()[..],
                &origin_tx.as_bytes(),
                &origin_depositor.as_ref().map(|x| x.as_bytes()),
                &(origin_event_index as i64),
                &tx_bytes,
                &timestamp,
                &TransactionStatus::Pending,
            ])
            .await?;
        Ok(res.get::<_, i64>(0))
    }

    pub async fn insert_concordium_event<'a, 'b>(
        &'a self,
        db_tx: &Transaction<'b>,
        tx_hash: &TransactionHash,
        event: &BridgeEvent,
        merkle_event_hash: Option<[u8; 32]>,
    ) -> anyhow::Result<i64> {
        let (event_type, data) = match event {
            BridgeEvent::TokenMap(tm) => (
                ConcordiumEventType::TokenMap,
                contracts_common::to_bytes(tm),
            ),
            BridgeEvent::Deposit(de) => {
                (ConcordiumEventType::Deposit, contracts_common::to_bytes(de))
            }
            BridgeEvent::Withdraw(we) => (
                ConcordiumEventType::Withdraw,
                contracts_common::to_bytes(we),
            ),
            BridgeEvent::GrantRole(gr) => (
                ConcordiumEventType::GrantRole,
                contracts_common::to_bytes(gr),
            ),
            BridgeEvent::RevokeRole(rr) => (
                ConcordiumEventType::RevokeRole,
                contracts_common::to_bytes(rr),
            ),
        };
        let res = db_tx
            .query_one(&self.insert_concordium_event, &[
                &&tx_hash.as_ref()[..],
                &event.event_index().map(|x| x as i64),
                &event_type,
                &data,
                &None::<Vec<u8>>,
                &merkle_event_hash.map(|x| x.to_vec()),
            ])
            .await?;
        Ok(res.get::<_, i64>(0))
    }
}

pub struct Database {
    pub client:          tokio_postgres::Client,
    connection_handle:   JoinHandle<Result<(), tokio_postgres::Error>>,
    prepared_statements: PreparedStatements,
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
        config: tokio_postgres::Config,
    ) -> anyhow::Result<(Option<u64>, Option<AbsoluteBlockHeight>, Self)> {
        let (client, connection) = config.connect(NoTls).await?;
        let connection_handle = tokio::spawn(connection);
        client.batch_execute(SCHEMA).await?;
        let insert_concordium_tx = client
            .prepare(
                "INSERT INTO concordium_transactions (tx_hash, origin_tx_hash, \
                 origin_tx_depositor, origin_event_index, tx, timestamp, status)
 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
            )
            .await?;
        let get_pending_concordium_txs = client
            .prepare(
                "SELECT tx_hash, tx FROM concordium_transactions WHERE status = 'pending' ORDER \
                 BY id ASC;",
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
                "INSERT INTO ethereum_transactions (tx_hash, tx, timestamp, status) VALUES ($1, \
                 $2, $3, $4) RETURNING id",
            )
            .await?;
        let insert_concordium_event = client
            .prepare(
                "INSERT INTO concordium_events (tx_hash, event_index, event_type, event_data, \
                 processed, event_merkle_hash) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
            )
            .await?;

        let mark_withdrawal_as_completed = client
            .prepare(
                "UPDATE concordium_events SET processed = $1 WHERE event_index = $2 RETURNING id;",
            )
            .await?;

        let get_pending_withdrawals = client
            .prepare(
                "SELECT tx_hash, event_index, event_data FROM concordium_events WHERE (processed \
                 IS NULL) AND event_type = 'withdraw' ORDER BY id ASC;",
            )
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
        log::debug!("Inserting Ethereum transaction {}.", tx_hash);
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
            .query_one(
                "SELECT MAX(event_index) FROM concordium_events WHERE (root IS NOT NULL);",
                &[],
            )
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
            if let Some((block, summary)) = status.is_finalized() {
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
        txs: &[(H256, Option<H160>, u64, BlockItem<P>)],
        // List of event indexes to mark as "done"
        wes: &[(H256, u64, U256, TransactionHash, u64, H160, u64)],
        // New token maps.
        maps: &[(H160, ContractAddress, String, u8)],
        // Removed token maps.
        unmaps: &[(H160, ContractAddress)],
    ) -> anyhow::Result<()> {
        let statements = &self.prepared_statements;
        let db_tx = self.client.transaction().await?;
        for (origin_tx, origin_sender, origin_event_index, tx) in txs {
            statements
                .insert_concordium_tx(&db_tx, origin_tx, origin_sender, *origin_event_index, tx)
                .await?;
        }
        for (tx_hash, id, amount, origin_tx_hash, origin_event_id, receiver, event_index) in wes {
            let rv = db_tx
                .query_opt(&statements.mark_withdrawal_as_completed, &[
                    &receiver.as_bytes(),
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
        block: BlockInfo,
        events: Vec<(TransactionHash, Vec<BridgeEvent>)>,
    ) -> anyhow::Result<Vec<(u64, [u8; 32])>> {
        let statements = &self.prepared_statements;
        let db_tx = self.client.transaction().await?;
        let mut withdraws = Vec::new();
        for (tx_hash, events) in events {
            for event in events {
                let merkle_event_hash = if let BridgeEvent::Withdraw(we) = &event {
                    let hash = crate::merkle::make_event_leaf_hash(tx_hash, we)?;
                    withdraws.push((we.event_index, hash));
                    Some(hash)
                } else {
                    None
                };
                statements
                    .insert_concordium_event(&db_tx, &tx_hash, &event, merkle_event_hash)
                    .await?;
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

pub async fn mark_concordium_txs(
    db_actions: tokio::sync::mpsc::Sender<DatabaseOperation>,
    mut client: v2::Client,
) -> anyhow::Result<()> {
    // TODO: We could just be listening for blocks. But if there are no pending
    // transactions that is not efficient.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(10000));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let (response, receiver) = tokio::sync::oneshot::channel();
        db_actions
            .send(DatabaseOperation::GetPendingConcordiumTransactions { response })
            .await?;
        let txs = receiver.await?;
        if !txs.is_empty() {
            log::debug!(
                "There are {} pending transactions. Checking them.",
                txs.len()
            );
        }
        for (tx_hash, _) in txs {
            let status = client.get_block_item_status(&tx_hash).await;
            match status {
                Ok(s) => {
                    if let Some((block, outcome)) = s.is_finalized() {
                        if outcome.is_success() {
                            log::debug!("Transaction {} finalized in block {}.", tx_hash, block);
                            db_actions
                                .send(DatabaseOperation::MarkConcordiumTransaction {
                                    tx_hash,
                                    state: TransactionStatus::Finalized,
                                })
                                .await?;
                        } else {
                            // TODO: Handle failure in some way.
                            // Transactions should generally not fail.
                            log::error!(
                                "Transaction {} finalized in block {} but failed.",
                                tx_hash,
                                block
                            );
                            db_actions
                                .send(DatabaseOperation::MarkConcordiumTransaction {
                                    tx_hash,
                                    state: TransactionStatus::Failed,
                                })
                                .await?;
                        }
                    } // else nothing to do, wait until next time.
                }
                Err(e) if e.is_not_found() => {
                    log::error!("A transaction has gone missing {}.", tx_hash);
                    // TODO: Figure out how to resume. Missing transactions will mean failure.
                    db_actions
                        .send(DatabaseOperation::MarkConcordiumTransaction {
                            tx_hash,
                            state: TransactionStatus::Missing,
                        })
                        .await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

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

pub async fn handle_database(
    mut db: Database,
    mut blocks: tokio::sync::mpsc::Receiver<DatabaseOperation>,
    mut bridge_manager: BridgeManager,
    ccd_transaction_sender: tokio::sync::mpsc::Sender<BlockItem<EncodedPayload>>,
    merkle_setter_sender: tokio::sync::mpsc::Sender<MerkleUpdate>,
) -> anyhow::Result<()> {
    while let Some(action) = blocks.recv().await {
        match action {
            DatabaseOperation::ConcordiumEvents {
                block,
                transaction_events,
            } => {
                let withdraws = db
                    .insert_concordium_events(block, transaction_events)
                    .await?;
                if !withdraws.is_empty() {
                    merkle_setter_sender
                        .send(MerkleUpdate::NewWithdraws { withdraws })
                        .await?
                }
            }
            DatabaseOperation::EthereumEvents { events } => {
                let mut wes = Vec::new();
                let mut txs = Vec::with_capacity(events.events.len());
                let mut maps = Vec::new();
                let mut unmaps = Vec::new();
                for event in events.events {
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
                                // TODO: Hardcoded token ID. Works with contracts as they are now,
                                // but is not ideal.
                                token_id: cis2::TokenId::new_unchecked(vec![0u8; 8]),
                            };
                            let update = concordium_contracts::StateUpdate::Deposit(deposit);
                            // TODO estimate execution energy.
                            let tx = bridge_manager.make_state_update_tx(100_000.into(), &update);
                            txs.push((event.tx_hash, Some(depositor), id.low_u64(), tx));
                        }
                        ethereum::EthEvent::TokenMapped {
                            id,
                            root_token,
                            child_token,
                            token_type: _,
                            name,
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
                            txs.push((event.tx_hash, None, id.low_u64(), tx));
                            maps.push((root_token, child_token, name, decimals));
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

                db.insert_transactions(events.last_number, &txs, &wes, &maps, &unmaps)
                    .await?;
                for (_, _, _, _, _, receiver, we) in wes {
                    merkle_setter_sender
                        .send(MerkleUpdate::WithdrawalCompleted {
                            original_event_index: we,
                            receiver,
                        })
                        .await?;
                }

                // We have now written all the transactions to the database. Now send them to
                // the Concordium node.
                for (_, _, _, tx) in txs {
                    let hash = tx.hash();
                    ccd_transaction_sender.send(tx).await?;
                    log::info!("Enqueued transaction {}.", hash);
                }
            }
            DatabaseOperation::MarkConcordiumTransaction { tx_hash, state } => {
                log::debug!("Marking {} as {:?}.", tx_hash, state);
                db.mark_concordium_tx(tx_hash, state).await?;
            }
            DatabaseOperation::GetPendingConcordiumTransactions { response } => {
                let txs = db.pending_concordium_txs().await?;
                response
                    .send(txs)
                    .map_err(|_| anyhow::anyhow!("Unable to send response. Terminating."))?;
            }
            DatabaseOperation::StoreEthereumTransaction {
                tx_hash,
                tx,
                response,
                ids,
                root,
            } => {
                let _id = db.insert_ethereum_tx(tx_hash, &tx, root, &ids).await?;
                response.send((tx, ids)).map_err(|_| {
                    anyhow::anyhow!(
                        "Unable to send response StoreEthereumTransaction. Terminating."
                    )
                })?;
            }
            DatabaseOperation::MarkSetMerkleCompleted {
                root,
                ids,
                response,
                success,
                tx_hash,
            } => {
                db.mark_merkle_root_set(root, &ids, success, tx_hash)
                    .await?;
                response.send(()).map_err(|_| {
                    anyhow::anyhow!("Unable to send response MarkSetMerkleCompleted. Terminating.")
                })?;
            }
        }
    }
    Ok(())
}
