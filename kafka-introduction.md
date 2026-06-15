# Apache Kafka Introduction

### Q1. What is Apache Kafka?

**Answer:**
Apache Kafka is a **distributed event-streaming platform** designed to publish, subscribe, store, and process streams of records in **real time**. It was originally developed at LinkedIn and open-sourced in 2011.

Key characteristics:
- **High throughput** — handles millions of messages per second
- **Fault tolerant** — data is replicated across brokers
- **Durable** — messages are persisted to disk
- **Scalable** — horizontally scalable via partitions
- **Decoupled** — producers and consumers are completely independent

**Flow:**
```
Producer ──► Kafka (Broker Cluster) ──► Consumer
```

**Real-world analogy:** Think of Kafka like a postal system — senders (producers) drop letters (messages) into mailboxes (topics), and recipients (consumers) collect them at their own pace.

**Use Cases:**
- Real-time analytics (fraud detection, monitoring dashboards)
- Event sourcing and CQRS
- Log aggregation
- Microservices communication
- Data pipeline / ETL

---

### Q2. What is Kafka's Architecture?

**Answer:**
Kafka architecture has 4 main components:

| Component | Role |
|---|---|
| **Producer** | Sends (publishes) messages to topics |
| **Broker** | Kafka server that stores and serves messages |
| **Topic/Partition** | Logical channel for messages, split into partitions |
| **Consumer** | Reads messages from topics |

Brokers are grouped into a **Cluster**, coordinated by either **ZooKeeper** (legacy) or **KRaft** (modern, built-in).

**Flow:**
```
Producer ──► Broker Cluster (Broker1, Broker2, Broker3) ──► Consumer
                     │
              Topics/Partitions
              (replicated across brokers)
```

**Deep Dive:**
- A topic can have N partitions spread across brokers
- Each partition has one **leader** broker (handles reads/writes) and N **follower** brokers (replicas)
- Consumers connect to the leader of each partition they subscribe to

---

### Q3. What is a Topic & Partition?

**Answer:**

**Topic:**
A **Topic** is a named, logical channel/category to which producers publish messages and from which consumers read. Think of it as a database table or a folder.

**Partition:**
Each topic is split into **Partitions** — ordered, immutable, append-only logs. Partitions enable:
- **Parallelism** — multiple consumers can read different partitions simultaneously
- **Horizontal scaling** — partitions spread across brokers

**Diagram:**
```
Topic: "orders"
  ├── Partition 0: [msg0] [msg1] [msg2] [msg3] ...
  ├── Partition 1: [msg0] [msg1] [msg2] [msg3] ...
  └── Partition 2: [msg0] [msg1] [msg2] [msg3] ...
```

**Key Rules:**
- Messages within a partition are strictly ordered
- Ordering is NOT guaranteed across partitions
- Partitions are the unit of parallelism in Kafka
- More partitions = more parallelism = higher throughput

**Interview Tip:** If you need strict ordering for a specific entity (e.g., all orders from user_id=123 in order), always use a **message key** — Kafka routes same-key messages to the same partition.

---

### Q4. What is an Offset?

**Answer:**
An **Offset** is a **unique sequential ID** assigned to each message within a partition. It starts at 0 and increments by 1 for each new message.

**Why offsets matter:**
- Consumers use offsets to track **exactly which messages they've already read**
- Consumers **commit offsets** to Kafka (or an external store) so they can resume from where they left off after a restart or failure

**Diagram:**
```
Partition 0:
  [0] [1] [2] [3] [4] [5]
                ▲
           read here (offset=3, next to read=4)
```

**Types of Offset Commit:**
- **Auto-commit** (`enable.auto.commit=true`) — periodic, easy but risks re-processing
- **Manual commit** — `commitSync()` or `commitAsync()` — precise control

**Key insight:** Offsets are **per-consumer-group per-partition**. Different consumer groups have independent offsets for the same topic — they don't interfere with each other.

---

### Q5. What is a Producer?

**Answer:**
A **Producer** is a client application that **publishes (writes) messages** to Kafka topics.

**How it works:**
1. Producer connects to a broker (bootstrap server)
2. Fetches metadata — which broker is the leader for which partition
3. Serializes the message (key + value)
4. Chooses a partition (by key hash, round-robin, or custom partitioner)
5. Batches messages and sends to the leader broker
6. Waits for acknowledgment (`acks`) based on config

**Flow:**
```
Producer ──► Partition 0 (Leader on Broker1)
         ──► Partition 1 (Leader on Broker2)
```

**Key Producer Configs:**

| Config | Meaning |
|---|---|
| `acks=0` | Fire and forget — no confirmation |
| `acks=1` | Leader acknowledges — moderate safety |
| `acks=all` | Leader + all ISR replicas — strongest guarantee |
| `retries` | Number of retry attempts on failure |
| `batch.size` | Max bytes per batch |
| `linger.ms` | Wait time before sending a batch |
| `compression.type` | snappy/gzip/lz4 for throughput |

---

### Q6. What is a Consumer Group?

**Answer:**
A **Consumer Group** is a set of consumers sharing the same `group.id`. They work together to consume a topic in parallel.

**Core Rule:** Each partition is assigned to **only one consumer** within a group at a time — this gives **load balancing** and **parallel reads**.

**Diagram:**
```
Topic "orders" (3 partitions):

  P0 ──► Consumer 1 ┐
  P1 ──► Consumer 1 │  Consumer Group "order-service"
  P2 ──► Consumer 2 ┘
```

**Scenarios:**
- **consumers < partitions** → some consumers handle multiple partitions
- **consumers = partitions** → ideal, 1:1 mapping
- **consumers > partitions** → extra consumers are idle (wasted)

**Why Consumer Groups are powerful:**
- Multiple independent applications (different group.ids) can each consume the same topic independently — Kafka sends all messages to all groups
- Within a group, work is distributed (each message processed once)

---

### Q7. What is a Broker & Cluster?

**Answer:**
A **Broker** is a single Kafka server — it stores messages in topics/partitions and serves read/write requests from producers and consumers.

A **Cluster** is a group of multiple brokers working together, providing:
- **Scalability** — distribute partitions across brokers
- **Fault tolerance** — replicas on multiple brokers so if one fails, others continue

**Diagram:**
```
Kafka Cluster
┌─────────────────────────────────┐
│  Broker 1  │  Broker 2  │  Broker 3  │
│ (Leader P0)│ (Leader P1)│ (Leader P2)│
│ (Replica P1│ (Replica P2│ (Replica P0│
└─────────────────────────────────┘
```

**Key Points:**
- One broker is the **Controller** (manages partition leadership election)
- Brokers are identified by a unique `broker.id`
- Producers/Consumers only need to know a few bootstrap brokers to discover the whole cluster

---

### Q8. What is Replication & ISR?

**Answer:**
**Replication** is Kafka's mechanism to copy partitions across multiple brokers for fault tolerance.

**Leader & Followers:**
- Each partition has exactly **one Leader** — handles all reads and writes
- **Followers** are replicas that passively replicate data from the leader
- If the leader fails, one of the followers is elected the new leader

**ISR (In-Sync Replicas):**
ISR is the set of replicas that are fully caught up with the leader. Only ISR members are eligible to become leaders.

**Flow:**
```
Producer ──► Leader Broker
               │
               ├──► Follower 1 (ISR) ──replicate──►
               └──► Follower 2 (ISR) ──replicate──►
```

**Key Configs:**

| Config | Meaning |
|---|---|
| `replication.factor` | How many copies of each partition (typically 3) |
| `min.insync.replicas` | Min ISR members needed for a write to succeed |
| `unclean.leader.election` | Allow out-of-sync replica to become leader (risks data loss) |

**Rule of thumb:** `replication.factor=3`, `min.insync.replicas=2`, `acks=all` → zero data loss guarantee.

---

## 🔵 Section 3: Reliability & Delivery

---

### Q9. ZooKeeper vs KRaft

**Answer:**

| Feature | ZooKeeper | KRaft |
|---|---|---|
| Role | External metadata manager | Built into Kafka (Raft protocol) |
| Architecture | Separate ZK cluster needed | No external dependency |
| Complexity | High (manage two systems) | Low (single system) |
| Performance | Metadata bottleneck at scale | Better scalability |
| Status | Legacy (being removed) | Modern (Kafka 2.8+ preview, 3.3+ stable) |

**ZooKeeper (legacy) Flow:**
```
ZooKeeper ◄──► Kafka Brokers (metadata sync)
```

**KRaft (modern) Flow:**
```
Kafka Brokers ◄──► Built-in Raft Quorum Controller
(no external dependency)
```

**Interview Tip:** KRaft was introduced because ZooKeeper caused scaling bottlenecks beyond ~200k partitions. From Kafka 3.3+, KRaft is production-ready. ZooKeeper mode is deprecated and will be removed in Kafka 4.0.

---

### Q10. What are Producer Acks?

**Answer:**
`acks` is a producer config that controls **how many broker acknowledgments the producer requires** before considering a write successful. It's a **durability vs. latency trade-off**.

**Three Modes:**

| acks | Meaning | Risk | Throughput |
|---|---|---|---|
| `0` | Fire-and-forget — no ack | Data loss possible | Highest |
| `1` | Leader ack only | Loss if leader crashes before replication | Medium |
| `all` (`-1`) | Leader + all ISR ack | No data loss (with min.insync.replicas≥2) | Lowest |

**Flow for acks=all:**
```
Producer ──► Leader ──► ISR Replica 1
                    └──► ISR Replica 2
                    └── ack to Producer (only after all ISR confirm)
```

**When to use what:**
- `acks=0` — metrics, logs where some loss is acceptable
- `acks=1` — moderate importance, latency-sensitive
- `acks=all` — financial transactions, critical events

---

### Q11. What is Partitioning & Keys?

**Answer:**
When a producer sends a message with a **key**, Kafka applies a hash function on the key to determine which partition the message goes to.

**Formula:**
```
partition = hash(key) % num_partitions
```

**Why it matters:**
- Same key → always same partition → **ordering preserved per key**
- Useful for entity-based ordering (e.g., all events for `user_id=42` in order)

**Flow:**
```
Key ──► hash() ──► Partition Number
```

**Without key:** Kafka uses round-robin across partitions (no ordering guarantee).

**Real Example:**
```
key="user_123" → hash → Partition 2
key="user_456" → hash → Partition 0
key="user_123" → hash → Partition 2 (same as before — ordering preserved!)
```

**Gotcha:** Changing the number of partitions breaks the key-to-partition mapping for existing keys.

---

### Q12. What is Consumer Rebalancing?

**Answer:**
**Consumer Rebalancing** is the process Kafka triggers when the consumer group membership changes — when consumers **join** (scale up) or **leave** (crash/scale down).

During rebalancing, **partitions are reassigned** across the current live consumers.

**Before Rebalance (2 consumers):**
```
Consumer 1: P0, P1
Consumer 2: P2, P3
```

**After Rebalance (3 consumers join):**
```
Consumer 1: P0
Consumer 2: P1
Consumer 3: P2, P3
```

**The Problem:**
Rebalancing causes a **stop-the-world pause** — all consumers stop processing during rebalance. This can cause latency spikes.

**Solutions:**
- **Cooperative (Incremental) Rebalancing** — only affected partitions are reassigned, others keep processing (Kafka 2.4+ with `CooperativeStickyAssignor`)
- **Static Group Membership** (`group.instance.id`) — consumers retain partition assignments across restarts, avoiding unnecessary rebalances

---

## 🟠 Section 4: Storage & Processing

---

### Q13. What is Kafka's Retention Policy?

**Answer:**
Kafka retains messages for a configured **time or size** — unlike traditional queues, messages are **not deleted after consumption**. Multiple consumer groups can re-read the same data.

**Retention Types:**

| Type | Config | Default |
|---|---|---|
| Time-based | `log.retention.hours` | 168 hours (7 days) |
| Size-based | `log.retention.bytes` | -1 (unlimited) |
| Both | Whichever limit hit first triggers deletion | — |

**How deletion works:**
```
Segment 0 (oldest):  [0][1][2] ──► deleted after retention period
Segment 1:           [3][4][5] ──► being read
Segment 2 (newest):  [6][7][8] ──► active writes
```

Kafka deletes entire **log segments** (not individual messages).

**Key use cases:**
- **Replay** — new consumer can start from offset 0 and reprocess all history
- **Audit** — retain 90 days of events for compliance
- **Decoupling** — slow consumers aren't a problem as long as they catch up within retention window

---

### Q14. What is Log Compaction?

**Answer:**
Log Compaction is a Kafka feature that retains only the **latest value per key**, deleting older duplicate entries. The log is compacted — not truncated by time/size.

**Before Compaction:**
```
[k1:v1] [k2:v1] [k1:v2] [k3:v1]
```

**After Compaction (latest value per key retained):**
```
[k2:v1] [k1:v2] [k3:v1]
```

**Flow:**
```
Before: k1→v1, k2→v1, k1→v2, k3→v1
After:  k2→v1, k1→v2, k3→v1  (old k1:v1 deleted)
```

**Ideal Use Cases:**
- **Database changelog** (CDC) — track latest state of each DB row
- **User preferences** — only the latest preference matters
- **Materialized views** — rebuild current state from Kafka

**Config:** `log.cleanup.policy=compact`

**Gotcha:** Null value (tombstone) = delete the key from the compacted log entirely.

---

### Q15. What is Offset Commit?

**Answer:**
Offset commit is how consumers **record their progress** — they commit the offset of the last successfully processed message so they can resume from there on restart.

**Two Types:**

**1. Auto-Commit (`enable.auto.commit=true`):**
```
Consumer ──► periodic auto-commit every 5s ──► Kafka __consumer_offsets topic
```
- Easy but risky: if consumer crashes between poll and commit, messages get reprocessed

**2. Manual Commit:**
```
Consumer ──► process message ──► commitSync() or commitAsync() ──► Kafka
```
- `commitSync()` — blocking, retries on failure, slower
- `commitAsync()` — non-blocking, faster, no automatic retry

**Flow:**
```
Consumer ──► poll() ──► process ──► commit(offset) ──► __consumer_offsets
```

**At-least-once delivery** (most common): commit after processing
**At-most-once delivery**: commit before processing (risk of loss)
**Exactly-once**: use EOS (see Q16)

---

### Q16. What is Exactly-Once Semantics (EOS)?

**Answer:**
Exactly-Once Semantics (EOS) guarantees that each message is **processed exactly once** — no duplicates, no data loss — even in the face of retries, failures, or network issues.

**Two components required:**

**1. Idempotent Producer:**
- Each message has a sequence number
- Broker deduplicates retries with the same sequence number
- Config: `enable.idempotence=true`

**2. Transactions:**
- Producer opens a transaction, writes to multiple topics/partitions atomically
- Either all writes commit or all abort
- Config: `transactional.id=<unique-id>`

**Flow:**
```
Idempotent Producer ──► Broker (deduplicates) ──► No Duplicates
         +
    Transaction API ──► Atomic read-process-write
```

**Full EOS Pattern (read-process-write):**
```
Consumer reads from Topic A
    └──► Process
         └──► Producer writes to Topic B (within transaction)
              └──► commitTransaction()
```

**Consumer side:** Set `isolation.level=read_committed` to only read committed transactional messages.

---

## 🟤 Section 5: Ecosystem

---

### Q17. What is Kafka Connect?

**Answer:**
**Kafka Connect** is a framework to **stream data between Kafka and external systems** (databases, S3, Elasticsearch, etc.) without writing custom code.

**Two types of Connectors:**

| Type | Direction | Example |
|---|---|---|
| **Source Connector** | External System → Kafka | MySQL → Kafka (CDC) |
| **Sink Connector** | Kafka → External System | Kafka → Elasticsearch |

**Flow:**
```
Source DB ──► Source Connector ──► Kafka Topic ──► Sink Connector ──► Destination
```

**Why use Kafka Connect:**
- No custom producer/consumer code needed
- Handles offset management, schema, retries automatically
- Scales horizontally (distributed mode)
- 100s of ready-made connectors available (Confluent Hub)

**Popular Connectors:**
- Debezium (CDC from MySQL/Postgres/MongoDB)
- JDBC Source/Sink
- S3 Sink
- Elasticsearch Sink
- HDFS Sink

---

### Q18. What is Kafka Streams?

**Answer:**
**Kafka Streams** is a **Java client library** for building real-time stream processing applications that read from and write back to Kafka topics.

**Supported Operations:**
- `filter()` — keep only matching records
- `map()` — transform records
- `join()` — join two streams or a stream with a table
- `aggregate()` / `groupBy()` — stateful aggregations
- `windowed operations` — time-window computations

**Flow:**
```
Input Topic ──► Kafka Streams App (filter/map/join/aggregate) ──► Output Topic
```

**Key Features:**
- **No separate cluster** needed — runs as a library inside your app
- **Fault tolerant** — state stored in RocksDB + backed up to Kafka changelogs
- **Exactly-once processing** supported
- **KTable** — abstraction for changelog/compacted topic (table view of a stream)
- **KStream** — abstraction for unbounded event stream

**vs. Apache Flink/Spark Streaming:**
- Kafka Streams is simpler, embedded, Java-only
- Flink/Spark are separate clusters with more complex operations but more power

---

### Q19. What is Schema Registry?

**Answer:**
**Schema Registry** is a centralized service that stores and manages **Avro / Protobuf / JSON schemas** for Kafka messages, ensuring producers and consumers use compatible schemas.

**Why it's needed:**
Without schema enforcement, a producer change (e.g., rename a field) silently breaks consumers. Schema Registry prevents this.

**Flow:**
```
Producer ──► Schema Registry (register/validate schema)
         ──► Kafka Topic (message with schema ID embedded)
         
Consumer ──► Schema Registry (fetch schema by ID)
         ──► Deserialize message correctly
```

**Compatibility Modes:**

| Mode | Meaning |
|---|---|
| BACKWARD | New schema can read data from old schema |
| FORWARD | Old schema can read data from new schema |
| FULL | Both backward and forward compatible |
| NONE | No compatibility check |

**Real benefit:** You can evolve schemas safely (add optional fields, rename with aliases) without breaking running consumers.

---

### Q20. Why is Kafka Fast?

**Answer:**
Kafka achieves extremely high throughput (millions of messages/sec) through multiple design decisions:

**1. Sequential Disk I/O:**
- Kafka writes messages sequentially to disk (append-only log)
- Sequential disk I/O is ~100x faster than random I/O
- Modern OS page cache makes sequential reads almost as fast as RAM

**2. Zero-Copy Transfer:**
- Uses OS `sendfile()` syscall to transfer data from disk to network without copying into user-space
- Saves CPU cycles and memory bandwidth

**3. Batching:**
- Producers batch multiple messages together
- Reduces network round trips significantly
- `batch.size` and `linger.ms` control this

**4. Compression:**
- Entire batches compressed (snappy/lz4/gzip/zstd)
- Less data over the wire = faster transfer

**5. Partitioning:**
- Parallel writes to multiple partition leaders (different brokers)
- Parallel reads by multiple consumers

**Summary Diagram:**
```
Sequential I/O  ──► Fast disk writes
Zero-Copy       ──► Fast network transfer
Batching        ──► Fewer round trips
Compression     ──► Less data over wire
Partitioning    ──► Horizontal parallelism
                    = Millions of msg/sec
```

---

## 🗺️ End-to-End Kafka Flow Summary

```
┌─────────────┐
│  Producer   │  → Creates message (key, value, headers)
└──────┬──────┘
       │ Serializes (Avro via Schema Registry)
       │ Picks partition (key hash or round-robin)
       │ Batches + optionally compresses
       ▼
┌─────────────────────────────────┐
│         Kafka Cluster           │
│  ┌─────────┐  ┌─────────┐      │
│  │Broker 1 │  │Broker 2 │ ...  │
│  │(Leader) │  │(Replica)│      │
│  └────┬────┘  └─────────┘      │
│       │ ISR replication         │
│       │ acks sent to producer   │
│  Topic/Partition/Offset          │
└──────────────┬──────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌─────────────┐
│ Consumer 1  │  │ Consumer 2  │  ← Same Consumer Group
│ (P0, P1)    │  │ (P2)        │
└──────┬──────┘  └──────┬──────┘
       │                │
       └────────────────┘
       Commits offsets to __consumer_offsets topic
       Deserializes via Schema Registry
       Processes exactly-once (if EOS enabled)
```

---

## 📋 Quick Reference Cheat Sheet

| Concept | One-Line Summary |
|---|---|
| Topic | Named channel for messages |
| Partition | Ordered log within a topic; unit of parallelism |
| Offset | Sequential ID of a message within a partition |
| Producer | Publishes messages to topics |
| Consumer | Reads messages from topics |
| Consumer Group | Group of consumers sharing work (load balance) |
| Broker | Kafka server storing partitions |
| Cluster | Multiple brokers for scale & fault tolerance |
| Leader | Broker handling reads/writes for a partition |
| Follower/ISR | Replicas in-sync with leader |
| ZooKeeper | Legacy external metadata manager |
| KRaft | Modern built-in metadata via Raft protocol |
| acks | Producer durability config (0/1/all) |
| Retention | How long Kafka keeps messages (time/size) |
| Log Compaction | Keep only latest value per key |
| Offset Commit | Consumer records its read progress |
| EOS | Exactly-once processing guarantee |
| Kafka Connect | Framework for external system integration |
| Kafka Streams | Java library for stream processing |
| Schema Registry | Central schema store for Avro/Protobuf/JSON |
| Why Fast | Seq I/O + Zero-copy + Batching + Compression + Partitioning |

---
