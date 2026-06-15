# Kafka: ZooKeeper vs KRaft

> Kafka 4.0+ removes ZooKeeper entirely. KRaft (Kafka Raft Metadata) is now the only mode.  
---

## Part 1: Why Did Kafka Use ZooKeeper in the First Place?

Before understanding KRaft, you need to understand what ZooKeeper was doing inside Kafka — because it was doing A LOT.

### ZooKeeper's Role in Old Kafka

ZooKeeper was a **separate distributed coordination service** that Kafka depended on to manage:

| Responsibility | What ZooKeeper Did |
|---|---|
| Broker Registration | Each broker registered itself in ZooKeeper on startup |
| Controller Election | ZooKeeper elected one broker as the "Controller" |
| Topic Metadata | Stored which topics/partitions exist and on which broker |
| Partition Leader Election | Tracked who the leader is for each partition |
| Consumer Group Offsets | Old consumers stored offsets in ZooKeeper (pre-0.9) |
| Access Control Lists (ACLs) | Stored security rules in ZooKeeper znodes |
| Broker Config | Dynamic broker configs stored in ZooKeeper |

### The Architecture Looked Like This

```
┌─────────────────────────────────────────────┐
│              ZooKeeper Ensemble              │
│   (3 or 5 ZK nodes for quorum)              │
│                                             │
│   /brokers/ids/1                            │
│   /brokers/ids/2                            │
│   /brokers/topics/my-topic/...              │
│   /controller  (which broker is leader)     │
└────────────────────┬────────────────────────┘
                     │ All brokers watch & write
         ┌───────────┼───────────┐
         ▼           ▼           ▼
    [Broker 1]  [Broker 2]  [Broker 3]
    (Controller) (Follower) (Follower)
```

---

## Part 2: The Problems With ZooKeeper Mode

### Problem 1: Two Systems to Operate

Running Kafka meant running TWO distributed systems:
- A ZooKeeper cluster (typically 3 or 5 nodes)
- A Kafka cluster (typically 3+ brokers)

This doubled your operational burden:
- Two sets of configs to manage
- Two sets of logs to monitor
- Two systems to upgrade, patch, and secure
- Two systems that could fail independently

### Problem 2: Metadata Bottleneck — The 200,000 Partition Wall

Every time a partition leader changed, a topic was created, or a broker joined/left, the **Controller Broker** had to:
1. Read from ZooKeeper
2. Process the change
3. Push updates to ALL brokers

With large clusters (hundreds of brokers, millions of partitions), this became a serious bottleneck.

**Real-world limit:** Clusters with more than ~200,000 partitions experienced severe slowdowns during controller failover because the new controller had to reload ALL metadata from ZooKeeper from scratch — this could take **30+ seconds** with large clusters.

### Problem 3: Controller Failover Was Slow

When the Controller Broker failed:
1. ZooKeeper detected the failure (after a timeout)
2. A new broker was elected as Controller
3. The new Controller had to **re-read all metadata from ZooKeeper** (all topics, partitions, ISRs, broker states)
4. Only then could it start managing the cluster

This "cold start" of controller state caused availability gaps proportional to cluster size.

### Problem 4: ZooKeeper's Own Consistency Challenges

ZooKeeper uses **ZAB (ZooKeeper Atomic Broadcast)** for consensus. But Kafka also had its own replication protocol. Having two separate consensus systems made reasoning about consistency extremely complex.

### Problem 5: Security Surface Area

With ZooKeeper, you had to secure:
- Kafka ↔ ZooKeeper communication (SASL, TLS)
- ZooKeeper ↔ ZooKeeper communication
- Client ↔ Kafka communication

ACLs stored in ZooKeeper meant the ZooKeeper cluster itself became a security-critical component.

---

## Part 3: What is KRaft?

**KRaft = Kafka Raft Metadata**

KRaft eliminates ZooKeeper by building the metadata management **directly into Kafka itself**, using the **Raft consensus algorithm**.

### The Core Idea

Instead of outsourcing coordination to ZooKeeper, Kafka now manages its own metadata using a special **internal topic** called `@metadata` and a dedicated group of brokers called **KRaft Controllers**.

```
┌──────────────────────────────────────────────────────┐
│                  Kafka Cluster (KRaft)                │
│                                                      │
│   ┌─────────────┐   ┌─────────────┐   ┌──────────┐  │
│   │ Controller 1 │   │ Controller 2│   │Controller│  │
│   │  (Active)   │   │ (Follower)  │   │(Follower)│  │
│   └──────┬──────┘   └─────────────┘   └──────────┘  │
│          │  Raft consensus on @metadata topic         │
│   ┌──────┴──────┐   ┌─────────────┐   ┌──────────┐  │
│   │  Broker 1   │   │  Broker 2   │   │ Broker 3 │  │
│   └─────────────┘   └─────────────┘   └──────────┘  │
└──────────────────────────────────────────────────────┘
```

**No ZooKeeper anywhere.**

---

## Part 4: How KRaft Works Internally

### The Raft Algorithm (Simplified)

Raft is a consensus algorithm that allows a group of servers to agree on a sequence of values even if some servers fail. In KRaft:

1. **One Active Controller** (the Raft leader) handles all metadata writes
2. **Follower Controllers** replicate all metadata changes
3. Changes require a **majority quorum** (e.g., 2 out of 3 controllers must confirm)
4. If the Active Controller fails, Raft elects a new leader from followers **in milliseconds**

### The `@metadata` Topic

KRaft stores all cluster metadata in a special internal Kafka topic called `@metadata`:
- This is a **single-partition, replicated topic**
- Every broker subscribes to this topic and maintains an in-memory copy of the metadata
- When a broker starts up, it replays the `@metadata` log to rebuild its state

```
@metadata topic events:
  [0] BrokerRegistration(id=1, host=kafka1, port=9092)
  [1] BrokerRegistration(id=2, host=kafka2, port=9092)
  [2] TopicCreate(name="orders", partitions=3)
  [3] PartitionLeaderChange(topic="orders", partition=0, leader=1)
  [4] BrokerDeregistration(id=3)
  ...
```

### Metadata Fetch by Brokers

In KRaft, every broker:
- Fetches updates from the Active Controller
- Maintains its own **in-memory metadata cache**
- Can answer metadata requests from clients **locally** without going to a central store

This eliminates the "all metadata through ZooKeeper" bottleneck.

---

## Part 5: ZooKeeper Mode vs KRaft Mode — Side-by-Side

### Architecture Comparison

| Aspect | ZooKeeper Mode | KRaft Mode |
|---|---|---|
| External dependency | ZooKeeper cluster required | None — self-contained |
| Metadata storage | ZooKeeper znodes | `@metadata` Kafka topic |
| Controller election | ZooKeeper ephemeral node race | Raft leader election |
| Metadata protocol | ZAB (ZooKeeper Atomic Broadcast) | Raft |
| Controller state on failover | Reload from ZooKeeper (slow) | Already replicated (fast) |
| Min nodes needed | 3 ZK + 3 Kafka = 6 nodes | 3 Kafka nodes (controllers + brokers) |

### Performance Comparison

| Metric | ZooKeeper Mode | KRraft Mode |
|---|---|---|
| Max practical partitions | ~200,000 | Millions (tested to 3M+) |
| Controller failover time | 30–120 seconds (large clusters) | Under 1 second |
| Metadata propagation | Through ZooKeeper → Controller → Brokers | Direct: Controller → Brokers |
| Broker startup time | Must sync with ZooKeeper | Replay local `@metadata` log |

### Operational Comparison

| Task | ZooKeeper Mode | KRaft Mode |
|---|---|---|
| Setup | Install + configure ZK cluster + Kafka | Just Kafka |
| Config files | `server.properties` + `zookeeper.properties` | `server.properties` only |
| Monitoring | ZK metrics + Kafka metrics separately | Kafka metrics only |
| Security | Secure ZK + Kafka separately | Secure Kafka only |
| Upgrades | Upgrade ZK first, then Kafka | Upgrade Kafka only |
| ACL storage | ZooKeeper znodes | Kafka `@metadata` log |

### Startup Process Comparison

**ZooKeeper Mode:**
```
Broker starts
  → Connects to ZooKeeper
  → Registers /brokers/ids/<id> in ZK
  → Watches /controller in ZK
  → Reads topic/partition metadata from ZK
  → Ready to serve clients
```

**KRaft Mode:**
```
Broker starts
  → Reads local @metadata log (fast, local disk)
  → Connects to KRaft Controller
  → Fetches any new @metadata events it missed
  → Ready to serve clients
```

---

## Part 6: KRaft Node Roles

In KRaft, each Kafka node has a **role** configured in `server.properties`:

### Role: `controller`
- Participates in Raft consensus
- Manages cluster metadata
- Does NOT serve producer/consumer traffic
- Dedicated controller nodes = best for large production clusters

### Role: `broker`
- Serves producer/consumer traffic
- Stores partition data
- Subscribes to `@metadata` from controllers
- Cannot participate in controller elections

### Role: `controller,broker` (Combined Mode)
- Acts as both controller and broker
- Simpler for small clusters / development
- Not recommended for large production clusters (controller work can interfere with broker work)

### Configuration in `server.properties`

```properties
# KRaft mode — no ZooKeeper
process.roles=broker,controller         # or just broker, or just controller

# Unique node ID
node.id=1

# The controller quorum voters
# Format: nodeId@host:controllerPort
controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093

# Listeners
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# The controller listener name
controller.listener.names=CONTROLLER
```

### Initialize KRaft Storage (One-Time Setup)
```bash
# Generate a cluster UUID
KAFKA_CLUSTER_ID=$(kafka-storage random-uuid)

# Format storage directories on each node
kafka-storage format \
  --config /opt/kafka/config/server.properties \
  --cluster-id $KAFKA_CLUSTER_ID
```

---

## Part 7: Migrating From ZooKeeper to KRaft

### Migration Timeline
- **Kafka 2.8** — KRaft introduced as early access (not production ready)
- **Kafka 3.3** — KRaft declared production ready
- **Kafka 3.4–3.7** — Migration tooling added (ZK → KRaft live migration)
- **Kafka 4.0** — ZooKeeper support **completely removed**

### Migration Approaches

#### Option A: Fresh Cluster (Recommended for new setups)
Just install Kafka 4.x and configure KRaft from day one. No migration needed.

#### Option B: Live Migration (Kafka 3.x with dual-write)

Kafka 3.x supports a **live migration** where you gradually move metadata from ZooKeeper to KRaft without downtime:

```
Phase 1: ZooKeeper-only mode (current state)
  ZooKeeper stores all metadata
  Kafka brokers read from ZooKeeper

Phase 2: Dual-write migration mode
  A KRaft controller is added to the cluster
  Metadata written to BOTH ZooKeeper and KRaft controller
  Brokers still read from ZooKeeper
  ZooKeeper = source of truth

Phase 3: KRaft migration mode
  KRaft controller becomes source of truth
  Brokers start reading from KRaft
  ZooKeeper still running but no longer authoritative

Phase 4: ZooKeeper-only shutdown
  ZooKeeper is decommissioned
  KRaft is the only metadata store
```

### Migration Steps (Kafka 3.6+)

```bash
# Step 1: Add KRaft controllers to cluster
# Add new brokers with process.roles=controller to server.properties

# Step 2: Start the migration
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  --command-config admin.properties \
  start-migration

# Step 3: Check migration status
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status

# Step 4: Complete the migration (finalize KRaft as source of truth)
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  finalize-migration

# Step 5: Shut down ZooKeeper
brew services stop zookeeper  # or systemctl stop zookeeper
```

### Verify KRaft is Running
```bash
# Check quorum status
kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  describe --status

# Output in KRaft mode:
# ClusterId:              Ks_XXXXXXXXXXXX
# LeaderId:               1
# LeaderEpoch:            5
# HighWatermark:          1042
# MaxFollowerLag:         0
# MaxFollowerLagTimeMs:   -1
# CurrentVoters:          [1,2,3]
# CurrentObservers:       [4,5,6]
```

---

## Part 8: Removed ZooKeeper Features & What Replaced Them

### Features Fully Removed in Kafka 4.0

#### 1. `zookeeper.connect` Config Property
```properties
# ZooKeeper Mode (REMOVED in Kafka 4.0)
zookeeper.connect=zk1:2181,zk2:2181,zk3:2181

# KRaft Replacement
controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
```

#### 2. ZooKeeper Shell for Kafka Admin Tasks
```bash
# OLD — ZooKeeper shell (REMOVED)
zookeeper-shell.sh localhost:2181
ls /brokers/ids
get /controller
ls /brokers/topics

# NEW — Use Kafka Admin tools instead
kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
kafka-broker-api-versions --bootstrap-server localhost:9092
kafka-topics --bootstrap-server localhost:9092 --list
```

#### 3. ZooKeeper-based Consumer Offsets (Already removed in Kafka 0.10)
Old Kafka (pre-0.10) stored consumer offsets in ZooKeeper at `/consumers/<group>/offsets`. This was migrated to the `__consumer_offsets` topic years ago. KRaft continues using `__consumer_offsets`.

#### 4. ZooKeeper-based ACL Storage
```bash
# OLD — ACLs stored in ZooKeeper at /kafka-acl/
kafka-acls.sh --zookeeper localhost:2181 --list  # REMOVED

# NEW — ACLs stored in @metadata log, managed via broker
kafka-acls --bootstrap-server localhost:9092 --list
```

#### 5. ZooKeeper-based Dynamic Broker Configs
```bash
# OLD — Dynamic configs stored in ZooKeeper
kafka-configs.sh --zookeeper localhost:2181 \
  --entity-type brokers --entity-name 1 --describe  # REMOVED

# NEW — Configs managed through broker API
kafka-configs --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 1 --describe
```

#### 6. `kafka-preferred-replica-election.sh` with ZooKeeper flag
```bash
# OLD (REMOVED)
kafka-preferred-replica-election.sh --zookeeper localhost:2181

# NEW
kafka-leader-election --bootstrap-server localhost:9092 \
  --election-type PREFERRED --all-topic-partitions
```

#### 7. `kafka-reassign-partitions.sh` ZooKeeper mode
```bash
# OLD (REMOVED)
kafka-reassign-partitions.sh --zookeeper localhost:2181 \
  --reassignment-json-file reassign.json --execute

# NEW
kafka-reassign-partitions --bootstrap-server localhost:9092 \
  --reassignment-json-file reassign.json --execute
```

#### 8. `kafka-topics.sh --zookeeper` flag (REMOVED)
```bash
# OLD (REMOVED)
kafka-topics.sh --zookeeper localhost:2181 --list

# NEW
kafka-topics --bootstrap-server localhost:9092 --list
```

### Complete Removed CLI Flags

| Tool | Removed Flag | Replacement |
|---|---|---|
| `kafka-topics` | `--zookeeper` | `--bootstrap-server` |
| `kafka-configs` | `--zookeeper` | `--bootstrap-server` |
| `kafka-acls` | `--zookeeper` | `--bootstrap-server` |
| `kafka-consumer-groups` | `--zookeeper` | `--bootstrap-server` |
| `kafka-reassign-partitions` | `--zookeeper` | `--bootstrap-server` |
| `kafka-preferred-replica-election` | `--zookeeper` | `--bootstrap-server` |
| All tools | `zookeeper.connect` config | `controller.quorum.voters` config |

---

## Part 9: KRaft Advantages — Why It's Better

### 1. Simpler Operations
- One system to deploy, monitor, and upgrade
- Fewer failure points
- Fewer config files

### 2. Faster Controller Failover
```
ZooKeeper Mode:  30–120 seconds to elect new controller + reload state
KRaft Mode:      < 1 second — state already replicated via Raft
```

### 3. Massive Scale Improvement
```
ZooKeeper Mode:  ~200,000 partitions practical limit
KRaft Mode:      Tested to 3,000,000+ partitions
```

### 4. Consistent Metadata
- In ZooKeeper mode, metadata was split between ZooKeeper and brokers' in-memory caches — could get out of sync
- In KRaft, `@metadata` is the single source of truth, and every broker subscribes to it directly

### 5. Stronger Consistency Guarantees
- ZAB (ZooKeeper) and Kafka's ISR replication were two independent consistency protocols
- KRaft unifies everything under Raft — one protocol, one consistency model

---

## Part 10: Things to Watch Out For in KRaft

### Quorum Size Must Be Odd
KRaft controllers need a majority to elect a leader:
- 1 controller → can tolerate 0 failures
- 3 controllers → can tolerate 1 failure
- 5 controllers → can tolerate 2 failures
- Always use odd numbers: 3, 5, or 7

### Controller Nodes Should Be Dedicated in Production
In large clusters, don't use `process.roles=broker,controller` — the Raft consensus work can interfere with broker latency. Use dedicated controller nodes.

### `@metadata` Log Grows Over Time
The `@metadata` log is compacted but can grow large in clusters with many partition changes. Monitor `log.dirs` disk usage on controller nodes.

### No More ZooKeeper Shell Debugging
In ZooKeeper mode, you could browse cluster state with `zookeeper-shell.sh`. In KRaft, use:
```bash
# Dump the @metadata log (for debugging)
kafka-dump-log \
  --files /var/kafka/data/__cluster_metadata-0/00000000000000000000.log \
  --cluster-metadata-decoder
```

---

## Quick Reference: ZooKeeper → KRaft Command Mapping

| Action | ZooKeeper Era Command | KRaft Command |
|---|---|---|
| List topics | `kafka-topics --zookeeper zk:2181 --list` | `kafka-topics --bootstrap-server kafka:9092 --list` |
| List ACLs | `kafka-acls --zookeeper zk:2181 --list` | `kafka-acls --bootstrap-server kafka:9092 --list` |
| Broker configs | `kafka-configs --zookeeper zk:2181 ...` | `kafka-configs --bootstrap-server kafka:9092 ...` |
| Controller info | `echo stat \| nc zk 2181` or ZK shell | `kafka-metadata-quorum --bootstrap-server kafka:9092 describe --status` |
| Cluster ID | ZooKeeper `/cluster/id` znode | `kafka-cluster --bootstrap-server kafka:9092 cluster-id` |
| Reassign partitions | `--zookeeper` flag | `--bootstrap-server` flag |
| Format storage | N/A | `kafka-storage format --config server.properties --cluster-id <UUID>` |

---
