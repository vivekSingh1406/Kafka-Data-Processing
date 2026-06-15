# Kafka Deep Dive — Architecture, Components

---

## Part 1: How Kafka Works

### The Core Idea

Kafka is a **distributed event streaming platform**. Think of it like a **post office for data**:
- Applications (Producers) **send letters (events/messages)**
- Letters go into **mailboxes sorted by category (Topics)**
- Each mailbox is divided into **numbered slots (Partitions)**
- A **mail server (Broker)** stores everything safely
- Recipients (Consumers) **pick up their letters** and mark which ones they've read (Offset)

---

## Part 2: Every Component Explained

---

### 🟠 1. Producer

**What it is:** Any application that sends (publishes) messages to Kafka.

**How it works:**
- Producer connects to a Kafka Broker
- It sends a message to a specific **Topic**
- It can optionally specify a **Partition Key** to control which partition the message goes to
- If no key is given → Kafka uses **Round Robin** across partitions

**Key Configs:**
| Config | Purpose |
|---|---|
| `acks=0` | Fire and forget — no confirmation |
| `acks=1` | Leader confirms receipt |
| `acks=all` | All ISR replicas confirm — safest |
| `retries=3` | Retry on failure |
| `enable.idempotence=true` | Prevent duplicate messages |

**Partition Key Strategy:**
- Same key → always same partition → **ordering guaranteed per key**
- Example: `orderId` as key → all events for that order land in the same partition in sequence

---

### 🟡 2. Topic

**What it is:** A named category or channel for messages. Like a database table, but for streams.

**Key facts:**
- Topics are **append-only logs** — messages are never deleted immediately
- Retention is time-based (default 7 days) or size-based
- Topics are split into **Partitions** for scalability
- A topic can have **N partitions** across **M brokers**

**Example:**
```
Topic: "order-events"
  ├── Partition 0  →  [msg0, msg1, msg5, ...]
  ├── Partition 1  →  [msg2, msg3, msg6, ...]
  └── Partition 2  →  [msg4, msg7, msg8, ...]
```

---

### 🔵 3. Partition

**What it is:** The unit of parallelism in Kafka. Each topic is divided into partitions.

**Why it matters:**
- Each partition is an **ordered, immutable sequence** of messages
- Ordering is **only guaranteed within a partition**, NOT across partitions
- More partitions = more throughput = more consumer parallelism
- Each partition is stored on exactly **one broker** (the leader)

**Partition Key Rules:**
```
Key = null         → Round Robin across partitions
Key = "user123"    → hash("user123") % numPartitions = always same partition
```

**Trade-off:** More partitions = more parallelism but also more overhead (more file handles, more replication traffic).

---

### 🟢 4. Broker

**What it is:** A Kafka server. It receives, stores, and serves messages.

**Key facts:**
- A Kafka cluster has **multiple brokers** (e.g., 3 brokers = 3 servers)
- Each broker holds some partitions as **Leader** and others as **Follower (replica)**
- The **Controller Broker** manages partition leader elections
- In KRaft mode (Kafka 4.x), there is **no Zookeeper** — brokers manage metadata themselves

**Broker responsibilities:**
- Accept writes from Producers
- Replicate data to follower brokers
- Serve reads to Consumers
- Handle leader election when a broker fails

---

### 🔴 5. Consumer

**What it is:** Any application that reads (subscribes to) messages from a Kafka topic.

**How it works:**
- Consumer **polls** for messages (Kafka is pull-based, not push-based)
- It reads from specific partitions
- It tracks its position using **Offsets**
- Consumers belong to a **Consumer Group**

**Pull vs Push:**
- Kafka uses **pull** → consumers control the pace
- This prevents overwhelming slow consumers

---

### 🟣 6. Consumer Group

**What it is:** A group of consumers working together to consume a topic in parallel.

**Rules:**
- Each partition is assigned to **exactly ONE consumer** in a group
- If consumers > partitions → extra consumers sit **idle**
- If consumers < partitions → one consumer handles multiple partitions
- Different consumer groups get **independent copies** of all messages

```
Topic: order-events (3 partitions)
Consumer Group A (3 consumers):
  Consumer A1 → Partition 0
  Consumer A2 → Partition 1
  Consumer A3 → Partition 2

Consumer Group B (1 consumer):
  Consumer B1 → Partition 0, 1, 2 (all partitions)
```

**Rebalancing:** When a consumer joins or leaves the group, Kafka redistributes partitions — this is called a **rebalance**. During rebalance, consumption is paused briefly.

---

### ⚪ 7. Offset

**What it is:** A unique, sequential ID for each message within a partition. Like a page number in a book.

```
Partition 0:  [0] [1] [2] [3] [4] [5] ...
                               ↑
                        Consumer is here (offset 3)
```

**Offset Commit Strategies:**
| Strategy | Behavior | Risk |
|---|---|---|
| Auto Commit | Commits periodically automatically | Can lose messages if consumer crashes |
| Manual Commit (sync) | You call `commitSync()` after processing | Safer, slight performance hit |
| Manual Commit (async) | You call `commitAsync()` | Faster, but no retry on failure |

**Where offsets are stored:** In a special internal Kafka topic: `__consumer_offsets`

---

### 🔷 8. Replication — Leader & Follower

**What it is:** Kafka copies each partition to multiple brokers for fault tolerance.

```
Partition 0:
  Broker 1 → LEADER   (handles reads/writes)
  Broker 2 → FOLLOWER (replica)
  Broker 3 → FOLLOWER (replica)
```

**Replication Factor:** How many copies exist. `replication-factor=3` → 1 leader + 2 followers.

**ISR (In-Sync Replicas):**
- The set of replicas that are **caught up** with the leader
- Only ISR replicas can become the new leader if the current leader fails
- If a replica falls behind → it's removed from ISR
- `min.insync.replicas=2` → at least 2 replicas must confirm a write (with `acks=all`)

**What happens when a broker fails:**
1. Controller detects the broker is down
2. One of the ISR followers is elected as the new leader
3. Producers/consumers automatically reconnect to the new leader

---

### 🔶 9. Serializer & Deserializer

**What it is:** Kafka messages are bytes. Serializer converts object → bytes for sending. Deserializer converts bytes → object for reading.

```
Producer Side:  Employee Object → Serializer → bytes → Kafka
Consumer Side:  bytes → Deserializer → Employee Object
```

**Common options:**
- `StringSerializer` / `StringDeserializer` → plain text
- `JsonSerializer` / `JsonDeserializer` → JSON (Spring Kafka)
- `AvroSerializer` → Avro with Schema Registry (Confluent)

---

### 🟤 10. Dead Letter Topic (DLT)

**What it is:** When a consumer fails to process a message after all retries, the message is sent to a DLT for investigation.

```
Normal Flow:    Kafka Topic → Consumer → Process ✅
Failure Flow:   Kafka Topic → Consumer → Retry 1 → Retry 2 → Retry 3 → DLT ❌
```

**Spring Kafka DLT:**
```java
@RetryableTopic(attempts = "3", dltTopicSuffix = ".DLT")
@KafkaListener(topics = "orders")
public void listen(String message) { ... }
```

---

### 🔸 11. Idempotent Producer

**What it is:** Guarantees that even if a message is sent multiple times (due to retries), it is written to Kafka **exactly once**.

```
Without idempotence: retry → duplicate message in Kafka
With idempotence:    retry → Kafka deduplicates → exactly one message
```

Enable with: `enable.idempotence=true`

Each message gets a **Producer ID + Sequence Number**. Broker rejects duplicates.

---

### 🔹 12. Exactly-Once Semantics (EOS)

**What it is:** The gold standard. Each message is processed **exactly once** end-to-end — no duplicates, no data loss.

**Three delivery guarantees:**
| Guarantee | Behavior |
|---|---|
| At most once | Message may be lost, never duplicated |
| At least once | No loss, but duplicates possible |
| Exactly once | No loss, no duplicates — hardest to achieve |

EOS requires:
- Idempotent producer
- Transactional API (`producer.beginTransaction()`, `producer.commitTransaction()`)
- Consumer with `isolation.level=read_committed`

---

### 🔧 13. Spring Kafka — KafkaTemplate & @KafkaListener

**KafkaTemplate (Producer):**
```java
@Autowired
private KafkaTemplate<String, String> kafkaTemplate;

public void sendMessage(String message) {
    kafkaTemplate.send("my-topic", message);
}
```

**@KafkaListener (Consumer):**
```java
@KafkaListener(topics = "my-topic", groupId = "my-group")
public void listen(String message) {
    System.out.println("Received: " + message);
}
```

**Listen with full metadata:**
```java
@KafkaListener(topics = "my-topic", groupId = "my-group")
public void listen(
    @Payload String message,
    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
    @Header(KafkaHeaders.OFFSET) long offset,
    @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp
) {
    System.out.println("Partition: " + partition + ", Offset: " + offset);
}
```

---

## Part 3: Kafka vs RabbitMQ

| Feature | Kafka | RabbitMQ |
|---|---|---|
| Model | Log-based (event log) | Queue-based (message queue) |
| Message Retention | Stored for retention period (days) | Deleted after consumed |
| Ordering | Per partition | Per queue |
| Throughput | Millions/sec | Thousands/sec |
| Replay | ✅ Yes (reset offset) | ❌ No |
| Best for | Event streaming, analytics, audit log | Task queues, RPC, complex routing |
| Consumer model | Pull | Push |
| Scaling | Add partitions | Add queues/consumers |

---

## Part 4: Interview Questions — Basic to Tricky

---

### 🟢 Basic Questions

**Q1. What is Kafka and why is it used?**
> Kafka is a distributed event streaming platform used to build real-time data pipelines. It's used for high-throughput, fault-tolerant, ordered message processing across microservices.

**Q2. What is a Topic in Kafka?**
> A topic is a named, append-only log where producers publish messages and consumers subscribe to read them. Topics are split into partitions for scalability.

**Q3. What is the role of a Broker?**
> A broker is a Kafka server that stores messages in partitions, handles producer writes, serves consumer reads, and participates in replication.

**Q4. What is an Offset?**
> An offset is a unique, sequential integer that identifies each message within a partition. Consumers use offsets to track which messages they've already processed.

**Q5. What is a Consumer Group?**
> A consumer group is a set of consumers that share the work of reading a topic. Each partition is assigned to exactly one consumer in the group, enabling parallel processing.

---

### 🟡 Intermediate Questions

**Q6. What happens when you have more consumers than partitions in a group?**
> Extra consumers will sit **idle**. Kafka assigns each partition to at most one consumer per group. So with 3 partitions and 5 consumers, 2 consumers receive no messages.

**Q7. How does Kafka guarantee ordering?**
> Kafka guarantees ordering **only within a single partition**. Messages with the same key always go to the same partition (via key hashing), so ordering is guaranteed per key. Across partitions, there is no ordering guarantee.

**Q8. What is ISR and why does it matter?**
> ISR (In-Sync Replicas) is the set of partition replicas that are fully caught up with the leader. Only ISR members can be elected as leader on failover. If `min.insync.replicas=2` and `acks=all`, the producer waits for at least 2 replicas to confirm before the write is acknowledged — ensuring durability.

**Q9. What is the difference between `acks=1` and `acks=all`?**
> `acks=1` means only the leader broker confirms receipt — if the leader dies before replication, data is lost. `acks=all` means all ISR replicas must confirm — much safer but slightly slower.

**Q10. What is Consumer Lag?**
> Consumer lag is the difference between the latest offset in a partition (Log End Offset) and the consumer's current offset. High lag means the consumer is behind and not keeping up with producers.

---

### 🔴 Advanced / Tricky Questions

**Q11. ⚠️ Can Kafka guarantee exactly-once delivery? How?**
> Yes, but it requires three things working together:
> 1. **Idempotent Producer** (`enable.idempotence=true`) — prevents duplicate writes
> 2. **Transactions** — producer wraps sends in `beginTransaction()`/`commitTransaction()`
> 3. **Consumer `isolation.level=read_committed`** — consumer only reads committed messages
> Without all three, you get at-least-once at best.

**Q12. ⚠️ What is a Rebalance and when does it cause problems?**
> A rebalance happens when consumers join or leave a group, causing Kafka to redistribute partitions. During rebalance, ALL consumers stop processing (stop-the-world). Problems arise when:
> - Rebalances happen frequently (consumer crashes, slow processing exceeding `max.poll.interval.ms`)
> - Processing is stateful and partition reassignment loses in-memory state
> Solution: Use `CooperativeStickyAssignor` for incremental rebalances that avoid full stop-the-world pauses.

**Q13. ⚠️ Why can a consumer read duplicate messages even with auto-commit enabled?**
> Auto-commit runs on a timer (default 5 seconds). If a consumer reads messages, processes them, but crashes before the next auto-commit, the offset is never committed. When the consumer restarts, it re-reads from the last committed offset → duplicates. Solution: Use manual `commitSync()` after processing.

**Q14. ⚠️ What is the difference between Kafka's `__consumer_offsets` topic and a database?**
> `__consumer_offsets` is an internal Kafka topic that stores consumer group offsets. Unlike a database, it's append-only, replicated across brokers, and compacted (keeps only the latest offset per group+partition). It's what allows Kafka to resume consumption after restarts without an external database.

**Q15. ⚠️ If a broker goes down, what exactly happens step by step?**
> 1. The Controller Broker detects the failure (via heartbeat timeout)
> 2. For each partition where the failed broker was the **leader**, the controller elects a new leader from the ISR
> 3. The cluster metadata is updated with new leaders
> 4. Producers and consumers get `NotLeaderForPartitionException` and fetch new metadata
> 5. They reconnect to the new leader and resume
> If the failed broker was only a follower → no interruption to producers/consumers.

**Q16. ⚠️ What happens if `min.insync.replicas` is 2 but only 1 replica is in ISR?**
> The producer (with `acks=all`) will receive a `NotEnoughReplicasException`. Kafka refuses to accept the write because it cannot guarantee the durability contract. This is a safety feature — it prevents data loss at the cost of availability.

**Q17. ⚠️ How does Kafka handle back-pressure?**
> Kafka doesn't push messages to consumers — consumers **pull** at their own pace. This is built-in back-pressure. If a consumer is slow, it simply polls less frequently. The messages remain in Kafka until the retention period expires. Contrast with RabbitMQ, which pushes messages and can overwhelm slow consumers.

**Q18. ⚠️ What is Log Compaction and when would you use it?**
> Log compaction is a Kafka cleanup policy where Kafka keeps only the **latest value for each key**. Instead of deleting old messages after a time period, it deletes old values for keys that have been updated.
> Use case: **Change Data Capture (CDC)** or **user profile snapshots** where you only need the latest state, not the full history.
> Enable with: `cleanup.policy=compact`

**Q19. ⚠️ You have 6 partitions and 3 consumers. One consumer dies. What happens?**
> 1. Kafka detects the consumer left the group (via heartbeat timeout or explicit leave)
> 2. A **rebalance** is triggered
> 3. The 2 partitions that belonged to the dead consumer are redistributed to the 2 remaining consumers
> 4. Now each surviving consumer handles 3 partitions
> 5. Lag accumulates during the rebalance window

**Q20. ⚠️ What is the difference between a KafkaTemplate send and a transactional send in Spring?**
> Regular `kafkaTemplate.send()` sends immediately with at-least-once guarantee. Transactional send wraps multiple sends in a transaction:
> ```java
> kafkaTemplate.executeInTransaction(kt -> {
>     kt.send("topic1", "msg1");
>     kt.send("topic2", "msg2");
>     return true;
> });
> ```
> Both sends succeed or both fail atomically. Required for exactly-once semantics.

---

## Part 5: Key Takeaways Summary

| Concept | One-Line Summary |
|---|---|
| Producer | Sends events to Kafka topics |
| Topic | Named, append-only log — like a table for streams |
| Partition | Unit of parallelism — ordering guaranteed within a partition |
| Broker | Kafka server — stores partitions and serves clients |
| Consumer | Pulls messages from partitions at its own pace |
| Consumer Group | Team of consumers — each partition assigned to exactly one |
| Offset | Sequential message ID per partition — tracks what's been read |
| ISR | Replicas caught up with leader — backbone of fault tolerance |
| Rebalance | Partition redistribution when consumers join/leave |
| DLT | Graveyard for messages that repeatedly fail processing |
| Idempotent Producer | Prevents duplicate writes on retry |
| EOS | Exactly-once end-to-end — idempotent + transactions + read_committed |
| Log Compaction | Keep only latest value per key — great for state snapshots |

---
