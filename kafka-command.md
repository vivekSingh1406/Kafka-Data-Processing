# Kafka Commands

> Kafka 4.3.0 installed via Homebrew on macOS  
> Broker: `localhost:9092` | Mode: KRaft (no Zookeeper needed)  

---

## 1. Kafka Service Management

### Check if Kafka is Running
```bash
brew services list
```
**Output:**
```
Name   Status  User   File
kafka  started vivek  ~/Library/LaunchAgents/homebrew.mxcl.kafka.plist
```

### Start Kafka
```bash
brew services start kafka
```

### Stop Kafka
```bash
brew services stop kafka
```

### Restart Kafka
```bash
brew services restart kafka
```

### Check Kafka Logs
```bash
brew services info kafka
tail -f ~/Library/Logs/Homebrew/kafka.log
```

---

## 2. Topic Commands

### List All Topics
```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```
**Output:**
```
kafka-spring-producer
__consumer_offsets
```

### Create a Topic (default 1 partition)
```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic kafka-spring-producer
```
**Output:**
```
Created topic kafka-spring-producer.
```

### Create a Topic with Partitions
```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic kafka-spring-producer \
  --partitions 3 \
  --replication-factor 1
```

### Describe a Topic (partitions, leader, replicas, ISR)
```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic kafka-spring-producer
```
**Output:**
```
Topic: kafka-spring-producer   TopicId: GSIE881NTCS3nZsKhR9EyQ   PartitionCount: 1   ReplicationFactor: 1
  Partition: 0   Leader: 1   Replicas: 1   Isr: 1
```

### Describe All Topics
```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe
```

### Delete a Topic
```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic kafka-spring-producer
```

---

## 3. Producing Messages

### Produce Simple Text Messages
```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer
```
Type messages, press **Enter** after each, **Ctrl+C** to exit:
```
Hello Kafka
Message One
Message Two
```

### Produce with Key
```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --property "parse.key=true" \
  --property "key.separator=:"
```
Type in `key:value` format:
```
user1:Hello Kafka
user2:Message Two
```

### Produce JSON Messages
```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer
```
Then type:
```json
{"name":"Vivek","department":"Technology","salary":4000000}
{"name":"Raj","department":"Engineering","salary":3500000}
```

---

## 4. Consuming Messages

### Consume Only New (live) Messages
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer
```

### Read ALL Messages from Beginning
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning
```

### Consume with Partition + Offset + Timestamp Metadata
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true
```
**Output:**
```
Partition:0
Offset:0
CreateTime:1750000000000
Hello Kafka

Partition:0
Offset:1
CreateTime:1750000001000
Message One
```

### Consume with Key Displayed
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning \
  --property print.key=true \
  --property key.separator=":"
```
**Output:**
```
null:Hello Kafka
null:Message One
```

### Consume with ALL Metadata (key + partition + offset + timestamp)
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true
```

### Consume a Fixed Number of Messages
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning \
  --max-messages 5
```

### Consume with a Consumer Group (tracks offset)
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning \
  --group my-consumer-group
```

### Consume from a Specific Partition
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --partition 0 \
  --offset earliest
```

---

## 5. Offset Commands

### Show Latest Offset per Partition
```bash
kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer
```
**Output:**
```
kafka-spring-producer:0:25
```
Meaning → `Topic : Partition : Latest Offset`

### Show Earliest Offset per Partition
```bash
kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --time earliest
```

### Show Offsets for ALL Topics
```bash
kafka-get-offsets \
  --bootstrap-server localhost:9092
```

---

## 6. Consumer Group Commands

### List All Consumer Groups
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```
**Output:**
```
group_json
group_id
my-consumer-group
```

### Describe a Consumer Group (lag, current offset)
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group group_json
```
**Output:**
```
GROUP       TOPIC                  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
group_json  kafka-spring-producer  0          15              15              0
```
> **LAG = 0** → consumer is caught up. **LAG > 0** → messages are pending.

### Describe All Consumer Groups
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups
```

### Reset Offset to Beginning
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group group_json \
  --reset-offsets \
  --to-earliest \
  --topic kafka-spring-producer \
  --execute
```

### Reset Offset to Latest
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group group_json \
  --reset-offsets \
  --to-latest \
  --topic kafka-spring-producer \
  --execute
```

### Reset Offset to Specific Offset Number
```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group group_json \
  --reset-offsets \
  --to-offset 10 \
  --topic kafka-spring-producer:0 \
  --execute
```

---

## 7. Broker & Cluster Info

### Show Broker API Versions
```bash
kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

### Show KRaft Quorum / Broker Status
```bash
kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  describe --status
```

### Check Cluster ID
```bash
kafka-cluster \
  --bootstrap-server localhost:9092 \
  cluster-id
```
**Output:**
```
Cluster ID: Ks_XXXXXXXXXXXXXXXXXXXXXXXX
```

### Check Topic Config (retention, segment size, etc.)
```bash
kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name kafka-spring-producer \
  --describe
```
**Output:**
```
  min.insync.replicas=1
  segment.bytes=1073741824
```

### Alter Topic Config (e.g. set retention to 1 day)
```bash
kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name kafka-spring-producer \
  --alter \
  --add-config retention.ms=86400000
```

---

## 8. Full End-to-End Flow

```bash
# 1. Start Kafka
brew services start kafka

# 2. Create topic with 3 partitions
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic kafka-spring-producer \
  --partitions 3 \
  --replication-factor 1

# 3. Verify topic
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic kafka-spring-producer

# 4. Produce messages  →  open Terminal 1
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer

# 5. Consume messages  →  open Terminal 2
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer \
  --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true

# 6. Check offsets
kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic kafka-spring-producer

# 7. Check consumer lag
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-consumer-group
```

---

## Quick Reference Cheat Sheet

| Purpose | Command |
|---|---|
| Check Kafka status | `brew services list` |
| Start Kafka | `brew services start kafka` |
| Stop Kafka | `brew services stop kafka` |
| Restart Kafka | `brew services restart kafka` |
| List all topics | `kafka-topics --list --bootstrap-server localhost:9092` |
| Create topic | `kafka-topics --create --topic X --bootstrap-server localhost:9092` |
| Describe topic | `kafka-topics --describe --topic X --bootstrap-server localhost:9092` |
| Delete topic | `kafka-topics --delete --topic X --bootstrap-server localhost:9092` |
| Produce messages | `kafka-console-producer --topic X --bootstrap-server localhost:9092` |
| Consume from beginning | `kafka-console-consumer --topic X --from-beginning --bootstrap-server localhost:9092` |
| Show partition + offset | `--property print.partition=true --property print.offset=true` |
| Show timestamps | `--property print.timestamp=true` |
| Show latest offsets | `kafka-get-offsets --topic X --bootstrap-server localhost:9092` |
| List consumer groups | `kafka-consumer-groups --list --bootstrap-server localhost:9092` |
| Check consumer lag | `kafka-consumer-groups --describe --group G --bootstrap-server localhost:9092` |
| Reset offset to start | `kafka-consumer-groups --reset-offsets --to-earliest --execute` |
| Check cluster ID | `kafka-cluster --bootstrap-server localhost:9092 cluster-id` |
| Broker KRaft status | `kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status` |

---

> ⚠️ **Mac Homebrew Note:** Always use commands **without** `.sh` extension.  
> ✅ `kafka-topics` → works  
> ❌ `kafka-topics.sh` → `zsh: command not found`
