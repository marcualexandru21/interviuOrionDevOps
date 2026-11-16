# Kafka și Fluent Bit pe EKS (Strimzi KRaft, mTLS)

Logurile JSON ale aplicației pyapp (5g-session-analyzer) sunt colectate de Fluent Bit de pe noduri și
publicate în Kafka. Kafka este buffer-ul dintre colectare și indexare (Logstash → Elasticsearch, faza
următoare): dacă indexarea este indisponibilă, logurile rămân în Kafka pe durata retenției, iar
consumatorul își reia citirea de la ultimul offset confirmat.

```
pyapp (stdout JSON) ──> Fluent Bit (DaemonSet, ns logging) ──TLS + cert client :9093──> Kafka (ns kafka, data-nodes)
                          ├─ level=ERROR ─> 5g-network-errors
                          └─ restul ──────> 5g-network-logs
```

| Fișier | Conținut |
|---|---|
| [`strimzi-values.yaml`](strimzi-values.yaml) | Operatorul Strimzi: namespace-uri urmărite, plasare pe app-nodes |
| [`kafka-cluster.yaml`](kafka-cluster.yaml) | Node pool KRaft + cluster: listener TLS/mTLS, ACL, plasare pe data-nodes |
| [`kafka-topic.yaml`](kafka-topic.yaml) | `5g-network-logs`, `5g-network-errors` |
| [`kafka-users.yaml`](kafka-users.yaml) | `fluent-bit-producer`, `logstash-consumer`: certificate, ACL-uri, quotas |
| [`sync-cluster-ca.sh`](sync-cluster-ca.sh) | Copiază CA-ul public al clusterului în namespace-ul logging |
| [`kcat-debug.yaml`](kcat-debug.yaml) | Client de test temporar (identitatea consumatorului) |
| [`../FluentBit/values.yaml`](../FluentBit/values.yaml) | Colectare, metadate Kubernetes, rutare pe level, output Kafka TLS |

---

## 1. Decizii

### Securitate

| Decizie | Configurare | Motiv |
|---|---|---|
| Doar TLS, fără plaintext | listener `tls` pe 9093, `tls: true` | Encryption in transit pentru tot traficul de loguri (PCI DSS Req. 4, NIS2). |
| Autentificare cu certificat de client | `authentication.type: tls`, câte un `KafkaUser` per client | Fiecare client are identitate proprie, revocabilă individual; fără parole partajate. |
| Autorizare pe operații | `authorization.type: simple`, ACL în `KafkaUser` | Producătorul poate doar `Write` în `5g-network-*`; consumatorul doar `Read` și doar în grupul `logstash-es`. |
| Quotas | `producerByteRate` 1 MiB/s, `consumerByteRate` 2 MiB/s | Un log storm sau un client defect nu poate satura brokerul (noisy neighbour). |
| Izolare de rețea | `networkPolicyPeers`: doar namespace-ul `logging` | Strimzi generează NetworkPolicy pentru listener; se aplică odată cu activarea network policy în VPC CNI. |
| Certificatele clienților în namespace-ul clienților | `userOperator.watchedNamespace: logging` | Secret-ul cu cheia privată a clientului nu se copiază între namespace-uri. Se copiază doar CA-ul public al clusterului. |

### Date și resurse

| Decizie | Configurare | Motiv |
|---|---|---|
| KRaft, fără ZooKeeper | `KafkaNodePool` controller + broker | Mai puține componente de operat; un singur tip de pod de recuperat. |
| Plasare pe nodul de date | nodeAffinity `workload=data` + toleration `dedicated=data` | Kafka nu concurează cu aplicațiile pentru memorie; volumul EBS rămâne în aceeași zonă cu nodul. |
| Stocare persistentă criptată | `gp3-storage`, `deleteClaim: false` | Encryption at rest; PVC-ul și discul (`Retain`) supraviețuiesc ștergerii resursei. |
| Heap sub limita pod-ului | `-Xmx768m`, limită `1536Mi` | Restul memoriei este page cache pentru segmente (zero-copy). |
| Topicuri doar declarative | `auto.create.topics.enable: "false"` | Niciun topic fără retenție creat implicit de un client. |
| Separarea erorilor | `5g-network-errors` (24h) vs `5g-network-logs` (6h) | Erorile sunt păstrate mai mult pentru investigații și pot avea consumator propriu. |
| Cheie de partiționare | `Message_Key_Field gnb_id` | Evenimentele unei antene ajung în aceeași partiție: ordinea per gNodeB se păstrează. |
| 3 partiții / topic | `partitions: 3` | Până la 3 consumatori paraleli în același grup (scalare orizontală a indexării). |
| Rotație de segmente pe timp | `segment.ms` 1h | Segmentul activ nu este șters niciodată; fără rotație pe timp, retenția nu se aplică la trafic mic. |
| Livrare sigură din Fluent Bit | `acks=-1`, buffer pe disc, `Retry_Limit no_limits` | La indisponibilitatea Kafka logurile se acumulează pe nod (max 256 MiB), nu se pierd. |

### Buget de resurse (requests)

| Componentă | Nod | CPU | Memorie |
|---|---|---|---|
| Kafka broker/controller | data-nodes | 250m | 1 GiB (limită 1.5 GiB) |
| Entity Operator (topic + user) | data-nodes | 100m | 512 MiB |
| Strimzi Cluster Operator | app-nodes | 200m | 384 MiB |
| Fluent Bit | fiecare nod | 20m | 32 MiB (limită 128 MiB) |

Pe data-nodes (t3.large) rămân ~5 GiB pentru Elasticsearch și Logstash.

## 2. Lab vs. producție

| Parametru | Lab | Producție |
|---|---|---|
| Brokeri | 1 (controller + broker) | 3 brokeri + 3 controlleri dedicați, pe AZ-uri diferite |
| Replication factor / `min.insync.replicas` | 1 / 1 | 3 / 2 |
| Distribuirea CA-ului clusterului | `sync-cluster-ca.sh` | cert-manager trust-manager sau External Secrets |
| Monitorizare | — (faza de observabilitate) | Kafka Exporter (consumer lag), JMX metrics, alerte pe under-replicated partitions |

---

## 3. Instalare

```bash
# 1. Namespace-uri
kubectl create namespace kafka
kubectl create namespace logging

# 2. Operatorul Strimzi
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi-cluster-operator strimzi/strimzi-kafka-operator \
  --version 1.3.1 -n kafka -f Kafka/strimzi-values.yaml   # Strimzi 1.3.1 suportă Kafka 4.3.2

# 3. Clusterul Kafka. data-nodes pornește de la 0 noduri: Cluster Autoscaler adaugă nodul (~3-4 min).
kubectl apply -f Kafka/kafka-cluster.yaml
kubectl wait kafka/orion-kafka --for=condition=Ready --timeout=600s -n kafka

# 4. Topicuri și utilizatori
kubectl apply -f Kafka/kafka-topic.yaml
kubectl apply -f Kafka/kafka-users.yaml
kubectl wait kafkauser --all --for=condition=Ready --timeout=120s -n logging

# 5. CA-ul clusterului în namespace-ul logging
bash Kafka/sync-cluster-ca.sh

# 6. Fluent Bit
helm repo add fluent https://fluent.github.io/helm-charts
helm install fluent-bit fluent/fluent-bit --version 0.58.3 \
  -n logging -f FluentBit/values.yaml   # Fluent Bit 5.1.3
```

## 4. Verificare

```bash
# Resurse Strimzi
kubectl get kafka,kafkanodepool,kafkatopic -n kafka
kubectl get kafkauser -n logging
kubectl get pods -n kafka -o wide            # broker și entity operator pe nodul workload=data

# Fluent Bit: fără erori de TLS / autorizare
kubectl logs -n logging daemonset/fluent-bit --tail=50

# Mesajele ajung în Kafka (citite cu identitatea consumatorului)
kubectl apply -f Kafka/kcat-debug.yaml
kubectl wait pod/kafka-debug-client -n logging --for=condition=Ready

KCAT="kcat -b orion-kafka-kafka-bootstrap.kafka.svc:9093 -X security.protocol=ssl \
  -X ssl.ca.location=/certs/ca/ca.crt \
  -X ssl.certificate.location=/certs/user/user.crt \
  -X ssl.key.location=/certs/user/user.key"

kubectl exec -n logging kafka-debug-client -- sh -c "$KCAT -L"                                  # metadate: topicuri, partiții
kubectl exec -n logging kafka-debug-client -- sh -c "$KCAT -C -t 5g-network-logs -o -3 -e -f 'p%p key=%k %s\n'"
kubectl exec -n logging kafka-debug-client -- sh -c "$KCAT -C -t 5g-network-errors -o -3 -e -f 'p%p key=%k %s\n'"

# ACL: consumatorul NU are voie să scrie -> Topic authorization failed
kubectl exec -n logging kafka-debug-client -- sh -c "echo test | $KCAT -P -t 5g-network-logs"

kubectl delete -f Kafka/kcat-debug.yaml
```

Rezultat așteptat: mesajele conțin câmpurile aplicației (`level`, `slice_id`, `gnb_id`,
`active_ue_connections`, `latency_ms`, `message`) plus metadatele `kubernetes.*`; cheia (`key=`) este
`gnb_id`; după cele 30 de secunde de warm-up ale aplicației, evenimentele ERROR apar doar în
`5g-network-errors`.

## 5. Puncte de conectare

- Bootstrap (intern, TLS): `orion-kafka-kafka-bootstrap.kafka.svc:9093`
- Topicuri: `5g-network-logs`, `5g-network-errors`
- Consumer group rezervat pentru indexare: `logstash-es`
