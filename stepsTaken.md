# Steps Taken: platformă EKS pentru telemetrie 5G, de la zero

Acest document descrie **tot** ce trebuie făcut, în ordine, ca să reconstruiești platforma de la un cont AWS gol:
serverul Jenkins, clusterul EKS, aplicația, monitorizarea, Kafka și pipeline-ul de loguri până în Kibana.

Pentru fiecare componentă găsești:
- **ce este și ce problemă rezolvă**, explicat pentru cineva care nu a mai lucrat cu ea;
- **codul final**, copiat exact din fișierele din repository;
- **comenzile**, în ordinea în care se rulează, plus verificarea după fiecare pas;
- **de ce am făcut-o așa**: decizia și ce se strică dacă nu o faci.

Arhitectura pe scurt:

```
GitHub ──poll──> Jenkins (EC2, IAM role) ──push──> ECR
                     │
                     └──kubectl apply (EKS Access Entry + RBAC)──> EKS (o singură zonă: eu-central-1a)
                                                                    │
  app-nodes (t3.large, 1-3, autoscaling)                            │  data-nodes (t3.large, 0-1, taint dedicated=data)
  ├─ pyapp x2 (5G telemetry, /metrics + loguri JSON)                │  ├─ Kafka (Strimzi KRaft, TLS + mTLS + ACL)
  ├─ Prometheus, Alertmanager, Grafana, kube-state-metrics          │  ├─ Elasticsearch (ECK, data stream, ILM 7 zile)
  ├─ Cluster Autoscaler (IRSA), operatorii Strimzi și ECK           │  ├─ Logstash (consumer group logstash-es)
  └─ CoreDNS, metrics-server, EBS CSI controller                    │  └─ Kibana
  pe toate nodurile: node-exporter, Fluent Bit, aws-node, kube-proxy, ebs-csi-node

Flux de loguri:  pyapp ─stdout─> Fluent Bit ─mTLS─> Kafka ─mTLS─> Logstash ─HTTPS─> Elasticsearch ─> Kibana
Flux de metrici: pyapp /metrics <─scrape─ Prometheus ─> Alertmanager ─> PagerDuty / email / Slack
```

---

## 0. Înainte de început: ce înlocuiești

Fișierele conțin valori legate de contul meu AWS. Pe alt cont, le înlocuiești peste tot (`grep -rn` după ele):

| Valoare în fișiere | Ce reprezintă | Unde apare |
|---|---|---|
| `216989107224` | ID-ul contului AWS | `Jenkinsfile`, `k8s/cluster-config.yaml`, `k8s/pyapp-deployment.yaml` |
| `arn:aws:kms:eu-central-1:216989107224:key/02388489-...` | cheia KMS pentru secretele EKS | `k8s/cluster-config.yaml` |
| `203.0.113.10/32`, `203.0.113.20/32` | IP-ul tău public și Elastic IP-ul Jenkins (valori de exemplu, RFC 5737) | `k8s/cluster-config.yaml` |
| `intervieworion/pyapp` | numele repository-ului ECR | `Jenkinsfile`, `k8s/pyapp-deployment.yaml` |
| `bootcamp-cluster`, `eu-central-1` | numele clusterului și regiunea | mai multe fișiere |
| `telecom-lab.example` | domeniu de exemplu pentru email-urile Alertmanager | `Prometheus/alertmanager.yaml` |

Ordinea de lucru și durata aproximativă:

| # | Etapă | Timp |
|---|---|---|
| 1 | Unelte locale | 15 min |
| 2 | Fundația AWS (KMS, ECR, IAM) | 15 min |
| 3 | Serverul Jenkins | 30 min |
| 4 | Aplicația și pipeline-ul (cod) | — |
| 5 | Clusterul EKS | 20 min (de așteptat) |
| 6 | StorageClass | 2 min |
| 7 | Cluster Autoscaler | 5 min |
| 8 | Namespace, RBAC, primul deploy | 10 min |
| 9 | Prometheus, Grafana, Alertmanager | 15 min |
| 10 | Kafka | 15 min |
| 11 | Fluent Bit | 5 min |
| 12 | Test cap-coadă Kafka | 5 min |
| 13 | Elasticsearch, Logstash, Kibana | 20 min |
| 14 | Verificare finală | 5 min |

---

## 1. Uneltele locale (Windows, Git Bash)

Infrastructura (clusterul, node group-urile, politicile IAM) se creează **de pe calculatorul administratorului**, nu din Jenkins.

### Comenzile:
```bash
# Unelte necesare (versiuni recente): AWS CLI v2, eksctl, kubectl, helm, git. openssl vine cu Git for Windows.
aws --version
eksctl version
kubectl version --client
helm version
git --version
openssl version

# Profilul AWS al administratorului
aws configure            # Access key ID / Secret / region: eu-central-1 / output: json
aws sts get-caller-identity
```

### De ce am făcut-o așa:
1. **Separarea Day 1 / Day 2.** Day 1 înseamnă crearea infrastructurii, făcută de administrator, rar și controlat. Day 2 înseamnă deploy-ul aplicațiilor, făcut de Jenkins, des și automat. Jenkins nu primește drepturi de administrator: un script compromis din pipeline ar putea altfel șterge tot contul.
2. **Utilizatorul care creează clusterul devine automat administratorul lui** (un Access Entry cu drepturi de `cluster-admin`). Folosește mereu aceeași identitate, altfel pierzi accesul.
3. **`kubectl` trebuie să fie cu cel mult o versiune minoră diferit de cluster** (1.36–1.38 pentru un cluster 1.37), altfel comenzile se pot comporta neașteptat.

---

## 2. Fundația AWS: KMS, ECR, IAM

Resursele de mai jos există **înaintea** clusterului, pentru că `cluster-config.yaml` le referă prin ARN.

### 2.1 Cheia KMS pentru secretele Kubernetes

```bash
aws kms create-key --region eu-central-1 \
  --description "EKS secrets encryption - bootcamp-cluster" \
  --query 'KeyMetadata.Arn' --output text
# Pui ARN-ul afișat în k8s/cluster-config.yaml, la secretsEncryption.keyARN

aws kms create-alias --region eu-central-1 --alias-name alias/eks-telecom --target-key-id <ARN>
aws kms enable-key-rotation --region eu-central-1 --key-id <ARN>
```

**De ce:** Secret-ele Kubernetes (parole, certificate) sunt stocate în etcd doar codificate base64, nu criptate. Cu `secretsEncryption`, EKS le criptează cu cheia ta KMS (*envelope encryption*). Cine are acces la discurile etcd sau la un backup nu le poate citi. Rotația automată anuală a cheii e o cerință PCI DSS (gestionarea cheilor criptografice).

### 2.2 Repository-ul ECR pentru imagini

```bash
aws ecr create-repository --region eu-central-1 \
  --repository-name intervieworion/pyapp \
  --image-tag-mutability IMMUTABLE \
  --image-scanning-configuration scanOnPush=true

# Păstrează doar ultimele 20 de imagini (costuri și suprafață de atac mai mici)
aws ecr put-lifecycle-policy --region eu-central-1 --repository-name intervieworion/pyapp \
  --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"keep last 20","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":20},"action":{"type":"expire"}}]}'
```

**De ce:**
1. **`IMMUTABLE`:** un tag (`v14`) nu poate fi suprascris. Ce rulează în cluster sub `v14` e garantat aceeași imagine care a fost testată. Consecință: dacă recreezi job-ul Jenkins, numerotarea build-urilor pornește iar de la 1, iar push-ul pentru `v1` eșuează cu `tag already exists`. Atunci ștergi imaginile vechi sau schimbi schema de tag-uri.
2. **`scanOnPush`:** ECR scanează fiecare imagine pentru vulnerabilități cunoscute (CVE), gratuit.

### 2.3 Politica IAM pentru Cluster Autoscaler

### Codul (`k8s/eks-cluster-autoscaler-policy.json`):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "autoscaling:DescribeAutoScalingGroups",
                "autoscaling:DescribeAutoScalingInstances",
                "autoscaling:DescribeLaunchConfigurations",
                "autoscaling:DescribeTags",
                "ec2:DescribeLaunchTemplateVersions",
                "ec2:DescribeInstanceTypes",
                "eks:DescribeNodegroup"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "autoscaling:SetDesiredCapacity",
                "autoscaling:TerminateInstanceInAutoScalingGroup"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceTag/k8s.io/cluster-autoscaler/bootcamp-cluster": "owned"
                }
            }
        }
    ]
}
```

```bash
aws iam create-policy \
  --policy-name AmazonEKSClusterAutoscalerPolicy \
  --policy-document file://k8s/eks-cluster-autoscaler-policy.json
# La modificări ulterioare: aws iam create-policy-version ... --set-as-default (max. 5 versiuni)
```

### De ce am făcut-o așa:
1. **Două statement-uri.** Acțiunile de citire (`Describe*`) sunt permise pe `*`, pentru că autoscaler-ul trebuie să vadă grupurile. Acțiunile care **schimbă** ceva (`SetDesiredCapacity`, `TerminateInstanceInAutoScalingGroup`) sunt permise doar pe ASG-urile cu tag-ul `k8s.io/cluster-autoscaler/bootcamp-cluster=owned`.
2. **Ce se strică fără condiție:** autoscaler-ul (sau cine îi fură token-ul) ar putea opri instanțe din **orice** Auto Scaling Group din cont, inclusiv din alte clustere sau aplicații.
3. EKS pune automat acest tag pe ASG-urile managed node group-urilor clusterului.
4. Politica e atașată **doar pod-ului** autoscaler-ului, prin IRSA (secțiunea 5), nu nodului.

### 2.4 Rolul IAM pentru Jenkins

Jenkins nu primește access keys. Primește un **IAM Role** atașat instanței EC2: AWS îi dă automat credențiale temporare, rotite la câteva ore, prin metadata service (IMDS).

```bash
cat > /tmp/ec2-trust.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Principal": { "Service": "ec2.amazonaws.com" }, "Action": "sts:AssumeRole" }
  ]
}
EOF

cat > /tmp/jenkins-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ECRPermissions",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:GetRepositoryPolicy",
        "ecr:DescribeRepositories",
        "ecr:ListImages",
        "ecr:DescribeImages",
        "ecr:BatchGetImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EKSPermissions",
      "Effect": "Allow",
      "Action": [
        "eks:DescribeCluster",
        "eks:ListClusters",
        "eks:AccessKubernetesApi"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam create-role --role-name Jenkins-EKS-ECR-Role \
  --assume-role-policy-document file:///tmp/ec2-trust.json
aws iam put-role-policy --role-name Jenkins-EKS-ECR-Role \
  --policy-name jenkins-ecr-eks --policy-document file:///tmp/jenkins-policy.json
aws iam attach-role-policy --role-name Jenkins-EKS-ECR-Role \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

aws iam create-instance-profile --instance-profile-name Jenkins-EKS-ECR-Role
aws iam add-role-to-instance-profile --instance-profile-name Jenkins-EKS-ECR-Role \
  --role-name Jenkins-EKS-ECR-Role
```

### De ce am făcut-o așa:
1. **Fără chei statice.** O cheie `AKIA...` salvată pe server sau în Jenkins poate fi copiată și folosită oriunde, ani la rând. Credențialele temporare ale rolului expiră singure.
2. **Jenkins poate doar:** să publice imagini în ECR, să afle adresa clusterului (`DescribeCluster`) și să se autentifice la API-ul Kubernetes. Nu poate crea sau șterge clustere, noduri sau roluri.
3. **Ce poate face în cluster** nu se decide aici, ci în Kubernetes RBAC (secțiunea 8). IAM răspunde la „cine ești”, RBAC la „ce ai voie”.
4. Varianta mai strictă pentru producție: `Resource` limitat la ARN-ul repository-ului ECR și al clusterului, în loc de `*`.

---

## 3. Serverul Jenkins (EC2)

Jenkins este serverul de CI/CD: la fiecare commit, construiește imaginea Docker, o publică în ECR și face deploy în EKS.

### 3.1 Instanța EC2

| Setare | Valoare | De ce |
|---|---|---|
| AMI | Ubuntu Server 24.04 LTS | suport lung, pachete Jenkins oficiale |
| Tip | t3.medium | Jenkins (Java) + build-uri Docker au nevoie de ~4 GiB |
| Disc | 30 GiB gp3, **criptat** | imaginile Docker ocupă spațiu; criptare at rest |
| IAM instance profile | `Jenkins-EKS-ECR-Role` | credențiale temporare, fără chei |
| Security Group | 22 și 8080 **doar de la IP-ul tău** | interfața Jenkins nu e publică |
| Elastic IP | da | IP fix, necesar în `publicAccessCIDRs` din clusterul EKS |
| IMDS | v2 obligatoriu (implicit pe AMI-urile noi) | protecție împotriva furtului de credențiale prin SSRF |

```bash
# Elastic IP pentru Jenkins (după ce instanța pornește)
aws ec2 allocate-address --region eu-central-1 --query AllocationId --output text
aws ec2 associate-address --region eu-central-1 --instance-id <i-...> --allocation-id <eipalloc-...>
```

### 3.2 Instalarea Jenkins și a uneltelor

Te conectezi prin SSH (sau prin SSM Session Manager, fără port 22) și rulezi ca root (`sudo su -`):

```bash
# 1. Cheia GPG a repository-ului Jenkins
mkdir -p ~/.gnupg && chmod 700 ~/.gnupg
apt-get update
apt-get install -y dirmngr gnupg
gpg --no-default-keyring --keyring /usr/share/keyrings/jenkins-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 5BA31D57EF5975CA
echo "deb [signed-by=/usr/share/keyrings/jenkins-keyring.gpg] https://pkg.jenkins.io/debian-stable binary/" \
  > /etc/apt/sources.list.d/jenkins.list

# 2. Java 21 (cerut de Jenkins LTS) și Jenkins
apt-get update
apt-get install -y openjdk-21-jre jenkins unzip
systemctl enable --now jenkins

# 3. Docker; utilizatorul jenkins poate rula docker
curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh
usermod -aG docker jenkins
systemctl enable --now docker
systemctl restart jenkins

# 4. ECR credential helper: Docker ia singur token-ul ECR din IAM Role
apt-get install -y amazon-ecr-credential-helper
mkdir -p /var/lib/jenkins/.docker
echo '{"credsStore": "ecr-login"}' > /var/lib/jenkins/.docker/config.json
chown -R jenkins:jenkins /var/lib/jenkins/.docker

# 5. AWS CLI v2, kubectl (aceeași versiune minoră ca clusterul), eksctl
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip && ./aws/install
curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
install -m 0755 kubectl /usr/local/bin/kubectl
curl -sL "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_Linux_amd64.tar.gz" | tar xz -C /tmp
mv /tmp/eksctl /usr/local/bin

# 6. Verificare
docker --version && aws --version && kubectl version --client
sudo -u jenkins aws sts get-caller-identity     # trebuie să afișeze assumed-role/Jenkins-EKS-ECR-Role
cat /var/lib/jenkins/secrets/initialAdminPassword
```

### 3.3 Configurarea din interfața Jenkins (`http://<Elastic-IP>:8080`)

1. Introduci parola inițială → **Install suggested plugins** → creezi utilizatorul admin.
2. **Credențiala GitHub.** Pe GitHub: Settings → Developer settings → **Fine-grained token**, doar pentru repository-ul proiectului, permisiunea **Contents: Read-only**, cu expirare.
   În Jenkins: Manage Jenkins → Credentials → Global → Add → **Username with password**: username = utilizatorul GitHub, password = token-ul, **ID = `github-credentials`**.
3. **Job-ul:** New Item → `pyapp-pipeline` → **Pipeline**:
   - Pipeline → Definition: **Pipeline script from SCM** → Git → URL-ul repository-ului, credentials `github-credentials`, branch `*/main`, Script Path `Jenkinsfile`;
   - Build Triggers → **Poll SCM** → `H/2 * * * *`.

### De ce am făcut-o așa:
1. **Credential helper în loc de `docker login`.** `aws ecr get-login-password | docker login` salvează token-ul în clar în `~/.docker/config.json`. Helper-ul obține token-ul la nevoie, din rolul IAM, și nu-l scrie nicăieri.
2. **Token GitHub fine-grained, read-only, pe un singur repository și cu expirare.** Dacă e furat, poate doar citi codul unui repository, și doar până expiră. Un token clasic are acces la toate repository-urile tale.
3. **Token-ul stă în Jenkins Credentials** (criptat), nu în URL-ul repository-ului (`https://ghp_xxx@github.com/...`), unde ar ajunge în `.git/config` și în logurile build-ului.
4. **Poll SCM în loc de webhook.** Un webhook GitHub cere ca portul 8080 să fie accesibil de pe internet. Am preferat ca Jenkins să întrebe GitHub la 2 minute, cu Security Group-ul închis. În producție: webhook printr-un reverse proxy cu IP-urile GitHub în allowlist.

---

## 4. Aplicația, imaginea și pipeline-ul

### 4.1 Aplicația (`pyApp/app.py`)

Simulează un serviciu de telemetrie 5G: emite evenimente pentru o antenă (`gnb_id`) și un network slice (`slice_id`), cu latență și număr de conexiuni (UE). În primele 30 de secunde trafic normal, apoi un incident: latență 250 ms, conexiunile scad la 50, toate evenimentele `ERROR`. Așa poți declanșa și verifica alertele și logurile oricând, cu un simplu restart de pod.

### Codul:
```python
import json
import logging
import os
import random
import signal
import sys
import time

from prometheus_client import Counter, Gauge, start_http_server

SERVICE = os.environ.get("SERVICE_NAME", "5g-session-analyzer")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
INTERVAL_MIN = float(os.environ.get("LOG_INTERVAL_MIN", "0.5"))
INTERVAL_MAX = float(os.environ.get("LOG_INTERVAL_MAX", "2.0"))

# Fixed label sets. Random gNodeB IDs would create hundreds of Prometheus series.
SLICES = ("sst-1", "sst-2", "sst-3")
GNBS = ("gNodeB-101", "gNodeB-202", "gNodeB-303")

EVENTS = Counter(
    "session_events_total",
    "Log events emitted by the session analyzer",
    ["level", "slice_id"],
)
ACTIVE_UE = Gauge(
    "active_ue_connections",
    "Active UE connections on a RAN slice",
    ["slice_id", "gnb_id"],
)
LATENCY = Gauge(
    "network_latency_ms",
    "Last observed RAN latency in milliseconds",
    ["slice_id", "gnb_id"],
)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(message)s"))
logger = logging.getLogger(SERVICE)
logger.setLevel(logging.INFO)
logger.propagate = False
logger.addHandler(handler)

stop = False

# Salvăm momentul pornirii pentru a ști când să declanșăm incidentul
START_TIME = time.time()

def handle_stop(signum, _frame):
    global stop
    stop = True
    emit("INFO", SLICES[0], GNBS[0], 0, 0, f"shutdown signal {signum}")

def emit(level, slice_id, gnb_id, active_ue, latency_ms, message):
    EVENTS.labels(level=level, slice_id=slice_id).inc()
    ACTIVE_UE.labels(slice_id=slice_id, gnb_id=gnb_id).set(active_ue)
    LATENCY.labels(slice_id=slice_id, gnb_id=gnb_id).set(latency_ms)
    logger.info(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "service": SERVICE,
        "slice_id": slice_id,
        "gnb_id": gnb_id,
        "active_ue_connections": active_ue,
        "latency_ms": latency_ms,
        "message": message,
    }))

def next_event():
    # Fixăm un slice și un gnb specific pentru a concentra erorile într-o singură serie de date
    slice_id = SLICES[0]
    gnb_id = GNBS[0]

    # ---------------------------------------------------------------------------------
    # SIMULARE DE INCIDENT (CHAOS ENGINEERING)
    # ---------------------------------------------------------------------------------
    # În primele 30 de secunde raportăm trafic sănătos (UE=5000, Latență=20ms).
    # Acest pas este necesar ca Prometheus să înregistreze starea `max_over_time > 1000`.
    if time.time() - START_TIME < 30:
        level = "INFO"
        active_ue = 5000
        latency_ms = 20
        message = "[WARMUP] Trafic normal generat pentru baseline-ul Prometheus..."
    else:
        # După 30 de secunde, declanșăm colapsul total pe toate metricele simultan.
        level = "ERROR"         # Declanșează HighApplicationErrorLogs (Rată de eroare 100%)
        active_ue = 50          # Declanșează MassiveUEDrop (A scazut sub 100 din 5000)
        latency_ms = 250        # Declanșează HighRANLatency (Sare peste 150ms)
        message = "[SIMULARE INCIDENT CRITIC] Drop masiv de conexiuni și latență critică!"

    return level, slice_id, gnb_id, active_ue, latency_ms, message

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    start_http_server(METRICS_PORT)
    emit("INFO", SLICES[0], GNBS[0], 0, 0, f"metrics listening on {METRICS_PORT}")
    while not stop:
        emit(*next_event())
        time.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX))
```

### De ce am făcut-o așa:
1. **Metrici Prometheus native** (`prometheus_client`, portul 8000, `/metrics`), fără un exporter separat.
2. **Label-uri cu valori fixe** (`SLICES`, `GNBS`). Fiecare combinație unică de label-uri e o serie de timp separată în Prometheus. Un ID generat aleator ar crea mii de serii și ar umple memoria Prometheus (*cardinality explosion*).
3. **Fără label-ul `service` pe metrici.** Identitatea (`service`, `namespace`, `pod`, `job`) o adaugă Prometheus din Kubernetes. Dacă o punea și aplicația, Prometheus redenumea label-ul aplicației în `exported_service`: două surse pentru aceeași informație.
4. **Loguri JSON pe stdout.** Standardul cloud-native: containerul nu scrie fișiere, runtime-ul colectează stdout, iar Fluent Bit parsează JSON-ul fără expresii regulate fragile.
5. **Graceful shutdown.** Kubernetes trimite `SIGTERM` înainte să oprească un pod; aplicația se închide curat, în loc să fie omorâtă cu `SIGKILL` după 30 de secunde.

### 4.2 Imaginea Docker (`Dockerfile`)

### Codul:
```dockerfile
FROM python:3.11-slim AS builder
# Instalam direct in /root/.local
RUN pip install --no-cache-dir --user prometheus-client==0.21.1

FROM python:3.11-slim AS runner
WORKDIR /app

RUN groupadd -g 10001 appgroup \
    && useradd -u 10001 -g appgroup -m -s /bin/bash appuser

COPY --from=builder /root/.local /home/appuser/.local
COPY pyApp/app.py .

RUN chown -R appuser:appgroup /app /home/appuser/.local
USER 10001

ENV PATH=/home/appuser/.local/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    METRICS_PORT=8000

EXPOSE 8000
CMD ["python", "app.py"]
```

### De ce am făcut-o așa:
1. **Multi-stage build.** Etapa `builder` instalează dependențele; imaginea finală le copiază doar pe ele. Nu ajung în producție cache-uri pip sau unelte de build: imagine mai mică, mai puține vulnerabilități.
2. **Versiune fixă** (`prometheus-client==0.21.1`). Același Dockerfile produce aceeași imagine și peste un an.
3. **Utilizator non-root cu UID numeric (10001).** Dacă aplicația e compromisă, atacatorul nu e root în container. UID-ul numeric permite Kubernetes să verifice `runAsNonRoot`.
4. **`PYTHONUNBUFFERED=1`.** Logurile apar imediat pe stdout, nu rămân într-un buffer care se pierde la crash. **`PYTHONDONTWRITEBYTECODE=1`.** Python nu scrie fișiere `.pyc`, deci containerul poate rula cu sistem de fișiere read-only.

### 4.3 Pipeline-ul (`Jenkinsfile`)

### Codul:
```groovy
pipeline {
    agent any

    environment {
        AWS_REGION = 'eu-central-1'
        AWS_ACCOUNT_ID = '216989107224'
        ECR_REPO = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/intervieworion/pyapp"
        IMAGE_TAG = "v${env.BUILD_ID}" // Versionare unica pe baza numarului de build
        EKS_CLUSTER_NAME = 'bootcamp-cluster' 
    }

    stages {
        stage('Checkout') {
            steps {
                // Trage codul folosind credentialele din Jenkins
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    echo "Building image: ${ECR_REPO}:${IMAGE_TAG}"
                    sh "docker build -t ${ECR_REPO}:${IMAGE_TAG} ."
                }
            }
        }

        stage('Push to ECR') {
            steps {
                script {
                    echo "Pushing image..."
                    // FĂRĂ login explicit. Credential helper-ul se ocupa de asta in spate.
                    sh "docker push ${ECR_REPO}:${IMAGE_TAG}"
                }
            }
        }


        stage('Deploy to EKS') {
            steps {
                script {
                    echo "Checking AWS Identity..."
                    sh "aws sts get-caller-identity"

                    echo "Updating Kubeconfig for cluster ${EKS_CLUSTER_NAME}..."
                    sh "aws eks update-kubeconfig --region ${AWS_REGION} --name ${EKS_CLUSTER_NAME}"

                    echo "Injecting image tag into Kubernetes manifest..."
                    // Sed inlocuieste placeholder-ul cu tag-ul real (v1, v2, v3...)
                    sh "sed -i 's/LATEST_TAG_PLACEHOLDER/${IMAGE_TAG}/g' k8s/pyapp-deployment.yaml"

                    echo "Applying manifests..."
                    sh "kubectl apply -f k8s/pyapp-deployment.yaml"

                    echo "Waiting for rollout to finish..."
                    sh "kubectl rollout status deployment/pyapp-deployment -n telecom-apps --timeout=60s"
                }
            }
        }
    }

    post {
        always {
            script {
                echo "Cleaning up local Docker images to prevent disk space exhaustion..."
                // Daca nu faci asta, discul Jenkins-ului se umple in cateva zile in productie
                sh "docker rmi ${ECR_REPO}:${IMAGE_TAG} || true"

                // Stergem credentialele temporare de EKS
                sh "rm -rf ~/.kube/config || true"
            }
        }
        failure {
            echo "Pipeline failed! Check the logs."
        }
    }
}
```

### De ce am făcut-o așa:
1. **Tag unic per build (`v${BUILD_ID}`), niciodată `latest`.** Știi exact ce versiune rulează în cluster, poți reveni la una anterioară, iar ECR (imutabil) refuză suprascrierea.
2. **Fără `docker login`.** Push-ul folosește ECR credential helper-ul (secțiunea 3).
3. **`kubectl rollout status` cu timeout.** Pipeline-ul pică dacă pod-urile noi nu devin `Ready`, deci un deploy stricat nu apare ca „SUCCESS”.
4. **`post { always }`:** șterge imaginea locală (altfel discul Jenkins se umple în câteva zile) și kubeconfig-ul generat (nu lasă credențiale pe disc).
5. Ce lipsește încă: teste automate și scanare de vulnerabilități (Trivy) înainte de push (vezi secțiunea 17).

---

## 5. Clusterul EKS (`k8s/cluster-config.yaml`)

EKS este Kubernetes administrat de AWS: AWS operează control plane-ul (API server, etcd, scheduler), tu operezi nodurile (instanțe EC2) și ce rulează pe ele. `eksctl` creează totul (VPC, subnet-uri, NAT, cluster, node group-uri, roluri IAM) din fișierul de mai jos, prin CloudFormation.

### Codul:
```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: bootcamp-cluster
  region: eu-central-1
  version: "1.37"

  tags:
    Environment: "Demo"
    Project: "Network-Intelligence"
    Owner: "platform-team"

# =========================================================================
# Endpoint-ul API Kubernetes
# privateAccess: true  -> nodurile (privateNetworking) și traficul din VPC folosesc endpoint-ul privat.
# publicAccess: true   -> necesar pentru administrare și pentru Jenkins (EC2 în afara VPC-ului clusterului),
#                         dar restricționat la IP-urile din publicAccessCIDRs, nu 0.0.0.0/0.
# Valorile de mai jos sunt din intervalul de documentație (RFC 5737): se înlocuiesc cu IP-ul
# de egress al administratorului/VPN-ului și IP-ul public (Elastic IP) al Jenkins.
# Pe un cluster existent: eksctl utils set-public-access-cidrs -f k8s/cluster-config.yaml --approve
# Producție: publicAccess: false, acces doar prin VPN / bastion / SSM.
# =========================================================================
vpc:
  nat:
    gateway: Single  # un singur NAT gateway (cost); producție: unul per AZ
  clusterEndpoints:
    publicAccess: true
    privateAccess: true
  publicAccessCIDRs:
    - "203.0.113.10/32"   # admin / VPN egress
    - "203.0.113.20/32"   # Jenkins (Elastic IP)

# 1. Securizarea Control Plane-ului (KMS Encryption)
# CE SE STRICĂ DACĂ LIPSEȘTE: Secretele Kubernetes stau în clear-text pe discurile etcd administrate de AWS.
secretsEncryption:
  keyARN: "arn:aws:kms:eu-central-1:216989107224:key/02388489-f2e1-4cd7-af7a-f2b2889a58e2"

# Logurile control plane-ului: necesare pentru audit, compliance și troubleshooting.
cloudWatch:
  clusterLogging:
    enableTypes: ["api", "audit", "authenticator", "controllerManager", "scheduler"]

iam:
  withOIDC: true
  serviceAccounts:
    - metadata:
        name: cluster-autoscaler
        namespace: kube-system
      # IRSA (IAM Roles for Service Accounts): politica proprie, cu condiție pe tag-ul clusterului,
      # în locul wellKnownPolicies (prea largi). Permisiunea aparține doar pod-ului autoscaler-ului.
      attachPolicyARNs:
        - "arn:aws:iam::216989107224:policy/AmazonEKSClusterAutoscalerPolicy"

# =========================================================================
# Maparea identitatilor externe (AWS IAM) la Kubernetes RBAC
# DE CE FACEM ASTA: Pentru a elimina execuția manuală de comenzi
# (eksctl create iamidentitymapping). Astfel, la o reconstrucție a clusterului
# în caz de Disaster Recovery, permisiunile pentru CI/CD sunt aplicate automat,
# iar accesul este auditabil direct din istoricul Git.
# =========================================================================
accessConfig:
  authenticationMode: API_AND_CONFIG_MAP
  accessEntries:
    - principalARN: arn:aws:iam::216989107224:role/Jenkins-EKS-ECR-Role
      kubernetesUsername: jenkins-pipeline
      kubernetesGroups:
        - cicd-deployers

addons:
  - name: aws-ebs-csi-driver
    # Aici folosim IRSA. Permisiunea e dată doar POD-ului de CSI, nu nodului!
    wellKnownPolicies:
      ebsCSIController: true

managedNodeGroups:
  # Aplicatii + monitorizare (pyapp, Prometheus, Grafana, Cluster Autoscaler, CoreDNS).
  # O singura zona: volumele EBS (Prometheus/Grafana) nu pot trece intre AZ-uri.
  # Fara taint: componentele de sistem (CoreDNS, metrics-server, ebs-csi-controller) trebuie sa aiba unde rula.
  - name: app-nodes
    # t3.large: 8 GiB și max 35 pod-uri/nod (limita ENI/IP a VPC CNI; t3.medium are doar 17).
    instanceType: t3.large
    availabilityZones: ["eu-central-1a"]
    minSize: 1
    maxSize: 3
    desiredCapacity: 1
    labels:
      workload: apps
    propagateASGTags: true
    volumeType: gp3
    volumeSize: 30
    # Doar IP-uri private; discuri criptate; fără SSH (acces prin SSM Session Manager)
    privateNetworking: true
    volumeEncrypted: true
    # Fără withAddonPolicies.autoScaler: drepturile de autoscaling sunt doar pe pod-ul
    # autoscaler-ului (IRSA). Pe rolul nodului, orice pod de pe nod ar putea porni/opri instanțe.
    iam:
      attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

  # Date (Kafka, Elasticsearch, Logstash, Kibana). Aceeasi zona ca app-nodes.
  # Taint: nimic nu ajunge aici fara toleration explicita. minSize 0 = il poti opri cand nu lucrezi.
  - name: data-nodes
    instanceType: t3.large
    availabilityZones: ["eu-central-1a"]
    minSize: 0
    maxSize: 1
    desiredCapacity: 1
    labels:
      workload: data
    taints:
      - key: dedicated
        value: data
        effect: NoSchedule
    propagateASGTags: true
    volumeType: gp3
    volumeSize: 30
    privateNetworking: true
    volumeEncrypted: true
    iam:
      attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

#fargateProfiles:
#  - name: stateless-apps-profile
#    selectors:
#      - namespace: fargate-apps
```

### Comenzile:
```bash
# IP-ul tău public, pentru publicAccessCIDRs (+ Elastic IP-ul Jenkins)
curl -s https://checkip.amazonaws.com

# Verificare fără să creezi nimic
eksctl create cluster -f k8s/cluster-config.yaml --dry-run

# Creare (~20 minute)
eksctl create cluster -f k8s/cluster-config.yaml

# Verificare
kubectl get nodes -L workload,topology.kubernetes.io/zone     # 2 noduri: apps + data, ambele eu-central-1a
aws eks list-access-entries --cluster-name bootcamp-cluster    # include Jenkins-EKS-ECR-Role
aws autoscaling describe-tags --filters Name=key,Values=k8s.io/cluster-autoscaler/bootcamp-cluster
```

### De ce am făcut-o așa:
1. **Endpoint public restricționat + privat.** Nodurile vorbesc cu API-ul prin endpoint-ul privat, din VPC. Endpoint-ul public rămâne pentru administrator și Jenkins (care e în alt VPC), dar acceptă conexiuni doar de la IP-urile din `publicAccessCIDRs`. În producție: `publicAccess: false`, acces prin VPN sau bastion.
2. **Un singur NAT gateway:** economie în lab. În producție: câte unul per zonă, altfel căderea zonei cu NAT-ul taie internetul tuturor nodurilor.
3. **`secretsEncryption` cu cheia KMS** din secțiunea 2.1: Secret-ele Kubernetes sunt criptate în etcd.
4. **Toate tipurile de loguri ale control plane-ului în CloudWatch.** Logul `audit` răspunde la întrebarea „cine a făcut ce și când” (cine a citit un Secret, cine a făcut `kubectl exec`). Pentru compliance și investigarea incidentelor e obligatoriu.
5. **IRSA (IAM Roles for Service Accounts) pentru Cluster Autoscaler și EBS CSI driver.** `withOIDC: true` face ca AWS să aibă încredere în token-urile emise de Kubernetes. Fiecare pod care are nevoie de AWS primește propriul rol IAM, cu exact permisiunile lui. **Ce se strică dacă dai permisiunile nodului** (`withAddonPolicies: autoScaler: true`): orice pod de pe nod, inclusiv aplicația, ar putea cere sau opri instanțe EC2.
6. **Access Entries în loc de `aws-auth`.** Accesul Jenkins în cluster e o resursă a API-ului EKS, auditată în CloudTrail. Vechiul ConfigMap `aws-auth` era fragil: o greșeală de indentare te bloca afară din cluster. Jenkins primește doar grupul Kubernetes `cicd-deployers`, iar ce poate face acest grup se definește în RBAC (secțiunea 8).
7. **Două node group-uri cu roluri diferite:**
   - `app-nodes`: aplicații, monitorizare și componente de control. **Fără taint**: CoreDNS, metrics-server și EBS CSI controller nu au toleration pentru taint-uri custom; dacă toate nodurile ar avea taint, clusterul ar rămâne fără DNS. Aplicațiile sunt legate de el prin `nodeSelector: workload=apps`.
   - `data-nodes`: Kafka, Elasticsearch, Logstash, Kibana. **Cu taint `dedicated=data:NoSchedule`**: nimic nu ajunge aici fără toleration explicită, deci componentele de date nu concurează pentru memorie cu aplicațiile. `minSize: 0`: îl poți opri când nu lucrezi.
8. **O singură zonă de disponibilitate (`eu-central-1a`) pentru ambele.** Un volum EBS există într-o singură zonă. Dacă un pod cu volum (Prometheus, Kafka, Elasticsearch) e mutat pe un nod din altă zonă, volumul nu se poate atașa și pod-ul rămâne `Pending`. Am văzut asta în practică: după un preemption, Alertmanager a putut reporni pe alt nod doar pentru că ambele erau în aceeași zonă. Producție: câte un node group per zonă și 3 replici pentru fiecare componentă cu date.
9. **t3.large și pentru aplicații.** Pe EKS fiecare pod primește un IP real din VPC, iar numărul de IP-uri depinde de tipul instanței: t3.medium suportă maximum **17 pod-uri**, t3.large **35**. Pe t3.medium, Fluent Bit a rămas `Pending` cu `Too many pods`, deși nodul avea CPU și RAM libere. Alternativa de producție: **VPC CNI prefix delegation** (~110 pod-uri pe nod, fără instanțe mai mari).
10. **`propagateASGTags: true`:** copiază label-urile și taint-urile ca tag-uri pe ASG. Așa autoscaler-ul știe ce fel de nod va porni și poate porni `data-nodes` de la 0 noduri când apare un pod Kafka.
11. **Securitatea nodurilor:**
    - **IP-uri private:** nodurile nu sunt accesibile direct de pe internet.
    - **Discuri criptate:** encryption at rest pentru nodurile însele.
    - **Fără SSH:** accesul se face prin SSM Session Manager, auditat în CloudTrail.
    - **IMDSv2 obligatoriu:** eksctl îl activează implicit, ca protecție împotriva SSRF.

---

## 6. StorageClass: discuri EBS criptate (`k8s/storageclass-gp3.yaml`)

Un **StorageClass** spune Kubernetes cum să creeze discuri când o aplicație cere un volum persistent (PVC). Driver-ul EBS CSI (instalat ca addon în secțiunea 5) creează discul EBS în AWS și îl atașează nodului.

### Codul:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3-storage
  annotations:
    storageclass.kubernetes.io/is-default-class: "true" # K8s va folosi gp3 pentru orice PVC care nu cere o clasă specifică
provisioner: ebs.csi.aws.com
reclaimPolicy: Retain        # Discul EBS NU se sterge cand se sterge PVC-ul sau clusterul
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true   # Permite mărirea discului on-the-fly (fără restart la Pod)
parameters:
  type: gp3
  fsType: ext4
  encrypted: "true"          # Activează criptarea AWS KMS pentru securitate

# ==============================================================================
# NOTĂ DE ARHITECTURĂ:
#
# 1. volumeBindingMode: WaitForFirstConsumer
#    De ce? Forțează AWS-ul să creeze discul EBS exact în aceeași zonă de
#    disponibilitate (AZ) în care este programat Pod-ul să ruleze (ex: eu-central-1a).
#    Dacă folosești Immediate, AWS face discul instant într-o zonă aleatorie, iar
#    dacă Pod-ul e programat în altă zonă, K8s dă eroare de atașare (EBS-urile nu
#    pot traversa AZ-urile).
#
# 2. encrypted: "true"
#    De ce? Conform standardelor de securitate din mediul financiar (PCI-DSS)
#    și Telecom, absolut toate datele trebuie criptate "at rest".
#    Acest flag cere driverului CSI să cripteze EBS-ul cu cheia KMS default a AWS.
#
# 3. allowVolumeExpansion: true
#    De ce? Pentru rezolvarea incidentelor fără downtime. Dacă un volum
#    (ex: baza de date sau Prometheus) atinge 99% spațiu, putem edita
#    PVC-ul ca să cerem mai mult spațiu (ex: de la 10Gi la 20Gi). K8s și AWS
#    vor redimensiona partiția "la cald" (on-the-fly), fără să omoare Pod-ul.
#
# 4. reclaimPolicy: Retain
#    De ce? Cu Delete (default), stergerea PVC-ului sterge si discul EBS: un
#    'kubectl delete' gresit sau un 'helm uninstall' inseamna pierdere de date.
#    Cu Retain, PV-ul trece in starea 'Released' si discul ramane in AWS, de unde
#    poate fi reatasat sau folosit pentru snapshot. Costul: volumele ramase trebuie
#    curatate manual (aws ec2 describe-volumes --filters Name=status,Values=available).
#    In productie se combina cu snapshot-uri programate (AWS Backup / VolumeSnapshot),
#    pentru ca Retain protejeaza doar impotriva stergerii, nu si a coruperii datelor.
#    Nota: reclaimPolicy este imutabil pe un StorageClass (se aplica prin delete + apply).
# ==============================================================================
```

### Comenzile:
```bash
kubectl apply -f k8s/storageclass-gp3.yaml
kubectl get sc       # doar gp3-storage are (default); dacă și gp2 apare (default):
kubectl annotate sc gp2 storageclass.kubernetes.io/is-default-class-
```

### De ce am făcut-o așa:
Explicațiile sunt în comentariile din fișier. Pe scurt:
- **`WaitForFirstConsumer`:** discul se creează în zona nodului ales pentru pod.
- **`encrypted: "true"`:** encryption at rest.
- **`allowVolumeExpansion`:** un disc plin se mărește fără downtime.
- **`reclaimPolicy: Retain`:** un `kubectl delete` sau `helm uninstall` greșit nu șterge datele.

Prețul `Retain`: discurile rămase după ștergerea clusterului trebuie șterse manual (secțiunea 16). `Retain` nu e backup: în producție se adaugă snapshot-uri programate.

---

## 7. Cluster Autoscaler

**Ce face:** când un pod rămâne `Pending` pentru că nu are loc pe niciun nod, Cluster Autoscaler mărește Auto Scaling Group-ul potrivit, iar AWS pornește un nod nou. Când un nod e subutilizat ~10 minute și pod-urile lui încap în altă parte, îl golește (respectând PodDisruptionBudget-urile) și îl oprește.

### Comenzile:
```bash
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm repo update

helm install cluster-autoscaler autoscaler/cluster-autoscaler --version 9.60.0 -n kube-system \
  --set autoDiscovery.clusterName=bootcamp-cluster \
  --set awsRegion=eu-central-1 \
  --set rbac.serviceAccount.create=false \
  --set rbac.serviceAccount.name=cluster-autoscaler \
  --set nodeSelector.workload=apps

# Verificare
kubectl -n kube-system get deploy cluster-autoscaler-aws-cluster-autoscaler \
  -o jsonpath='{.spec.template.spec.containers[0].image}'; echo     # v1.37.x = versiunea clusterului
kubectl -n kube-system get configmap cluster-autoscaler-status -o yaml   # Health: Healthy, ambele node group-uri
kubectl -n kube-system logs deploy/cluster-autoscaler-aws-cluster-autoscaler --tail=20 | grep -i accessdenied || echo OK
```

### Demonstrație de scale-up (opțional):
```bash
kubectl create deployment scale-test --image=registry.k8s.io/pause:3.10 --replicas=10
kubectl set resources deployment scale-test --requests=cpu=500m
kubectl get nodes -L workload -w        # în 2-3 minute apare un nod app-nodes nou
kubectl delete deployment scale-test    # după ~10 minute nodul în plus dispare
```

### De ce am făcut-o așa:
1. **`rbac.serviceAccount.create=false`:** chart-ul folosește ServiceAccount-ul creat de eksctl, care are adnotarea IRSA către rolul cu politica din secțiunea 2.3. Fără asta, autoscaler-ul nu are credențiale AWS și nu poate scala nimic.
2. **`autoDiscovery.clusterName`:** autoscaler-ul găsește singur node group-urile, după tag-uri. Un node group nou nu cere reconfigurare.
3. **Versiunea autoscaler-ului = versiunea minoră a clusterului (1.37).** Autoscaler-ul simulează scheduler-ul Kubernetes; o versiune diferită ia decizii greșite.
4. **De ce nu ajută la DaemonSet-uri:** un pod de DaemonSet e legat de un anume nod. Un nod nou nu-i face loc pe nodul plin. Pentru asta folosim prioritatea pod-ului (secțiunea 11).

---

## 8. Namespace, RBAC și primul deploy

### 8.1 RBAC pentru Jenkins (`k8s/jenkins-rbac.yaml`)

**RBAC (Role-Based Access Control)** definește ce poate face o identitate în Kubernetes. Un **Role** e o listă de permisiuni într-un namespace; un **RoleBinding** o leagă de un utilizator sau grup.

### Codul:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: cicd-deployer-role
  namespace: telecom-apps
rules:
  # 1. Permisiuni complete (CRUD) DOAR pe resursele standard de aplicație
  - apiGroups: ["", "apps", "autoscaling", "networking.k8s.io", "policy"]
    resources: ["deployments", "pods", "services", "ingresses", "configmaps", "horizontalpodautoscalers", "poddisruptionbudgets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  # 2. Permisiuni Write-Only pe Secrete. Jenkins face doar deploy, nu le citește înapoi.
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: cicd-deployer-binding
  namespace: telecom-apps
subjects:
  - kind: Group
    name: cicd-deployers # Maparea din aws-auth
    apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: Role
  name: cicd-deployer-role
  apiGroup: rbac.authorization.k8s.io
```

### Comenzile:
```bash
kubectl create namespace telecom-apps
kubectl apply -f k8s/jenkins-rbac.yaml       # aplicat de administrator, NU de Jenkins

# Verificare least privilege
kubectl auth can-i create poddisruptionbudgets -n telecom-apps --as jenkins-pipeline --as-group cicd-deployers   # yes
kubectl auth can-i get secrets               -n telecom-apps --as jenkins-pipeline --as-group cicd-deployers   # no
kubectl auth can-i create deployments        -n kube-system  --as jenkins-pipeline --as-group cicd-deployers   # no
```

### De ce am făcut-o așa:
1. **`Role`, nu `ClusterRole`:** Jenkins are drepturi doar în `telecom-apps`. Un pipeline compromis nu poate atinge `kube-system`, monitorizarea sau Kafka.
2. **Secrete write-only:** Jenkins poate crea și actualiza Secret-e (de exemplu la deploy), dar nu le poate **citi**. Un pipeline compromis nu poate exfiltra parolele aplicațiilor.
3. **Nume de resurse corecte:** `horizontalpodautoscalers`, nu `hpa`. Prescurtarea există doar în kubectl; în RBAC nu e recunoscută, iar permisiunea pur și simplu nu se aplică.
4. **`poddisruptionbudgets` în grupul `policy`:** fără el, deploy-ul care include PDB-ul pică cu `forbidden`.
5. **RBAC-ul îl aplică administratorul**, nu Jenkins: o identitate nu-și poate acorda singură drepturi.

### 8.2 Manifestul aplicației (`k8s/pyapp-deployment.yaml`)

### Codul:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pyapp-deployment
  namespace: telecom-apps
  labels:
    app: pyapp
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  selector:
    matchLabels:
      app: pyapp
  template:
    metadata:
      labels:
        app: pyapp
    spec:
      # Aplicația nu apelează API-ul Kubernetes: nu primește token de ServiceAccount.
      automountServiceAccountToken: false
      nodeSelector:
        workload: apps
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        fsGroup: 10001
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: pyapp
          image: 216989107224.dkr.ecr.eu-central-1.amazonaws.com/intervieworion/pyapp:LATEST_TAG_PLACEHOLDER
          imagePullPolicy: Always
          ports:
            - containerPort: 8000
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
            requests:
              cpu: "100m"
              memory: "128Mi"
            limits:
              cpu: "250m"
              memory: "256Mi"
          livenessProbe:
            httpGet:
              path: /metrics
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /metrics
              port: 8000
            periodSeconds: 5

---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: pyapp-pdb
  namespace: telecom-apps
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: pyapp

---
apiVersion: v1
kind: Service
metadata:
  name: pyapp-service
  namespace: telecom-apps
  labels:
    app: pyapp
spec:
  selector:
    app: pyapp
  ports:
    - name: port-svc-pyapp
      protocol: TCP
      port: 8000
      targetPort: 8000
  type: ClusterIP
```

### Comenzile:
```bash
# Commit + push în repository -> Jenkins (Poll SCM) pornește build-ul în max. 2 minute
git add -A && git commit -m "..." && git push origin main

# După build
kubectl get pods,pdb -n telecom-apps -o wide
kubectl get deploy pyapp-deployment -n telecom-apps -o yaml | grep -E "readOnlyRootFilesystem|maxUnavailable"
```

### De ce am făcut-o așa:
1. **`maxUnavailable: 0`, `maxSurge: 1`:** la deploy, Kubernetes pornește întâi pod-ul nou și îl oprește pe cel vechi abia după ce cel nou trece readiness probe-ul. Aplicația are mereu 2 replici funcționale.
2. **PodDisruptionBudget `minAvailable: 1`:** la evacuările voluntare (drain de nod, scale-down al autoscaler-ului, preemption) cel puțin un pod rămâne pornit. PDB-ul **nu** oprește un `kubectl delete pod` explicit.
3. **Liveness vs. readiness probe.** Liveness repornește un proces blocat. Readiness scoate pod-ul din Service până e gata să primească trafic.
4. **`securityContext` complet** (standardul Pod Security „restricted”):
   - **`runAsNonRoot` și UID 10001:** aplicația nu rulează ca root;
   - **`allowPrivilegeEscalation: false`:** procesul nu poate obține mai multe drepturi decât a pornit cu ele;
   - **`capabilities: drop ALL`:** nicio capabilitate specială de kernel;
   - **`readOnlyRootFilesystem`:** un atacator nu poate scrie sau descărca unelte în container;
   - **`seccomp RuntimeDefault`:** sunt blocate apelurile de sistem rar folosite și periculoase.
5. **`requests` și `limits`:** `requests` e ce rezervă scheduler-ul (și ce folosește autoscaler-ul în calcule); `limits` e plafonul. Peste limita de memorie, pod-ul e oprit (OOMKilled), ca să nu-i afecteze pe vecini.
6. **`nodeSelector: workload: apps`:** aplicația nu poate ajunge pe nodurile de date.
7. **`imagePullPolicy: Always` cu tag unic:** fiecare deploy verifică imaginea în ECR. Cu tag-uri imutabile, e garantat același conținut.

---

## 9. Monitorizare: Prometheus, Alertmanager, Grafana, node-exporter

**Componentele stack-ului `kube-prometheus-stack`:**

| Componentă | Ce face |
|---|---|
| **Prometheus** | la fiecare 15 secunde citește (*scrape*) endpoint-urile `/metrics` și stochează seriile de timp; evaluează regulile de alertă |
| **Prometheus Operator** | transformă resursele `ServiceMonitor` și `PrometheusRule` în configurație Prometheus. Nu editezi `prometheus.yml` de mână |
| **node-exporter** (DaemonSet) | expune metricile fiecărui nod: CPU, memorie, disc, rețea. Rulează pe toate nodurile, inclusiv pe cel de date |
| **kube-state-metrics** | expune starea obiectelor Kubernetes: replici disponibile, pod-uri în restart, PVC-uri |
| **Alertmanager** | primește alertele, le grupează, suprimă duplicatele și le trimite pe canalul potrivit |
| **Grafana** | dashboard-uri peste datele din Prometheus |

### 9.1 Rutarea alertelor (`Prometheus/alertmanager.yaml`)

### Codul:
```yaml
# =======================================================================
# Configurația Alertmanager (fără secrete).
# Se încarcă în cluster ca Secret-ul 'alertmanager-config' (cheia alertmanager.yaml),
# referit din Prometheus/values.yaml prin alertmanagerSpec.configSecret.
# Credențialele (parolă SMTP, cheie PagerDuty, webhook Slack) stau în Secret-ul separat
# 'alertmanager-credentials', montat de operator în
# /etc/alertmanager/secrets/alertmanager-credentials/ și citit prin câmpurile *_file.
# Crearea celor două Secrete: vezi README.md, secțiunea Alerting.
# =======================================================================

global:
  resolve_timeout: 5m

  # --- Configurație Globală SMTP ---
  smtp_smarthost: 'smtp.telecom-lab.example:587'
  smtp_from: 'alertmanager@telecom-lab.example'
  smtp_auth_username: 'alertmanager@telecom-lab.example'
  smtp_auth_password_file: '/etc/alertmanager/secrets/alertmanager-credentials/smtp-password'
  smtp_require_tls: true

route:
  # =======================================================================
  # GROUPING (ANTI-SPAM)
  # =======================================================================
  group_by: ['alertname', 'namespace', 'service', 'severity']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

  # Default route (pentru alerte care nu au label-ul 'severity' setat)
  receiver: 'slack-medium'

  routes:
    # 1. CRITICAL -> Sună telefonul! (PagerDuty)
    - matchers:
        - severity="critical"
      receiver: 'pagerduty-critical'

    # 2. HIGH -> Trimite Email
    - matchers:
        - severity="high"
      receiver: 'email-high'

    # 3. MEDIUM / WARNING -> Trimite pe Slack
    - matchers:
        - severity=~"medium|warning"
      receiver: 'slack-medium'

# =======================================================================
# INHIBITION RULES (Regulile de liniște)
# =======================================================================
inhibit_rules:
  # 1. O alertă CRITICAL suprimă automat orice alertă High, Medium sau Warning
  # de pe același serviciu. Când cade baza de date cu CRITICAL, nu mă interesează
  # alerta de WARNING că merge greu query-ul.
  - source_matchers:
      - severity="critical"
    target_matchers:
      - severity=~"high|medium|warning"
    equal: ['namespace', 'job']

  # 2. O alertă HIGH suprimă alertele Medium și Warning
  - source_matchers:
      - severity="high"
    target_matchers:
      - severity=~"medium|warning"
    equal: ['namespace', 'job']

# =======================================================================
# RECEIVERS
# =======================================================================
receivers:
  # --- 1. RECEIVER PAGERDUTY (CRITICAL) ---
  - name: 'pagerduty-critical'
    pagerduty_configs:
      - routing_key_file: '/etc/alertmanager/secrets/alertmanager-credentials/pagerduty-routing-key'
        send_resolved: true
        severity: 'critical'
        description: '[CRITICAL] {{ .CommonAnnotations.summary }}'
        details:
          runbook: '{{ .CommonAnnotations.runbook_url }}'
          description: '{{ .CommonAnnotations.description }}'

  # --- 2. RECEIVER EMAIL (HIGH) ---
  - name: 'email-high'
    email_configs:
      - to: 'oncall-backend@telecom-lab.example'
        send_resolved: true
        html: |
          <h3>Alertă K8s: {{ .CommonAnnotations.summary }}</h3>
          <p><strong>Detalii:</strong> {{ .CommonAnnotations.description }}</p>
          <p><strong>Runbook:</strong> <a href="{{ .CommonAnnotations.runbook_url }}">Cum să rezolvi problema</a></p>

  # --- 3. RECEIVER SLACK (MEDIUM / WARNING) ---
  - name: 'slack-medium'
    slack_configs:
      - api_url_file: '/etc/alertmanager/secrets/alertmanager-credentials/slack-webhook-url'
        channel: '#telecom-alerts'
        send_resolved: true
        title: '[{{ .Status | toUpper }}] {{ .CommonAnnotations.summary }}'
        text: >-
          *Detalii:* {{ .CommonAnnotations.description }}
          *Runbook:* {{ .CommonAnnotations.runbook_url }}
```

### De ce am făcut-o așa:
1. **Rutare pe severitate.** `critical` merge la PagerDuty, care sună de gardă. `high` merge pe email. `warning` merge pe Slack: e vizibil, dar nu trezește pe nimeni. O alertă care trezește omul degeaba îl învață să ignore alertele (*alert fatigue*).
2. **Grupare** (`group_by`, `group_wait`, `repeat_interval`): 50 de pod-uri căzute înseamnă o singură notificare, nu 50.
3. **Inhibiție:** cât timp o alertă `critical` e activă pentru un `job` dintr-un namespace, alertele mai puțin grave pentru același job sunt suprimate. `equal` folosește doar label-uri pe care **ambele** alerte le au: cu `alertname` în listă, inhibiția nu s-ar declanșa niciodată.
4. **Fără credențiale în fișier.** Parola SMTP, cheia PagerDuty și webhook-ul Slack sunt citite din fișiere (`*_file`), montate dintr-un Secret creat separat. Fișierul de configurare poate sta în Git.

### 9.2 Valorile Helm (`Prometheus/values.yaml`)

### Codul:
```yaml
prometheus:
  prometheusSpec:
    # 1. Păstrăm metricele doar 15 zile pe discul scump. Restul s-ar duce în S3 (via Thanos).
    retention: 15d
    nodeSelector:
      workload: apps

    # 2. Resurse stricte (Protejăm nodurile de "Noisy Neighbors")
    resources:
      requests:
        cpu: 500m
        memory: 1Gi
      limits:
        cpu: 1000m
        memory: 2Gi

    storageSpec:
      volumeClaimTemplate:
        spec:
          storageClassName: "gp3-storage"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 10Gi

    # În producție am adăuga:
    # replicas: 2
    # podAntiAffinity: ... (să fie pe AZ-uri diferite)

alertmanager:
  alertmanagerSpec:
    # Configurația vine din Prometheus/alertmanager.yaml (Secret 'alertmanager-config'),
    # nu din values: fișierul de configurare nu conține credențiale.
    configSecret: alertmanager-config
    # Credențialele, montate în /etc/alertmanager/secrets/alertmanager-credentials/
    secrets:
      - alertmanager-credentials
    nodeSelector:
      workload: apps
    # Alertmanager folosește foarte puține resurse, dar îi setăm limite pentru siguranță.
    resources:
      requests:
        cpu: 100m
        memory: 128Mi
      limits:
        cpu: 200m
        memory: 256Mi
    storage:
      volumeClaimTemplate:
        spec:
          storageClassName: "gp3-storage"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 2Gi

grafana:
  nodeSelector:
    workload: apps
  # Protecție pentru Grafana
  resources:
    requests:
      cpu: 250m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi
  persistence:
    enabled: true
    storageClassName: "gp3-storage"
    size: 5Gi

# Operatorul si kube-state-metrics stau tot pe app-nodes.
# node-exporter (DaemonSet) NU primeste nodeSelector: trebuie sa ruleze pe toate nodurile,
# inclusiv pe data-nodes (chart-ul are implicit toleration pentru orice taint NoSchedule).
prometheusOperator:
  nodeSelector:
    workload: apps

kube-state-metrics:
  nodeSelector:
    workload: apps
```

### De ce am făcut-o așa:
1. **Retenție 15 zile pe 10 GiB:** pentru istoricul lung, în producție se folosește Thanos sau Amazon Managed Prometheus, pe S3, nu un disc mare și scump.
2. **Resurse explicite pentru fiecare componentă:** fără limite, un Prometheus care primește brusc multe serii poate consuma toată memoria nodului (*noisy neighbour*).
3. **Toate pe `app-nodes`, mai puțin node-exporter,** care trebuie să ruleze pe toate nodurile.
4. **Alertmanager citește configurația din Secret-ul `alertmanager-config`** (`configSecret`), iar credențialele din `alertmanager-credentials`. Ambele trebuie create înainte de instalare.

### 9.3 Colectarea metricilor aplicației (`Prometheus/pyapp-servicemonitor.yaml`)

### Codul:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: pyapp-servicemonitor
  namespace: telecom-apps  # Același namespace cu aplicația ta
  labels:
    release: monitoring    # CRITIC: Selectorul pentru Prometheus Operator
spec:
  # Definim exact unde căutăm Service-ul (nu lăsăm la ghici)
  namespaceSelector:
    matchNames:
      - telecom-apps
  selector:
    matchLabels:
      app: pyapp     # Trebuie să se potrivească cu label-urile Service-ului tău
  endpoints:
    - port: port-svc-pyapp    # Trebuie să se potrivească EXACT cu NUMELE portului din Service-ul tău, nu cu numărul (8000)
      path: /metrics        # Endpoint-ul unde expune Python metricile
      scheme: http          # Explicităm protocolul (în bănci des folosim https + mTLS aici)
      interval: 15s         # La cât timp vine Prometheus să facă scrape
      scrapeTimeout: 10s    # Dacă aplicația nu dă metricele în 10s, Prometheus închide conexiunea, prevenind blocajele în cascadă. (Trebuie să fie mai mic decât intervalul!)

      # Opțional: drop la metrice inutile pentru a salva RAM în Prometheus
      # metricRelabelings:
      #   - sourceLabels: [__name__]
      #     regex: 'go_.*' # Ignorăm metricele interne Go dacă aplicația era de Go și ne interesează doar metricele de business
      #     action: drop
```

**De ce:** ServiceMonitor e *service discovery* declarativ: „găsește Service-urile cu label-ul `app: pyapp` și citește `/metrics` de pe portul numit `port-svc-pyapp`”. Pod-uri noi, scalări sau IP-uri schimbate sunt preluate automat. Label-ul `release: monitoring` e obligatoriu: Prometheus Operator ignoră resursele fără el, deci release-ul Helm trebuie să se numească exact `monitoring`.

### 9.4 Alerte de disponibilitate (`Prometheus/pyapp-alerts.yaml`)

### Codul:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: pyapp-alerts
  namespace: telecom-apps
  labels:
    # OBLIGATORIU: Fără acest label care corespunde cu Helm-ul (ex: release: monitoring),
    # Prometheus Operator va ignora complet acest fișier (Failure mode: Silențiere totală).
    release: monitoring
spec:
  groups:
    - name: pyapp-availability
      rules:

        # =========================================================================
        # 1. DISPONIBILITATEA SERVICIULUI (CRITICAL) - VERSIUNEA HA
        # =========================================================================
        - alert: PyAppServiceDegraded
          # DE CE AȘA (LOGICĂ DE PRODUCȚIE HA):
          # - absent(...): Acoperă cazul în care podurile sunt șterse complet, scalate la 0
          #   sau serviciul a dispărut din Kubernetes.
          # - sum(up) / count(up) < 0.5: Avem mai puțin de 50% din pod-uri funcționale.
          #   Într-o bancă, un singur pod down nu este critic dacă ai replici. Alertăm critic
          #   doar dacă majoritatea replicilor au căzut și clienții sunt afectați.
          expr: >
            absent(up{job="pyapp-service", namespace="telecom-apps"})
            or
            (
              sum by (job, namespace) (up{job="pyapp-service", namespace="telecom-apps"})
              /
              count by (job, namespace) (up{job="pyapp-service", namespace="telecom-apps"})
            ) < 0.5
          for: 2m
          labels:
            severity: critical
            team: telecom-backend
          annotations:
            summary: "Disponibilitate critică pentru pyapp-service (< 50% pod-uri active)"
            description: "Serviciul a pierdut majoritatea replicilor sau este complet jos. Clienții primesc erori de conexiune."
            # Orice alertă trebuie să aibă instrucțiuni de intervenție.
            runbook_url: "https://wiki.telecom.local/runbooks/pyapp/service-degraded"

        # =========================================================================
        # 1.1 UN SINGUR POD PICAT (WARNING)
        # =========================================================================
        - alert: PyAppSinglePodDown
          # LOGICĂ DE PRODUCȚIE:
          # Vrem totuși să știm dacă un pod izolat e jos.
          # - up(...) == 0: Acoperă cazul în care Podul rulează, dar aplicația face freeze
          #   (ex: deadlock pe baza de date) și Prometheus primește timeout la scrape.
          # Punem severitate "warning" ca să nu trezească omul noaptea. K8s (self-healing) ar trebui să-l restarteze.
          expr: up{job="pyapp-service", namespace="telecom-apps"} == 0
          for: 5m
          labels:
            severity: warning
            team: telecom-backend
          annotations:
            summary: "Pod-ul {{ $labels.instance }} este down"
            description: "Unul dintre pod-urile serviciului nu răspunde de 5 minute. Kubernetes ar trebui să îl restarteze."
            runbook_url: "https://wiki.telecom.local/runbooks/pyapp/pod-troubleshooting"

        # =========================================================================
        # 3. SATURAȚIA MEMORIEI - PREVENȚIE OOM KILLED (WARNING)
        # =========================================================================
        - alert: PyAppHighMemoryUsage
          # DE CE AȘA:
          # - container_memory_working_set_bytes: Este metrica reală folosită de Kubernetes pentru OOM (exclude cache-ul eliberabil).
          # - > 0 la limit_bytes: Dacă un dezvoltator uită să declare 'resources.limits.memory' în Deployment, limita va fi 0.
          #   Fără '> 0', vei avea 'Division by Zero' și PromQL va crăpa în tăcere.
          # - pod!="": la restartul unui pod, cAdvisor raportează pentru scurt timp serii fără label-ul pod
          #   (agregate la nivel de cgroup), care ar produce alerte false. Filtrul le exclude.
          expr: sum by (pod) (container_memory_working_set_bytes{namespace="telecom-apps", container="pyapp", pod!=""}) / sum by (pod) (container_spec_memory_limit_bytes{namespace="telecom-apps", container="pyapp", pod!=""} > 0) * 100 > 85
          for: 5m
          labels:
            severity: warning
            team: telecom-backend
          annotations:
            summary: "Pod-ul {{ $labels.pod }} consumă peste 85% din memorie"
            description: "Posibil memory leak în aplicația Python. Investigați înainte ca procesul să fie terminat forțat (OOMKilled)."
            runbook_url: "https://wiki.telecom.local/runbooks/pyapp/memory-leak-investigation"
```

### 9.5 Alerte de business 5G (`Prometheus/pyapp-business-alerts.yaml`)

### Codul:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: pyapp-business-alerts
  namespace: telecom-apps
  labels:
    release: monitoring
spec:
  groups:
    - name: pyapp.business.rules
      rules:

        # =========================================================================
        # 1. LATENȚĂ MARE PE ECHIPAMENTUL RADIO (gNodeB)
        # =========================================================================
        - alert: HighRANLatency
          expr: max by (gnb_id, slice_id) (network_latency_ms{service="pyapp-service"}) > 150
          for: 3m
          labels:
            severity: critical
            team: telecom-ran
          annotations:
            summary: "Latență critică pe antena {{ $labels.gnb_id }} (Slice: {{ $labels.slice_id }})"
            description: "Latența a depășit 150ms constant timp de 3 minute. Valoarea maximă observată: {{ $value }}ms."
            runbook_url: "https://kibana.telecom.local/app/discover?query=gnb_id:{{ $labels.gnb_id }}"

        # =========================================================================
        # 2. RATA DE LOG-URI "ERROR" CRESCUTĂ (Error Ratio)
        # =========================================================================
        - alert: HighApplicationErrorLogs
          expr: >
            (
              sum by (slice_id) (rate(session_events_total{service="pyapp-service", level="ERROR"}[5m]))
              /
              sum by (slice_id) (rate(session_events_total{service="pyapp-service"}[5m]))
            ) > 0.05
            and sum by (slice_id) (rate(session_events_total{service="pyapp-service"}[5m])) > 1
          for: 5m
          labels:
            severity: warning
            team: telecom-backend
          annotations:
            summary: "Rata logurilor de ERROR a depășit 5% pe slice-ul {{ $labels.slice_id }}"
            description: "Multe erori de procesare relative la volumul de trafic. Verificați logurile în Kibana."
            runbook_url: "https://kibana.telecom.local/app/discover?query=level:ERROR AND slice_id:{{ $labels.slice_id }}"

        # =========================================================================
        # 3. DROP MASIV DE CONEXIUNI UE (User Equipment)
        # =========================================================================
        - alert: MassiveUEDrop
          expr: >
            max by (gnb_id, slice_id) (active_ue_connections{service="pyapp-service"}) < 100
            and max_over_time(max by (gnb_id, slice_id) (active_ue_connections{service="pyapp-service"})[1h:1m]) > 1000
          for: 2m
          labels:
            severity: critical
            team: telecom-ran
          annotations:
            summary: "Deconectare masivă pe {{ $labels.gnb_id }} (Slice: {{ $labels.slice_id }})"
            description: "Numărul de conexiuni a scăzut brusc la {{ $value }}. Antena era activă recent."
            runbook_url: "https://wiki.telecom.local/runbooks/5g/ue-drop"
```

### De ce am făcut-o așa (ambele fișiere):
1. **Alertele pe impact, nu pe cauză.** `PyAppServiceDegraded` e critică doar când majoritatea replicilor au căzut, adică doar atunci când clienții sunt afectați. Un singur pod căzut e `warning`: Kubernetes îl repornește singur.
2. **`for:`:** condiția trebuie să persiste (2–5 minute) înainte să pornească alerta. Elimină alertele produse de un vârf de o secundă.
3. **Protecție la trafic mic:** `and ... > 1` în regulile pe raport. 1 eroare din 1 cerere, la 3 noaptea, înseamnă „100% erori”, dar nu e un incident.
4. **`> 0` la numitor** în regula de memorie: dacă un container nu are limită de memorie, raportul ar fi o împărțire la zero.
5. **Alerte de business:** latență RAN pe antenă, deconectări masive de UE, rata erorilor pe slice. Spun *ce* s-a stricat pentru utilizatori, nu doar „CPU mare”.
6. **`runbook_url` pe fiecare alertă:** omul de gardă primește direct pașii de rezolvare. Linkurile spre Kibana filtrează deja logurile antenei respective.

### Comenzile:
```bash
kubectl create namespace monitoring

# Configurația Alertmanager (fără secrete)
kubectl -n monitoring create secret generic alertmanager-config \
  --from-file=alertmanager.yaml=Prometheus/alertmanager.yaml

# Credențialele: din fișiere ținute în afara repository-ului (nu ajung în istoricul shell-ului)
mkdir -p ~/.secrets && chmod 700 ~/.secrets
# ~/.secrets/smtp-password, ~/.secrets/pagerduty-routing-key, ~/.secrets/slack-webhook-url
# (pentru canalele nefolosite pui o valoare oarecare: configurația se încarcă, doar trimiterea pe acel canal eșuează)
kubectl -n monitoring create secret generic alertmanager-credentials \
  --from-file=smtp-password=$HOME/.secrets/smtp-password \
  --from-file=pagerduty-routing-key=$HOME/.secrets/pagerduty-routing-key \
  --from-file=slack-webhook-url=$HOME/.secrets/slack-webhook-url

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install monitoring prometheus-community/kube-prometheus-stack --version 93.3.0 \
  -n monitoring -f Prometheus/values.yaml

kubectl apply -f Prometheus/pyapp-servicemonitor.yaml \
              -f Prometheus/pyapp-alerts.yaml \
              -f Prometheus/pyapp-business-alerts.yaml
```

### Verificare:
```bash
kubectl get pods -n monitoring -o wide       # node-exporter pe fiecare nod, restul pe app-nodes
kubectl get pvc -n monitoring                # 3 x Bound (Prometheus 10Gi, Alertmanager 2Gi, Grafana 5Gi)

# Prometheus: http://localhost:9090 -> Status / Targets (pyapp UP), Alerts
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090
# query: active_ue_connections  -> label service="pyapp-service", fara exported_service

# Grafana: http://localhost:3000, utilizator admin
kubectl -n monitoring get secret monitoring-grafana -o jsonpath='{.data.admin-password}' | base64 -d; echo
kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80
# Dashboard-uri incluse: Kubernetes / Compute Resources / *, Node Exporter / Nodes
```

**Testul alertelor:** după un `kubectl rollout restart deployment/pyapp-deployment -n telecom-apps`, aplicația intră în incident după 30 de secunde. În 3–5 minute, `HighRANLatency`, `MassiveUEDrop` și `HighApplicationErrorLogs` trec din `Pending` în `Firing` (Prometheus → Alerts).

---

## 10. Kafka (Strimzi, KRaft, mTLS)

### Concepte, pe scurt
- **De ce Kafka în pipeline-ul de loguri:** e un tampon între colectare (Fluent Bit) și indexare (Elasticsearch). Dacă Elasticsearch cade sau e lent, logurile se adună în Kafka pe durata retenției și nu se pierd. Producătorul și consumatorul nu depind unul de disponibilitatea celuilalt (*decuplare*).
- **Broker:** serverul Kafka. **Topic:** categorie de mesaje.
- **Partiție:** o bucată din topic. Ordinea mesajelor e garantată în cadrul unei partiții.
- **Cheie:** mesajele cu aceeași cheie ajung mereu în aceeași partiție.
- **Offset:** poziția unui mesaj în partiție. **Consumer group:** consumatori care își împart partițiile; Kafka ține minte offset-ul fiecărui grup.
- **Retenție:** Kafka șterge mesajele după timp sau dimensiune, nu după ce au fost citite.
- **KRaft:** Kafka își gestionează singur metadatele, fără ZooKeeper.
- **Strimzi:** operator Kubernetes pentru Kafka. Clusterul, topicurile și utilizatorii sunt resurse YAML (`Kafka`, `KafkaTopic`, `KafkaUser`), versionate în Git.

### 10.1 Operatorul (`Kafka/strimzi-values.yaml`)

### Codul:
```yaml
# Strimzi Cluster Operator (chart strimzi/strimzi-kafka-operator), instalat în namespace-ul kafka.

# Operatorul urmărește implicit namespace-ul în care e instalat (kafka).
# logging este adăugat pentru User Operator (KafkaUser-ii clienților stau acolo).
watchNamespaces:
  - logging

# Strimzi 1.3.x: Topic/User Operator într-un alt namespace decât clusterul Kafka este dezactivat
# implicit. Îl activăm explicit: User Operator creează Secret-ele cu certificatele clienților
# direct în logging, deci cheile private nu sunt copiate între namespace-uri.
extraEnvs:
  - name: STRIMZI_ENTITY_OPERATOR_WATCHED_NAMESPACE_ENABLED
    value: "true"

# Operatorul este o componentă de control, nu de date: stă pe app-nodes.
nodeSelector:
  workload: apps

resources:
  requests:
    cpu: 200m
    memory: 384Mi
  limits:
    cpu: 1000m
    memory: 384Mi
```

### 10.2 Clusterul (`Kafka/kafka-cluster.yaml`)

### Codul:
```yaml
# =============================================================================
# Kafka (Strimzi, KRaft) - un singur nod dual-role (controller + broker), lab.
# Rulează pe node group-ul data-nodes (label workload=data, taint dedicated=data).
# Acces doar prin listener TLS cu autentificare mTLS și autorizare ACL (simple).
# =============================================================================
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: kafka
  labels:
    strimzi.io/cluster: orion-kafka
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 15Gi
        class: gp3-storage    # StorageClass criptat (EBS + KMS), reclaimPolicy Retain
        deleteClaim: false    # PVC-ul rămâne dacă resursa Kafka este ștearsă
        kraftMetadata: shared # metadatele KRaft stau pe același volum
  resources:
    requests:
      cpu: 250m
      memory: 1Gi
    limits:
      cpu: 1000m
      memory: 1536Mi
  jvmOptions:
    # Heap-ul rămâne sub limita pod-ului: restul memoriei este page cache pentru log segments
    # (Kafka citește/scrie prin page cache, zero-copy). Heap = limită => performanță slabă și OOMKilled.
    -Xms: 512m
    -Xmx: 768m
  template:
    pod:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values: ["data"]
      tolerations:
        - key: dedicated
          operator: Equal
          value: data
          effect: NoSchedule
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: orion-kafka
  namespace: kafka
spec:
  kafka:
    version: 4.3.2
    metadataVersion: "4.3"
    listeners:
      # Singurul listener pentru clienți: TLS + certificat de client (mTLS).
      # Nu există listener plaintext: traficul de loguri este criptat in transit.
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: tls
        # Strimzi generează NetworkPolicy pentru listener: doar namespace-ul logging
        # (Fluent Bit, Logstash) are voie pe 9093. Aplicată când VPC CNI are network policy activat.
        networkPolicyPeers:
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: logging
    authorization:
      type: simple   # ACL-uri pe utilizator (definite în KafkaUser)
    config:
      # Single-node lab. Producție (3 brokeri pe AZ-uri diferite): replication factor 3, min.insync.replicas 2.
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
      # Topicurile se creează doar declarativ (KafkaTopic în Git), niciodată implicit de un client.
      auto.create.topics.enable: "false"
  entityOperator:
    # Topic Operator: sincronizează KafkaTopic (ns kafka) cu clusterul.
    topicOperator:
      resources:
        requests:
          cpu: 50m
          memory: 256Mi
        limits:
          cpu: 300m
          memory: 384Mi
    # User Operator: urmărește KafkaUser din namespace-ul clienților (logging) și creează acolo
    # Secret-ele cu certificatele de client, deci certificatele nu trebuie copiate între namespace-uri.
    userOperator:
      watchedNamespace: logging
      resources:
        requests:
          cpu: 50m
          memory: 256Mi
        limits:
          cpu: 300m
          memory: 384Mi
    template:
      pod:
        affinity:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
                - matchExpressions:
                    - key: workload
                      operator: In
                      values: ["data"]
        tolerations:
          - key: dedicated
            operator: Equal
            value: data
            effect: NoSchedule
```

### 10.3 Topicurile (`Kafka/kafka-topic.yaml`)

### Codul:
```yaml
# =============================================================================
# Topicurile pentru logurile pyapp (5g-session-analyzer).
# Fluent Bit scrie evenimentele normale în 5g-network-logs și pe cele cu level=ERROR
# în 5g-network-errors. Cheia mesajului este gnb_id: toate evenimentele unei antene
# ajung în aceeași partiție, deci ordinea lor se păstrează.
#
# Dimensionare disc (volum Kafka 15 GiB):
#   5g-network-logs   3 partiții x 2 GiB   = 6 GiB
#   5g-network-errors 3 partiții x 512 MiB = 1.5 GiB
#   rest: segmente active, metadate KRaft, __consumer_offsets
# =============================================================================
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: 5g-network-logs
  namespace: kafka
  labels:
    strimzi.io/cluster: orion-kafka
spec:
  # 3 partiții = până la 3 consumatori paraleli în același consumer group.
  partitions: 3
  replicas: 1                     # lab, 1 broker (producție: 3)
  config:
    cleanup.policy: delete
    retention.ms: 21600000        # 6h: buffer între colectare și indexare, nu arhivă
    retention.bytes: 2147483648   # 2 GiB / partiție
    segment.ms: 3600000           # segmentul activ se închide la 1h: retenția se aplică și la trafic mic
    segment.bytes: 268435456      # 256 MiB
    compression.type: lz4         # loguri JSON redundante, cost CPU mic
    min.insync.replicas: 1
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: 5g-network-errors
  namespace: kafka
  labels:
    strimzi.io/cluster: orion-kafka
spec:
  partitions: 3
  replicas: 1
  config:
    cleanup.policy: delete
    retention.ms: 86400000        # 24h: erorile se păstrează mai mult pentru investigarea incidentelor
    retention.bytes: 536870912    # 512 MiB / partiție
    segment.ms: 3600000
    segment.bytes: 67108864       # 64 MiB: volum mic, segmente mici
    compression.type: lz4
    min.insync.replicas: 1
```

### 10.4 Utilizatorii (`Kafka/kafka-users.yaml`)

### Codul:
```yaml
# =============================================================================
# Identitățile clienților Kafka (mTLS + ACL + quotas).
# Stau în namespace-ul logging, urmărit de User Operator (userOperator.watchedNamespace):
# Secret-ul cu certificatul fiecărui client este creat în același namespace cu clientul.
# Secret-ul are numele KafkaUser-ului și conține user.crt, user.key, user.p12, user.password
# și ca.crt (CA-ul de clienți). Pentru a verifica brokerii, clienții au nevoie și de CA-ul
# clusterului (certificat public), sincronizat în logging de Kafka/sync-cluster-ca.sh.
# =============================================================================

# Producătorul: Fluent Bit. Poate doar să scrie în topicurile 5g-network-*.
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: fluent-bit-producer
  namespace: logging
  labels:
    strimzi.io/cluster: orion-kafka
spec:
  authentication:
    type: tls
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: 5g-network-
          patternType: prefix
        operations:
          - Write
          - Describe
  quotas:
    # Un producător scăpat de sub control (log storm) nu poate satura brokerul.
    producerByteRate: 1048576     # 1 MiB/s
---
# Consumatorul: Logstash (faza de indexare în Elasticsearch). Poate doar să citească,
# și doar în consumer group-ul lui.
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: logstash-consumer
  namespace: logging
  labels:
    strimzi.io/cluster: orion-kafka
spec:
  authentication:
    type: tls
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: 5g-network-
          patternType: prefix
        operations:
          - Read
          - Describe
      - resource:
          type: group
          name: logstash-es
          patternType: literal
        operations:
          - Read
  quotas:
    consumerByteRate: 2097152     # 2 MiB/s
```

### 10.5 CA-ul clusterului în namespace-ul clienților (`Kafka/sync-cluster-ca.sh`)

### Codul:
```bash
#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Copiază CA-ul public al clusterului Kafka (Strimzi) din namespace-ul kafka în
# namespace-ul clienților (logging), ca Fluent Bit / Logstash să poată verifica
# certificatul brokerilor.
#
# Copiază doar certificatul public (ca.crt), nu și cheia privată a CA-ului.
# Idempotent: poate fi rulat oricând (create --dry-run | apply).
#
# Strimzi reînnoiește CA-ul clusterului înainte de expirare (implicit la 365 zile).
# După reînnoire scriptul trebuie rulat din nou și clienții reporniți (--restart).
# În producție această copiere o face un operator (cert-manager trust-manager sau
# External Secrets), nu un script.
#
# Utilizare: Kafka/sync-cluster-ca.sh [--restart]
# -----------------------------------------------------------------------------
set -euo pipefail

CLUSTER="orion-kafka"
SRC_NS="kafka"
DST_NS="logging"
DST_SECRET="kafka-cluster-ca"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

kubectl get secret "${CLUSTER}-cluster-ca-cert" -n "$SRC_NS" \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > "$tmp"

if [[ ! -s "$tmp" ]]; then
  echo "CA-ul clusterului nu a fost găsit (${SRC_NS}/${CLUSTER}-cluster-ca-cert). Kafka este Ready?" >&2
  exit 1
fi

kubectl create secret generic "$DST_SECRET" -n "$DST_NS" \
  --from-file=ca.crt="$tmp" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret ${DST_NS}/${DST_SECRET} actualizat."

if [[ "${1:-}" == "--restart" ]]; then
  kubectl rollout restart daemonset/fluent-bit -n "$DST_NS"
fi
```

### Comenzile:
```bash
kubectl create namespace kafka
kubectl create namespace logging

helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi-cluster-operator strimzi/strimzi-kafka-operator \
  --version 1.3.1 -n kafka -f Kafka/strimzi-values.yaml
kubectl get crd | grep strimzi            # kafkas, kafkanodepools, kafkatopics, kafkausers...

# data-nodes poate fi la 0 noduri: Cluster Autoscaler îl pornește (~2-3 min)
kubectl apply -f Kafka/kafka-cluster.yaml
kubectl wait kafka/orion-kafka --for=condition=Ready --timeout=600s -n kafka

kubectl apply -f Kafka/kafka-topic.yaml
kubectl apply -f Kafka/kafka-users.yaml
kubectl wait kafkauser --all --for=condition=Ready --timeout=120s -n logging

bash Kafka/sync-cluster-ca.sh
```

### Verificare:
```bash
kubectl get kafka,kafkanodepool,kafkatopic -n kafka         # READY True
kubectl get pods -n kafka -o wide                           # broker + entity-operator (2/2) pe nodul workload=data
kubectl get pvc -n kafka                                    # 15Gi Bound
kubectl get kafkauser -n logging                            # tls / simple / True
kubectl describe secret fluent-bit-producer -n logging      # chei: ca.crt, user.crt, user.key, user.p12, user.password
kubectl get secret fluent-bit-producer -n logging -o jsonpath='{.data.user\.crt}' \
  | base64 -d | openssl x509 -noout -subject -issuer -enddate    # CN=fluent-bit-producer, issuer clients-ca
kubectl get secret kafka-cluster-ca -n logging -o jsonpath='{.data.ca\.crt}' \
  | base64 -d | openssl x509 -noout -subject                     # cluster-ca
```

### De ce am făcut-o așa:
1. **Doar listener TLS, cu certificat de client (mTLS).** Traficul e criptat (encryption in transit, PCI DSS Req. 4). Fiecare client are identitatea lui, revocabilă individual, fără parole partajate.
2. **Două autorități de certificare.** `cluster-ca` semnează certificatul brokerului; clienții îl folosesc ca să verifice că vorbesc cu Kafka-ul real, nu cu un impostor (*man-in-the-middle*). `clients-ca` semnează certificatele clienților; brokerul îl folosește ca să-i verifice. De aceea Fluent Bit are nevoie de **ambele**: certificatul propriu (din `KafkaUser`) și `cluster-ca` (copiat de script).
3. **ACL-uri (`authorization: simple`):**
   - Fluent Bit poate doar scrie (`Write`) în `5g-network-*`.
   - Logstash poate doar citi (`Read`), și doar în grupul `logstash-es`.
   - Consumatorul are nevoie de `Read` și pe **grup**: acolo își salvează offset-urile. Cu `literal`, nu poate intra în grupul altei aplicații și nu le poate muta offset-urile.
4. **Cote:** 1 MiB/s pentru producător, 2 MiB/s pentru consumator. Un *log storm* nu poate satura brokerul pentru ceilalți clienți.
5. **`networkPolicyPeers`:** Strimzi generează un NetworkPolicy care permite pe portul 9093 doar namespace-ul `logging`. Se aplică după activarea network policy în VPC CNI. Ai trei straturi de protecție: rețea, identitate, permisiuni (*defense in depth*).
6. **`userOperator.watchedNamespace: logging` + `STRIMZI_ENTITY_OPERATOR_WATCHED_NAMESPACE_ENABLED=true`:** Secret-ele cu cheile private ale clienților sunt create direct lângă clienți și nu se copiază între namespace-uri. Se copiază doar `cluster-ca`, un certificat **public**. În Strimzi 1.3.x funcția e dezactivată implicit: fără variabila de mediu, Entity Operator nu pornește și Kafka nu ajunge `Ready`.
7. **Heap JVM 768m dintr-o limită de 1,5 GiB.** Kafka citește și scrie prin page cache-ul Linux și trimite datele direct din cache pe rețea (*zero-copy*). Dacă tot RAM-ul merge la heap, Kafka e lent și pod-ul riscă OOMKilled.
8. **Topicuri doar declarative** (`auto.create.topics.enable: false`): o greșeală de tastare nu creează un topic nou, fără retenție, care umple discul.
9. **Două topicuri:**
   - `5g-network-logs`: evenimente normale, 6 ore;
   - `5g-network-errors`: doar `ERROR`, 24 de ore.
   
   Erorile rămân mai mult pentru investigații și pot avea un consumator separat. Cele 3 partiții permit până la 3 consumatori în paralel.
10. **`segment.ms: 1h`:** segmentul activ nu se șterge niciodată; la trafic mic, fără rotație pe timp, retenția de 6 ore nu s-ar aplica.
11. **Spațiul pe disc e calculat:** 3 × 2 GiB + 3 × 512 MiB = 7,5 GiB din 15 GiB. Discul nu se poate umple din cauza topicurilor.
12. **Versiuni fixate** (`--version 1.3.1`): aceeași comandă instalează același lucru și peste o lună.

---

## 11. Fluent Bit: colectarea logurilor (`FluentBit/values.yaml`)

**Ce face:** pyapp scrie pe stdout. `containerd` salvează fiecare linie în `/var/log/containers/<pod>_<namespace>_<container>-<id>.log` pe nod, în format CRI. Fluent Bit rulează ca **DaemonSet** (un pod pe fiecare nod), citește aceste fișiere și le trimite în Kafka. Pipeline-ul are trei etape:

`INPUT tail → FILTER kubernetes / modify / rewrite_tag → OUTPUT kafka (2 topicuri)`

### Codul:
```yaml
# =============================================================================
# Fluent Bit (chart fluent/fluent-bit) - DaemonSet în namespace-ul logging.
# Citește logurile JSON ale pyapp de pe nod, adaugă metadate Kubernetes și le trimite
# în Kafka prin TLS cu certificat de client (KafkaUser fluent-bit-producer).
#   level=ERROR      -> topic 5g-network-errors
#   restul           -> topic 5g-network-logs
# =============================================================================
kind: DaemonSet

# Agent de nod: nu are voie să rămână Pending pe un nod plin (limită de pod-uri sau resurse).
# Cu prioritatea maximă, scheduler-ul evacuează un pod cu prioritate mică, care este
# reprogramat pe alt nod. Aceeași clasă ca aws-node și kube-proxy.
priorityClassName: system-node-critical

# Rulează pe toate nodurile, inclusiv pe data-nodes (taint dedicated=data).
tolerations:
  - operator: Exists
    effect: NoSchedule

resources:
  requests:
    cpu: 20m
    memory: 32Mi
  limits:
    cpu: 200m
    memory: 128Mi

# Root este necesar doar pentru citirea /var/log/pods (fișiere root, 0640).
# Fără capabilități, fără escaladare, root filesystem read-only.
securityContext:
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]

testFramework:
  enabled: false

extraVolumes:
  # Stare pe nod: poziția în fișiere (DB) și buffer-ul pe disc când Kafka nu e disponibil.
  - name: state
    hostPath:
      path: /var/fluent-bit/state
      type: DirectoryOrCreate
  # CA-ul clusterului Kafka (certificat public), sincronizat de Kafka/sync-cluster-ca.sh.
  - name: kafka-cluster-ca
    secret:
      secretName: kafka-cluster-ca
  # Certificatul de client, creat de Strimzi User Operator din KafkaUser fluent-bit-producer.
  - name: kafka-user
    secret:
      secretName: fluent-bit-producer

extraVolumeMounts:
  - name: state
    mountPath: /var/fluent-bit/state
  - name: kafka-cluster-ca
    mountPath: /fluent-bit/kafka/ca
    readOnly: true
  - name: kafka-user
    mountPath: /fluent-bit/kafka/user
    readOnly: true

config:
  service: |
    [SERVICE]
        Daemon                    Off
        Flush                     1
        Log_Level                 info
        Parsers_File              /fluent-bit/etc/parsers.conf
        Parsers_File              /fluent-bit/etc/conf/custom_parsers.conf
        HTTP_Server               On
        HTTP_Listen               0.0.0.0
        HTTP_Port                 2020
        Health_Check              On
        storage.path              /var/fluent-bit/state/flb-storage/
        storage.sync              normal
        storage.checksum          off
        storage.backlog.mem_limit 16M

  inputs: |
    # Doar containerul pyapp din telecom-apps: <pod>_<namespace>_<container>-<id>.log
    [INPUT]
        Name              tail
        Tag               kube.*
        Path              /var/log/containers/*_telecom-apps_pyapp-*.log
        multiline.parser  cri, docker
        DB                /var/fluent-bit/state/flb_kube.db
        Mem_Buf_Limit     16MB
        storage.type      filesystem
        Skip_Long_Lines   On
        Refresh_Interval  10

  filters: |
    # Metadate Kubernetes (pod, namespace, labels). Merge_Log parsează JSON-ul aplicației
    # și pune câmpurile (level, slice_id, gnb_id, latency_ms...) la rădăcina înregistrării.
    [FILTER]
        Name                kubernetes
        Match               kube.*
        Kube_Tag_Prefix     kube.var.log.containers.
        Merge_Log           On
        Keep_Log            Off
        K8S-Logging.Parser  On
        K8S-Logging.Exclude On
        Labels              On
        Annotations         Off

    # Câmpuri tehnice ale formatului CRI, fără valoare în analiză.
    [FILTER]
        Name    modify
        Match   kube.*
        Remove  stream
        Remove  _p

    # Evenimentele ERROR primesc tag-ul errors.* și merg în topicul de erori (keep=false: nu se duplică).
    [FILTER]
        Name                   rewrite_tag
        Match                  kube.*
        Rule                   $level ^ERROR$ errors.$TAG false
        Emitter_Name           errors_emitter
        Emitter_Storage.type   filesystem
        Emitter_Mem_Buf_Limit  8M

  outputs: |
    [OUTPUT]
        Name                             kafka
        Match                            kube.*
        Brokers                          orion-kafka-kafka-bootstrap.kafka.svc:9093
        Topics                           5g-network-logs
        Message_Key_Field                gnb_id
        Format                           json
        Timestamp_Key                    @timestamp
        Timestamp_Format                 iso8601
        Retry_Limit                      no_limits
        storage.total_limit_size         256M
        rdkafka.security.protocol        ssl
        rdkafka.ssl.ca.location          /fluent-bit/kafka/ca/ca.crt
        rdkafka.ssl.certificate.location /fluent-bit/kafka/user/user.crt
        rdkafka.ssl.key.location         /fluent-bit/kafka/user/user.key
        rdkafka.request.required.acks    -1
        rdkafka.compression.codec        lz4
        rdkafka.queue.buffering.max.ms   500
        rdkafka.message.timeout.ms       60000

    [OUTPUT]
        Name                             kafka
        Match                            errors.*
        Brokers                          orion-kafka-kafka-bootstrap.kafka.svc:9093
        Topics                           5g-network-errors
        Message_Key_Field                gnb_id
        Format                           json
        Timestamp_Key                    @timestamp
        Timestamp_Format                 iso8601
        Retry_Limit                      no_limits
        storage.total_limit_size         64M
        rdkafka.security.protocol        ssl
        rdkafka.ssl.ca.location          /fluent-bit/kafka/ca/ca.crt
        rdkafka.ssl.certificate.location /fluent-bit/kafka/user/user.crt
        rdkafka.ssl.key.location         /fluent-bit/kafka/user/user.key
        rdkafka.request.required.acks    -1
        rdkafka.compression.codec        lz4
        rdkafka.queue.buffering.max.ms   500
        rdkafka.message.timeout.ms       60000
```

### Comenzile:
```bash
helm repo add fluent https://fluent.github.io/helm-charts
helm repo update
helm install fluent-bit fluent/fluent-bit --version 0.58.3 \
  -n logging -f FluentBit/values.yaml

kubectl get pods -n logging -o wide        # un pod fluent-bit pe fiecare nod, 1/1 Running
kubectl logs -n logging -l app.kubernetes.io/name=fluent-bit --prefix --tail=20
# cauți: inotify_fs_add(): ... pyapp-deployment-...  și  [output:kafka:kafka.0] brokers=...:9093
# fără: SSL handshake failed / Topic authorization failed / Broker transport failure
```

### De ce am făcut-o așa:
1. **Doar logurile pyapp** (`Path *_telecom-apps_pyapp-*.log`). Colectarea întregului cluster umple Kafka și Elasticsearch cu loguri de sistem pe care nu le citește nimeni. Colectezi intenționat ce ai nevoie.
2. **`DB`:** Fluent Bit ține minte poziția în fiecare fișier. La restart nu retrimite logurile vechi și nu sare peste cele noi.
3. **Filtrul `kubernetes` cu `Merge_Log On`:**
   - adaugă metadatele: pod, namespace, nod, label-uri;
   - desface JSON-ul aplicației la rădăcina înregistrării. Câmpurile se numesc la fel ca în Prometheus (`level`, `gnb_id`, `slice_id`).
4. **`rewrite_tag`:** evenimentele `level=ERROR` primesc alt tag și merg **doar** în topicul de erori (`keep false`, fără duplicare).
5. **`Message_Key_Field gnb_id`:** ordinea evenimentelor e garantată per antenă.
6. **Livrare fără pierderi:**
   - **`acks=-1`:** un mesaj e considerat trimis abia când brokerul confirmă scrierea pe disc;
   - **buffer pe disc** (`storage.type filesystem`, max. 256 MiB): dacă Kafka nu răspunde, logurile se acumulează pe nod și sunt retrimise când revine (*backpressure*);
   - **`Retry_Limit no_limits`:** Fluent Bit reîncearcă până reușește, nu renunță după câteva încercări.
7. **`priorityClassName: system-node-critical`.** Un agent de nod nu are voie să rămână fără loc. Pe un nod plin (am atins limita de 17 pod-uri pe t3.medium), scheduler-ul a evacuat un pod cu prioritate mică (Alertmanager), care a repornit pe alt nod, iar Fluent Bit a primit locul (*preemption*). Fără prioritate, logurile nodului cu aplicația nu ar fi fost colectate.
8. **Securitate:**
   - **root:** e necesar doar pentru citirea fișierelor de log ale nodului (aparțin lui root);
   - **în rest, restricții:** `drop ALL`, fără escaladare, sistem de fișiere read-only.
9. **Toleration pentru orice taint `NoSchedule`:** rulează și pe nodul de date.
10. **De ce Fluent Bit și nu Fluentd:** e scris în C, folosește ~30 MiB per nod, față de sute de MiB pentru Fluentd (Ruby).

---

## 12. Test cap-coadă: pyapp → Fluent Bit → Kafka (`Kafka/kcat-debug.yaml`)

### Codul:
```yaml
# Client de test temporar (kcat) cu identitatea logstash-consumer.
# Folosit pentru a verifica fluxul pyapp -> Fluent Bit -> Kafka și ACL-urile.
# Se șterge după test: kubectl delete -f Kafka/kcat-debug.yaml
apiVersion: v1
kind: Pod
metadata:
  name: kafka-debug-client
  namespace: logging
  labels:
    app: kafka-debug-client
spec:
  nodeSelector:
    workload: apps
  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: kcat
      image: edenhill/kcat:1.7.1
      command: ["sleep", "3600"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
      resources:
        requests:
          cpu: 10m
          memory: 16Mi
        limits:
          cpu: 200m
          memory: 64Mi
      volumeMounts:
        - name: kafka-cluster-ca
          mountPath: /certs/ca
          readOnly: true
        - name: kafka-user
          mountPath: /certs/user
          readOnly: true
  volumes:
    - name: kafka-cluster-ca
      secret:
        secretName: kafka-cluster-ca
    - name: kafka-user
      secret:
        secretName: logstash-consumer
```

### Comenzile:
```bash
kubectl apply -f Kafka/kcat-debug.yaml
kubectl wait pod/kafka-debug-client -n logging --for=condition=Ready --timeout=120s

KCAT="kcat -b orion-kafka-kafka-bootstrap.kafka.svc:9093 -X security.protocol=ssl \
  -X ssl.ca.location=/certs/ca/ca.crt \
  -X ssl.certificate.location=/certs/user/user.crt \
  -X ssl.key.location=/certs/user/user.key"

kubectl exec -n logging kafka-debug-client -- sh -c "$KCAT -L"     # 1 broker, 2 topicuri x 3 partiții
kubectl exec -n logging kafka-debug-client -- sh -c \
  "$KCAT -C -t 5g-network-errors -o -2 -e -f 'partitia=%p cheia=%k\n%s\n\n'"

# Rutarea INFO: un pod nou emite 30 de secunde de trafic normal
kubectl rollout restart deployment/pyapp-deployment -n telecom-apps
sleep 40
kubectl exec -n logging kafka-debug-client -- sh -c \
  "$KCAT -C -t 5g-network-logs -o -2 -e -f 'partitia=%p cheia=%k\n%s\n\n'"

# ACL-uri: ambele TREBUIE să eșueze
kubectl exec -n logging kafka-debug-client -- sh -c "echo test | $KCAT -P -t 5g-network-logs"   # Topic authorization failed
kubectl exec -n logging kafka-debug-client -- sh -c "$KCAT -G alt-grup 5g-network-errors -e"    # Group authorization failed

kubectl delete -f Kafka/kcat-debug.yaml     # nu lași unelte de debug în cluster
```

### Ce dovedește fiecare rezultat:
1. **Mesaje cu `level`, `gnb_id`, `latency_ms` la rădăcină, plus blocul `kubernetes`:** colectarea și filtrele funcționează.
2. **`cheia=gNodeB-101` și toate mesajele în aceeași partiție:** partiționarea după cheie funcționează. Cu trafic real, sute de antene s-ar distribui pe toate partițiile. O antenă mult mai activă decât celelalte face din partiția ei o *hot partition*: un singur consumator primește tot lucrul.
3. **INFO doar în `5g-network-logs`, ERROR doar în `5g-network-errors`:** rutarea funcționează.
4. **Cele două erori de autorizare:** un certificat furat de la consumator nu poate scrie date false și nu poate interfera cu alte grupuri.

---

## 13. Elasticsearch, Logstash, Kibana (ECK)

### Concepte, pe scurt
- **Elasticsearch:** motor de căutare. Construiește un **index inversat**: pentru fiecare valoare (de exemplu `gNodeB-101`), lista documentelor în care apare. De aceea căutarea în milioane de loguri durează milisecunde.
- **Data stream și ILM:** un data stream e o serie de indexuri (unul pe zi), văzute ca unul singur. ILM (Index Lifecycle Management) creează indexul nou zilnic și îl șterge pe cel mai vechi de 7 zile.
- **Logstash:** consumatorul Kafka. Citește, transformă și scrie în Elasticsearch.
- **Kibana:** interfața web pentru căutare și grafice.
- **ECK (Elastic Cloud on Kubernetes):** operatorul Elastic. Generează certificatele TLS și parola utilizatorului `elastic`, conectează Kibana la Elasticsearch și face upgrade-uri ordonate.

### 13.1 Operatorul (`Elastic/eck-values.yaml`)

### Codul:
```yaml
# ECK operator (chart elastic/eck-operator), instalat în namespace-ul elastic-system.

# Operatorul gestionează doar namespace-ul logging, nu tot clusterul (least privilege).
managedNamespaces:
  - logging

# Componentă de control, nu de date: stă pe app-nodes.
nodeSelector:
  workload: apps

resources:
  requests:
    cpu: 100m
    memory: 150Mi
  limits:
    cpu: 500m
    memory: 512Mi
```

### 13.2 Elasticsearch și Kibana (`Elastic/elasticsearch.yaml`)

### Codul:
```yaml
# =============================================================================
# Elasticsearch + Kibana (ECK), un nod, pe data-nodes.
# ECK activează implicit TLS pe HTTP (CA generat de operator) și autentificarea:
# encryption in transit și acces doar cu utilizator, fără configurare suplimentară.
# =============================================================================
apiVersion: elasticsearch.k8s.elastic.co/v1
kind: Elasticsearch
metadata:
  name: logs
  namespace: logging
spec:
  version: 9.5.6
  nodeSets:
    - name: default
      count: 1   # lab; producție: 3 noduri master-eligible pe AZ-uri diferite
      config:
        # Fără mmap, nu e nevoie de vm.max_map_count=262144 pe nod (care cere un init container privilegiat).
        # Producție: mmap activ, sysctl setat pe nod (launch template / DaemonSet).
        node.store.allow_mmap: false
      podTemplate:
        spec:
          affinity:
            nodeAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                nodeSelectorTerms:
                  - matchExpressions:
                      - key: workload
                        operator: In
                        values: ["data"]
          tolerations:
            - key: dedicated
              operator: Equal
              value: data
              effect: NoSchedule
          containers:
            - name: elasticsearch
              env:
                # Heap = ~50% din memorie; restul pentru page cache (Lucene citește segmentele prin OS).
                - name: ES_JAVA_OPTS
                  value: "-Xms768m -Xmx768m"
              resources:
                requests:
                  cpu: 250m
                  memory: 1536Mi
                limits:
                  cpu: 1000m
                  memory: 1536Mi
      volumeClaimTemplates:
        - metadata:
            name: elasticsearch-data   # numele cerut de ECK pentru volumul de date
          spec:
            accessModes: ["ReadWriteOnce"]
            storageClassName: gp3-storage
            resources:
              requests:
                storage: 10Gi
---
apiVersion: kibana.k8s.elastic.co/v1
kind: Kibana
metadata:
  name: logs
  namespace: logging
spec:
  version: 9.5.6
  count: 1
  elasticsearchRef:
    name: logs        # ECK conectează Kibana la Elasticsearch (utilizator de serviciu + CA)
  podTemplate:
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values: ["data"]
      tolerations:
        - key: dedicated
          operator: Equal
          value: data
          effect: NoSchedule
      containers:
        - name: kibana
          env:
            - name: NODE_OPTIONS
              value: "--max-old-space-size=700"
          resources:
            requests:
              cpu: 100m
              memory: 768Mi
            limits:
              cpu: 1000m
              memory: 1Gi
```

### 13.3 Inițializarea: retenție, mapping, utilizator dedicat (`Elastic/es-bootstrap.yaml`)

### Codul:
```yaml
# =============================================================================
# Inițializare Elasticsearch (Job idempotent: toate apelurile sunt PUT).
#   - ILM policy 5g-network: rollover zilnic, ștergere după 7 zile
#   - index template 5g-network*: data stream, 0 replici (single node), mapping explicit
#   - rol + utilizator logstash_writer: poate doar să scrie în 5g-network* (least privilege)
# Parola utilizatorului vine din Secret-ul logstash-es-writer (generat, nu e în Git).
# Re-rulare: kubectl delete job es-bootstrap -n logging && kubectl apply -f Elastic/es-bootstrap.yaml
# =============================================================================
apiVersion: batch/v1
kind: Job
metadata:
  name: es-bootstrap
  namespace: logging
spec:
  backoffLimit: 6
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: OnFailure
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile:
          type: RuntimeDefault
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values: ["data"]
      tolerations:
        - key: dedicated
          operator: Equal
          value: data
          effect: NoSchedule
      containers:
        - name: bootstrap
          # Imaginea Elasticsearch: conține curl, rulează non-root, aceeași versiune ca clusterul
          # și e deja descărcată pe nod. Fără o imagine suplimentară de întreținut și scanat.
          image: docker.elastic.co/elasticsearch/elasticsearch:9.5.6
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 100m
              memory: 64Mi
          env:
            - name: ELASTIC_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: logs-es-elastic-user
                  key: elastic
            - name: LOGSTASH_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: logstash-es-writer
                  key: password
          volumeMounts:
            - name: es-ca
              mountPath: /certs/es
              readOnly: true
          command: ["/bin/sh", "-ec"]
          args:
            - |
              ES=https://logs-es-http.logging.svc:9200
              es() { curl -sS --fail-with-body --cacert /certs/es/ca.crt -u "elastic:${ELASTIC_PASSWORD}" \
                       -H 'Content-Type: application/json' "$@"; echo; }

              until es "$ES/_cluster/health?wait_for_status=yellow&timeout=30s" >/dev/null; do
                echo "astept Elasticsearch..."; sleep 5
              done

              echo "ILM policy"
              es -X PUT "$ES/_ilm/policy/5g-network" -d '{
                "policy": { "phases": {
                  "hot":    { "actions": { "rollover": { "max_age": "1d", "max_primary_shard_size": "5gb" } } },
                  "delete": { "min_age": "7d", "actions": { "delete": {} } }
                } } }'

              echo "Index template"
              es -X PUT "$ES/_index_template/5g-network" -d '{
                "index_patterns": ["5g-network*"],
                "data_stream": {},
                "priority": 500,
                "template": {
                  "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "index.lifecycle.name": "5g-network"
                  },
                  "mappings": {
                    "properties": {
                      "@timestamp":            { "type": "date" },
                      "level":                 { "type": "keyword" },
                      "service":               { "type": "keyword" },
                      "gnb_id":                { "type": "keyword" },
                      "slice_id":              { "type": "keyword" },
                      "latency_ms":            { "type": "float" },
                      "active_ue_connections": { "type": "integer" },
                      "message":               { "type": "text" },
                      "kafka":      { "properties": { "topic": { "type": "keyword" } } },
                      "kubernetes": { "properties": {
                        "namespace_name": { "type": "keyword" },
                        "pod_name":       { "type": "keyword" },
                        "host":           { "type": "keyword" },
                        "labels":         { "type": "flattened" }
                      } }
                    }
                  }
                } }'

              echo "Rol logstash_writer"
              es -X PUT "$ES/_security/role/logstash_writer" -d '{
                "cluster": ["monitor"],
                "indices": [ { "names": ["5g-network*"],
                               "privileges": ["create_doc", "create_index", "auto_configure", "view_index_metadata"] } ]
              }'

              echo "Utilizator logstash_writer"
              es -X PUT "$ES/_security/user/logstash_writer" \
                 -d "{\"password\":\"${LOGSTASH_PASSWORD}\",\"roles\":[\"logstash_writer\"]}"

              echo "Bootstrap complet."
      volumes:
        - name: es-ca
          secret:
            secretName: logs-es-http-certs-public
```

### 13.4 Logstash (`Elastic/logstash.yaml`)

### Codul:
```yaml
# =============================================================================
# Logstash: consumă din Kafka (mTLS, consumer group logstash-es) și scrie în
# Elasticsearch (TLS, utilizator logstash_writer) în data stream-ul 5g-network.
# =============================================================================
apiVersion: v1
kind: ConfigMap
metadata:
  name: logstash-config
  namespace: logging
data:
  logstash.yml: |
    api.http.host: "0.0.0.0"
    pipeline.workers: 1
    pipeline.ordered: auto
    xpack.monitoring.enabled: false
  pipeline.conf: |
    input {
      kafka {
        bootstrap_servers       => "orion-kafka-kafka-bootstrap.kafka.svc:9093"
        topics                  => ["5g-network-logs", "5g-network-errors"]
        group_id                => "logstash-es"
        client_id               => "logstash-es"
        # 1 thread în lab. Scalare: până la 3 consumatori (= nr. de partiții) în același grup.
        consumer_threads        => 1
        # La prima pornire a grupului citește de la începutul retenției, nu doar mesajele noi.
        auto_offset_reset       => "earliest"
        codec                   => "json"
        decorate_events         => "basic"
        security_protocol       => "SSL"
        ssl_truststore_type     => "PEM"
        ssl_truststore_location => "/certs/kafka-ca/ca.crt"
        ssl_keystore_type       => "PKCS12"
        ssl_keystore_location   => "/certs/kafka-user/user.p12"
        ssl_keystore_password   => "${KAFKA_KEYSTORE_PASSWORD}"
      }
    }

    filter {
      mutate {
        # Topicul sursă devine câmp: în Kibana se poate filtra logs vs errors.
        add_field    => { "[kafka][topic]" => "%{[@metadata][kafka][topic]}" }
        # Câmpuri redundante sau fără valoare de analiză. event.original este o copie
        # integrală a mesajului (ECS) care ar dubla spațiul ocupat în index.
        remove_field => [ "time", "[event][original]",
                          "[kubernetes][docker_id]", "[kubernetes][container_hash]", "[kubernetes][pod_id]" ]
      }
    }

    output {
      elasticsearch {
        hosts                       => ["https://logs-es-http.logging.svc:9200"]
        user                        => "logstash_writer"
        password                    => "${LOGSTASH_ES_PASSWORD}"
        ssl_enabled                 => true
        ssl_certificate_authorities => ["/certs/es/ca.crt"]
        # Data stream creat automat pe baza index template-ului 5g-network* (es-bootstrap).
        index                       => "5g-network"
        action                      => "create"
        data_stream                 => "false"
        manage_template             => false
        ilm_enabled                 => false
      }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logstash
  namespace: logging
  labels:
    app: logstash
spec:
  replicas: 1
  selector:
    matchLabels:
      app: logstash
  template:
    metadata:
      labels:
        app: logstash
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
        seccompProfile:
          type: RuntimeDefault
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values: ["data"]
      tolerations:
        - key: dedicated
          operator: Equal
          value: data
          effect: NoSchedule
      containers:
        - name: logstash
          image: docker.elastic.co/logstash/logstash:9.5.6
          # Fără readOnlyRootFilesystem: entrypoint-ul imaginii Logstash scrie în config/ la pornire.
          # Compensat prin: non-root, drop ALL, fără escaladare; date doar în emptyDir.
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          env:
            - name: LS_JAVA_OPTS
              value: "-Xms384m -Xmx384m"
            - name: KAFKA_KEYSTORE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: logstash-consumer      # creat de Strimzi User Operator
                  key: user.password
            - name: LOGSTASH_ES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: logstash-es-writer
                  key: password
          ports:
            - name: api
              containerPort: 9600
          readinessProbe:
            httpGet:
              path: /
              port: api
            initialDelaySeconds: 30
            periodSeconds: 10
          resources:
            requests:
              cpu: 100m
              memory: 768Mi
            limits:
              cpu: 1000m
              memory: 1Gi
          volumeMounts:
            - name: config
              mountPath: /usr/share/logstash/config/logstash.yml
              subPath: logstash.yml
            - name: config
              mountPath: /usr/share/logstash/pipeline/logstash.conf
              subPath: pipeline.conf
            - name: data
              mountPath: /usr/share/logstash/data
            - name: kafka-ca
              mountPath: /certs/kafka-ca
              readOnly: true
            - name: kafka-user
              mountPath: /certs/kafka-user
              readOnly: true
            - name: es-ca
              mountPath: /certs/es
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: logstash-config
        - name: data
          emptyDir: {}
        - name: kafka-ca
          secret:
            secretName: kafka-cluster-ca
        - name: kafka-user
          secret:
            secretName: logstash-consumer
        - name: es-ca
          secret:
            secretName: logs-es-http-certs-public
```

### Comenzile (ordinea contează):
```bash
# 1. Operatorul ECK
helm repo add elastic https://helm.elastic.co
helm repo update
helm install elastic-operator elastic/eck-operator --version 3.5.0 \
  -n elastic-system --create-namespace -f Elastic/eck-values.yaml

# 2. Elasticsearch + Kibana (3-5 minute până la Ready)
kubectl apply -f Elastic/elasticsearch.yaml
kubectl get elasticsearch,kibana -n logging -w        # HEALTH green, PHASE Ready

# 3. Parola lui Logstash: generată, nu o vezi, nu ajunge în Git
kubectl create secret generic logstash-es-writer -n logging \
  --from-literal=password="$(openssl rand -hex 24)"

# 4. Inițializare (Elasticsearch trebuie să fie Ready)
kubectl apply -f Elastic/es-bootstrap.yaml
kubectl wait job/es-bootstrap -n logging --for=condition=complete --timeout=180s
kubectl logs -n logging job/es-bootstrap          # acknowledged / created ... Bootstrap complet.

# 5. Logstash (după bootstrap: are nevoie de utilizator, template și CA-ul Elasticsearch)
kubectl apply -f Elastic/logstash.yaml
kubectl logs -n logging deploy/logstash --tail=30  # Successfully joined group ... Adding newly assigned partitions
```

### Verificare:
```bash
ES_PASS=$(kubectl get secret logs-es-elastic-user -n logging -o jsonpath='{.data.elastic}' | base64 -d)
kubectl exec -n logging logs-es-default-0 -- curl -s -k -u "elastic:$ES_PASS" \
  "https://localhost:9200/_cat/indices/.ds-5g-network*?v&h=index,docs.count,store.size"
unset ES_PASS

# Kibana: https://localhost:5601 (accepți certificatul self-signed), utilizator elastic
kubectl get secret logs-es-elastic-user -n logging -o jsonpath='{.data.elastic}' | base64 -d; echo
kubectl port-forward -n logging svc/logs-kb-http 5601:5601
```
În Kibana:
1. **Stack Management → Data Views → Create:** name `5G network`, index pattern `5g-network`, timestamp `@timestamp`.
2. **Discover**, interval **Last 2 hours**, căutare KQL `level : "ERROR" and gnb_id : "gNodeB-101"`.
3. Coloane: `level`, `gnb_id`, `slice_id`, `latency_ms`, `active_ue_connections`, `kafka.topic`, `kubernetes.pod_name`.
4. Click pe `latency_ms` → **Visualize**: graficul latenței arată saltul de la 20 ms la 250 ms după fiecare restart al aplicației.

### De ce am făcut-o așa:
1. **TLS și autentificare din prima**, generate de ECK: encryption in transit și fără acces anonim.
2. **Heap 768m dintr-o limită de 1,5 GiB:** aceeași regulă ca la Kafka. Lucene citește indexul prin page cache.
3. **`node.store.allow_mmap: false`:** altfel Elasticsearch cere `vm.max_map_count=262144` pe nod, adică un init container privilegiat. În producție se setează sysctl-ul pe nod și mmap rămâne activ, pentru performanță.
4. **Template-ul înainte de primul document.** Fără el, Elasticsearch creează indexul cu setări implicite:
   - 1 replică, deci status `yellow` pe un singur nod;
   - fără ștergere automată;
   - tipuri de câmpuri ghicite: `gnb_id` ar fi `text` (căutare aproximativă), nu `keyword` (exactă), iar graficele pe el n-ar funcționa.
5. **`kubernetes.labels` de tip `flattened`:** fiecare label nou de pe pod-uri ar deveni altfel un câmp nou în mapping (*mapping explosion*).
6. **Utilizator dedicat `logstash_writer`:** poate doar crea documente și indexuri în `5g-network*`. Logstash nu folosește niciodată superutilizatorul `elastic`.
7. **Job idempotent (doar `PUT`):** poate fi rulat de oricâte ori, cu același rezultat.
8. **Logstash:**
   - **`auto_offset_reset => earliest`:** la prima pornire citește tot ce era deja în Kafka. Logurile au așteptat consumatorul, ceea ce dovedește rolul de tampon.
   - **Câmpuri eliminate:** `event.original` (copia integrală a mesajului, care ar dubla indexul), `time`, `docker_id`, `container_hash`, `pod_id`. Rezultatul: ~230 de bytes per document în index, față de ~1 KB per mesaj în Kafka.
   - **`kafka.topic`** e adăugat ca câmp, ca să poți filtra în Kibana logurile față de erori.
9. **Kibana fără endpoint public:** `kubectl port-forward` folosește autentificarea ta AWS și nu deschide niciun port pe internet. În producție: SSO (SAML/OIDC) și VPN.
10. **ECK gestionează doar namespace-ul `logging`** (`managedNamespaces`): least privilege și pentru operatori.

---

## 14. Verificare finală, cap-coadă

```bash
kubectl get nodes -L workload,topology.kubernetes.io/zone      # apps + data, eu-central-1a
kubectl get pods -A | grep -v -E "Running|Completed"            # doar antetul
kubectl get pvc -A                                              # toate Bound, gp3-storage
kubectl get kafka,kafkatopic -n kafka                           # READY True
kubectl get kafkauser -n logging                                # READY True
kubectl get elasticsearch,kibana -n logging                     # green
kubectl get events -A --field-selector type=Warning --sort-by=.lastTimestamp | tail -20
```

Lanțul complet, verificat:
1. **pyapp** emite metrici și loguri JSON.
2. **Prometheus** citește metricile; alertele de business pornesc la incident.
3. **Fluent Bit** colectează logurile și le trimite prin mTLS în Kafka, în topicul potrivit.
4. **Logstash** citește din Kafka prin mTLS și scrie în Elasticsearch prin HTTPS, cu utilizator dedicat.
5. **Kibana** găsește `level:ERROR and gnb_id:gNodeB-101` în timp real.

---

## 15. Probleme întâlnite și cum le-am rezolvat

| Simptom | Cauza | Rezolvarea |
|---|---|---|
| `kubectl wait kafka` nu se termină; log operator: `watchedNamespace set to a different namespace but the feature is disabled` | Strimzi 1.3.x dezactivează implicit User Operator-ul cross-namespace | `extraEnvs: STRIMZI_ENTITY_OPERATOR_WATCHED_NAMESPACE_ENABLED=true` în `strimzi-values.yaml`, apoi `helm upgrade` |
| Fluent Bit `Pending`: `Too many pods`, `No preemption victims found` | t3.medium are max. 17 pod-uri (IP-uri ENI); DaemonSet-ul avea prioritate 0 | `priorityClassName: system-node-critical`; pe termen lung t3.large sau VPC CNI prefix delegation |
| Pod `Pending` după ce nodul vechi a fost șters, volumul nu se atașează | Node group pe mai multe AZ, discul EBS în altă zonă | node group-uri într-o singură zonă (`availabilityZones`) |
| Jenkins face build, dar modificările lipsesc (`COPY app.py` → `CACHED`) | Commit-ul conținea doar fișierele din staging (`git add` dintr-un subfolder) | `git add -A` din rădăcina repository-ului; verifici `git status` înainte de commit |
| Deploy-ul pică `forbidden` la PodDisruptionBudget | Role-ul Jenkins nu avea grupul `policy` | `poddisruptionbudgets` în `jenkins-rbac.yaml`, aplicat de administrator |
| Logstash: `NOT_COORDINATOR` la prima pornire | Kafka creează topicul intern `__consumer_offsets` la primul consumer group | nimic: clientul reîncearcă singur și intră în grup după câteva secunde |
| Alertele de business folosesc `service="pyapp-service"`, dar metricile aveau `exported_service` | Aplicația punea propriul label `service` | label-ul scos din aplicație; identitatea vine doar din Kubernetes |
| Inhibiția din Alertmanager nu suprimă nimic | `equal` conținea `alertname` / label-uri pe care alerta critică nu le avea | `equal: ['namespace', 'job']` și `sum by (job, namespace)` în regula critică |
| Fișier modificat pe disc revine la versiunea veche | IDE-ul (IntelliJ) a salvat peste el conținutul din editor | **File → Reload All from Disk** înainte de editare |
| `update-authentication-mode`: `from API_AND_CONFIG_MAP to API_AND_CONFIG_MAP` | Clusterul era deja în modul corect | se trece direct la `eksctl create accessentry` |

---

## 16. Ștergerea completă (teardown)

Ordinea contează. Volumele trebuie eliberate cât timp driver-ul EBS încă rulează. Cu `reclaimPolicy: Retain`, ștergerea unui obiect PV **nu** șterge discul EBS.

```bash
# 1. Componentele care dețin resurse AWS (discuri, load balancere)
kubectl delete -f Elastic/logstash.yaml -f Elastic/elasticsearch.yaml
helm uninstall elastic-operator -n elastic-system
helm uninstall fluent-bit -n logging
kubectl delete -f Kafka/kafka-users.yaml -f Kafka/kafka-topic.yaml -f Kafka/kafka-cluster.yaml
helm uninstall strimzi-cluster-operator -n kafka
helm uninstall monitoring -n monitoring

# 2. PVC-urile (Kafka își păstrează PVC-ul intenționat: deleteClaim: false)
kubectl delete pvc --all -n kafka
kubectl delete pvc --all -n monitoring
kubectl delete pvc --all -n logging

# 3. Notezi ID-urile discurilor EBS, apoi ștergi obiectele PV
kubectl get pv -o custom-columns=PV:.metadata.name,EBS:.spec.csi.volumeHandle,CLAIM:.spec.claimRef.name
kubectl delete pv --all

# 4. Niciun Load Balancer rămas în VPC
kubectl get svc -A | grep LoadBalancer        # gol

# 5. Clusterul, node group-urile, NAT-ul și VPC-ul
eksctl delete cluster -f k8s/cluster-config.yaml --wait

# 6. Ce mai poate costa: discuri neatașate, NAT gateway, Elastic IP
aws ec2 describe-volumes --region eu-central-1 --filters Name=status,Values=available \
  --query "Volumes[].{ID:VolumeId,Size:Size,Created:CreateTime}"
aws ec2 delete-volume --region eu-central-1 --volume-id <vol-id>
aws ec2 describe-nat-gateways --region eu-central-1 --filter Name=state,Values=available --query "NatGateways[].NatGatewayId"
aws ec2 describe-addresses --region eu-central-1 --query "Addresses[].{IP:PublicIp,Assoc:AssociationId}"
```

Ce **rămâne** intenționat, pentru următoarea reconstrucție: cheia KMS (ștergerea are o perioadă de așteptare de minimum 7 zile), repository-ul ECR, politica IAM a autoscaler-ului, rolul Jenkins și serverul Jenkins. Dacă nu mai ai nevoie de Jenkins, oprește instanța și eliberează Elastic IP-ul: un Elastic IP neatașat se plătește.

Pauză în loc de ștergere: `eksctl scale nodegroup --cluster bootcamp-cluster --name data-nodes --nodes 0`.

---

## 17. Lab vs. producție și ce urmează

| Subiect | Lab | Producție |
|---|---|---|
| Zone de disponibilitate | una (`eu-central-1a`) | câte un node group per zonă, replici distribuite cu topology spread |
| API EKS | public, restricționat pe IP | privat, prin VPN / bastion / SSM |
| NAT | unul | unul per zonă |
| Kafka | 1 broker, RF 1 | 3 brokeri + 3 controlleri pe zone diferite, RF 3, `min.insync.replicas: 2` |
| Elasticsearch | 1 nod, 0 replici, fără mmap | 3+ noduri, 1 replică, `vm.max_map_count` pe noduri, snapshot-uri în S3 |
| Prometheus | 1 replică, 15 zile | 2 replici, stocare pe termen lung (Thanos / AMP) |
| CA-ul Kafka în alte namespace-uri | script | cert-manager trust-manager / External Secrets |
| Acces Kibana / Grafana | `port-forward`, utilizator local | SSO cu roluri, prin VPN |
| Backup | `Retain` (protecție la ștergere) | snapshot-uri EBS programate (AWS Backup / VolumeSnapshot) |

**Următorii pași:**
1. Alerte pentru pipeline-ul de loguri: consumer lag-ul grupului `logstash-es` (Kafka Exporter), starea Elasticsearch (elasticsearch-exporter), retry-urile eșuate ale Fluent Bit.
2. NetworkPolicies `default deny` per namespace, cu network policy activat în VPC CNI. Pentru listener-ul Kafka, policy-ul există deja.
3. MinIO ca depozit S3 pentru snapshot-urile Elasticsearch.
4. Alarme de securitate în CloudWatch pe CloudTrail (folosirea contului root, modificări IAM/KMS/security groups, login fără MFA) și GuardDuty pentru EKS.
5. Trivy în pipeline: build-ul pică la vulnerabilități critice.
6. VPC CNI prefix delegation, ca alternativă la instanțe mai mari.
