# Aether: High-Performance Distributed Systems Core

O Aether é um runtime de orquestração e computação distribuída de baixa latência escrito em Rust. O sistema usa Protocol Buffers sobre transporte gRPC/HTTP2 para definir contratos fortemente tipados entre nós Master, Workers e Clients.

Esta versão evolui o protótipo inicial para uma base operacional mais segura, com identificação explícita de Workers, política de comandos, telemetria ampliada, scheduler com consciência de capacidade, suporte multiplataforma inicial e uma interface web de controle chamada **Aether Console**.

---

## Arquitetura

```text
Client CLI / Aether Console
        │ InjectTask
        ▼
Aether Master ── Scheduler ──► Workers
   │                           │
   ├── gRPC :50051              ├── WorkerService :50052+
   └── Web UI :8080             └── shell controlado por política
```

## Principais capacidades

- Master gRPC para heartbeat, fila e despacho de tarefas.
- Worker gRPC para execução remota com streaming de saída.
- Client CLI para injeção manual de tarefas.
- Aether Console em `http://IP_DO_MASTER:8080`.
- `--worker-id` explícito.
- `--worker-port` explícito.
- Política de allowlist/denylist de comandos.
- Bloqueio de execução como root por padrão no Worker.
- Scheduler baseado em CPU livre, RAM livre, temperatura e slots.
- Tags de Workers para roteamento de tarefas.
- Suporte inicial a Linux, WSL2 e Windows nativo via PowerShell.
- Logs estruturados com `tracing`.

---

## Stack técnica

- Rust stable
- Tokio
- Tonic gRPC
- Prost/Protocol Buffers
- Axum para Aether Console
- Sysinfo para telemetria
- Clap para CLI
- Serde/TOML/JSON para configuração e API

---

## Instalação no Ubuntu Server

```bash
sudo apt update
sudo apt install -y git curl ca-certificates build-essential pkg-config protobuf-compiler
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustup update stable
```

```bash
git clone https://github.com/CFSJCODE/Cluster-Aether.git
cd Cluster-Aether
cargo build --release
```

---

## Execução local

Você pode manter a configuração em TOML e sobrescrever valores pontuais pela CLI:

```bash
./target/release/aether --mode master --config config/aether.master.example.toml
./target/release/aether --mode worker --config config/aether.worker.example.toml --worker-id worker-lab-01
```

Os exemplos em `config/` definem portas, tags, limites de concorrência e política de comandos.

### Master com Aether Console

```bash
./target/release/aether --mode master --port 50051 --web-port 8080
```

Acesse:

```text
http://127.0.0.1:8080
```

### Worker local

```bash
./target/release/aether \
  --mode worker \
  --worker-id ryzen-local \
  --master-ip 127.0.0.1 \
  --port 50051 \
  --worker-port 50052 \
  --max-concurrent-tasks 4 \
  --tags local,ryzen,high-performance,linux
```

### Client CLI

```bash
./target/release/aether \
  --mode client \
  --master-ip 127.0.0.1 \
  --port 50051 \
  --command "hostname && uptime"
```

---

## Topologia recomendada para laboratório

```text
Ryzen 5 4600G + Ubuntu Server
├── Master        :50051
├── Aether Console:8080
└── Worker local  :50052, CPUQuota 650% via systemd

HP Z230 + Windows/WSL2
└── Worker remoto :50052

Samsung Essentials E34 + Windows/WSL2
└── Worker remoto :50052
```

---

## Segurança operacional

O Worker executa comandos recebidos pela rede. Portanto:

- não exponha as portas do cluster à Internet;
- execute somente em LAN, VPN ou laboratório isolado;
- use firewall por IP;
- não execute o Worker como root;
- mantenha `allow_unsafe_commands = false`;
- adicione autenticação/mTLS antes de qualquer uso produtivo.

---

## Serviços systemd

Exemplos estão em:

```text
deploy/systemd/aether-master.service
deploy/systemd/aether-worker-local.service
```

Instalação sugerida:

```bash
sudo useradd --system --home /opt/aether --shell /usr/sbin/nologin aether
sudo mkdir -p /opt/aether/bin
sudo cp ./target/release/aether /opt/aether/bin/aether
sudo cp deploy/systemd/aether-master.service /etc/systemd/system/
sudo cp deploy/systemd/aether-worker-local.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aether-master
sudo systemctl enable --now aether-worker-local
```

---

## Roadmap

O plano de evolução por fases está documentado em [`docs/ROADMAP.md`](docs/ROADMAP.md).
