# Cluster-Aether — Roadmap técnico por fases

## Fase 1 — Hardening operacional

- Adicionar `--worker-id` explícito para evitar colisões entre Linux, WSL, Windows e containers.
- Adicionar `--worker-port` explícito para não depender de `master_port + 1`.
- Bloquear execução do Worker como root por padrão.
- Criar política de comandos com allowlist e denylist defensiva.
- Desativar o Process Thief por padrão, mantendo-o como experimento futuro.
- Substituir `println!` por logs estruturados via `tracing`.

## Fase 2 — Suporte multiplataforma

- Resolver identificação do Worker por `--worker-id`, `AETHER_WORKER_ID`, `COMPUTERNAME`, `HOSTNAME` ou `/etc/hostname`.
- Executar comandos por `sh -c` em Unix/Linux e por PowerShell em Windows.
- Registrar sistema operacional, shell, tags e slots de execução no heartbeat.

## Fase 3 — Scheduler com consciência de capacidade

- Calcular score por CPU livre, RAM livre, temperatura e slots livres.
- Bloquear despacho para Workers sem slots disponíveis.
- Permitir seleção por tags de Worker.
- Registrar tarefas nos estados `QUEUED`, `DISPATCHED`, `SUCCEEDED`, `FAILED` e `RETRYING`.

## Fase 4 — Aether Console

- Expor interface web no Master.
- Expor APIs JSON para status do cluster, Workers e tarefas.
- Permitir injeção de tarefas pela interface web.
- Exibir histórico recente de tarefas, saída parcial e Workers online.

## Fase 5 — Ubuntu Server e operação contínua

- Incluir exemplos de `systemd` para Master e Worker local.
- Incluir exemplos de configuração TOML.
- Documentar execução em Ubuntu Server, WSL2 e Workers remotos.
- Usar `CPUQuota` para permitir Master + Worker local no Ryzen 5 4600G sem saturar o nó coordenador.

## Fase 6 — Próximas evoluções recomendadas

- Persistir Workers, tarefas e auditoria em SQLite.
- Adicionar autenticação da Aether Console.
- Implementar token/mTLS entre Master e Workers.
- Implementar cancelamento de tarefa.
- Implementar upload de scripts e coleta de artefatos.
- Expor métricas Prometheus.
- Criar instalador de Workers via script gerado pelo Master.
