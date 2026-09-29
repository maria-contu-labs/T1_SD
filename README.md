# Trabalho 1 — DiMEx + Snapshot

Trabalho desenvolvido para a disciplina de Sistemas Distribuídos.

## Integrante

Maria Eduarda Santana Contu

## Estrutura

```text
T1_SD/
├── DIMEX/
│   └── DIMEX.go
├── PP2PLink/
│   └── PP2PLink.go
├── checkMX.go
├── snapshotChecker.go
├── useDIMEX-f.go
├── useDIMEX-snapshot.go
└── go.mod
```

O projeto utiliza o `PP2PLink` disponibilizado no template da disciplina e implementa o algoritmo de exclusão mútua distribuída no módulo `DIMEX`.

A Parte 2 estende o DiMEx com o algoritmo de snapshot de Chandy-Lamport.

## Requisitos

- Go instalado
- três terminais abertos na pasta do projeto

## Parte 1 — DiMEx

Antes da execução:

```bash
rm -f mxOUT.txt
```

Execute três processos.

### Processo 0

```bash
go run useDIMEX-f.go 0 \
127.0.0.1:5000 \
127.0.0.1:6001 \
127.0.0.1:7002
```

### Processo 1

```bash
go run useDIMEX-f.go 1 \
127.0.0.1:5000 \
127.0.0.1:6001 \
127.0.0.1:7002
```

### Processo 2

```bash
go run useDIMEX-f.go 2 \
127.0.0.1:5000 \
127.0.0.1:6001 \
127.0.0.1:7002
```

Após deixar os processos executando por algum tempo, encerre-os com `Ctrl+C`.

Para verificar o arquivo gerado:

```bash
go run checkMX.go
```

Uma execução correta não deve apresentar ocorrências de:

```text
||
```

ou:

```text
..
```

## Parte 2 — Snapshot de Chandy-Lamport

Antes da execução:

```bash
rm -f snapshots_p*.jsonl
rm -f mxOUT-snapshot.txt
```

Execute novamente três processos.

### Processo 0

```bash
go run useDIMEX-snapshot.go 0 \
127.0.0.1:5000 \
127.0.0.1:6001 \
127.0.0.1:7002
```

### Processo 1

```bash
go run useDIMEX-snapshot.go 1 \
127.0.0.1:5000 \
127.0.0.1:6001 \
127.0.0.1:7002
```

### Processo 2

```bash
go run useDIMEX-snapshot.go 2 \
127.0.0.1:5000 \
127.0.0.1:6001 \
127.0.0.1:7002
```

O processo 0 inicia 300 snapshots sucessivos enquanto continua utilizando o DiMEx normalmente.

Ao final são gerados:

```text
snapshots_p0.jsonl
snapshots_p1.jsonl
snapshots_p2.jsonl
```

Para verificar a quantidade:

```bash
wc -l snapshots_p*.jsonl
```

Para avaliar os snapshots e suas invariantes:

```bash
go run snapshotChecker.go
```

Na execução correta, os snapshots devem passar por todas as invariantes.

## Resultado de validação

A execução correta do snapshot produziu:

```text
Snapshots encontrados: 300
Snapshots completos: 300
Snapshots válidos: 300
Snapshots inválidos: 0
Snapshots incompletos: 0

RESULTADO: invariantes preservadas.
```