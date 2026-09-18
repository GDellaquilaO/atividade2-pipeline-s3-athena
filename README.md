# Atividade 2 — Pipeline Medallion no Amazon S3 + Athena

Pipeline completo de ingestão, validação de qualidade (Data Quality), quarentena de anomalias,
camadas analíticas (Raw / Silver / Gold) e auditoria de metadados no Amazon Athena.

## Estrutura do repositório

```
├── ingestao.py        # Gera dados simulados (com anomalias) e sobe para a camada Raw
├── processamento.py   # Data Quality, quarentena, Silver e Gold
├── requirements.txt   # boto3
├── prints/            # Capturas do console Athena (queries + resultados)
└── README.md
```

## Pré-requisitos

- Python 3.x com `pip install boto3`
- Credenciais AWS configuradas (`aws configure` ou `python -m awscli configure`)
- Bucket S3: `atividade2-guilherme-2026` (região us-east-2)
- Athena com local de resultados: `s3://atividade2-guilherme-2026/athena-results/`

## Como executar

```bash
python ingestao.py        # 1) gera dados com anomalias e sobe para raw/
python processamento.py   # 2) DQ + quarentena + Silver + Gold
```

Depois, no Athena, executar os scripts SQL da seção abaixo.

## Estrutura de pastas no S3

```
s3://atividade2-guilherme-2026/
├── raw/
│   ├── clientes/ingest_date=YYYY-MM-DD/clientes.csv
│   ├── produtos/ingest_date=YYYY-MM-DD/produtos.csv
│   └── pedidos/ingest_date=YYYY-MM-DD/pedidos.csv
├── quarantine/
│   └── pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json   # JSON com motivo da rejeição
├── processed/
│   └── fato_vendas/ingest_date=YYYY-MM-DD/fato_vendas.csv   # Silver (JOIN + valor_total)
├── gold/
│   ├── receita_por_cliente/ingest_date=YYYY-MM-DD/
│   └── receita_por_produto/ingest_date=YYYY-MM-DD/
└── athena-results/                                           # resultados das queries
```

## Regras de Data Quality aplicadas

1. `quantidade <= 0` → registro rejeitado
2. `cliente_id` inexistente na dimensão clientes → rejeitado
3. `product_id` inexistente na dimensão produtos → rejeitado

Todos os rejeitados são gravados em JSON na quarentena **com o motivo da rejeição**.

## Queries do Athena (auditoria e conciliação)

### Criação das tabelas

```sql
CREATE DATABASE IF NOT EXISTS atividade2;

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2.raw_pedidos (
  pedido_id int, cliente_id int, product_id int, quantidade int, data_pedido string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://atividade2-guilherme-2026/raw/pedidos/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2.silver_fato_vendas (
  pedido_id int, data_pedido string, cliente_id int, nome_cliente string,
  cidade string, estado string, product_id int, produto string, categoria string,
  preco double, quantidade int, valor_total double
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://atividade2-guilherme-2026/processed/fato_vendas/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE atividade2.raw_pedidos;
MSCK REPAIR TABLE atividade2.silver_fato_vendas;
```

### Auditoria de metadados ($path e $file_size)

```sql
SELECT pedido_id, quantidade,
       "$path" AS caminho_arquivo,
       "$file_size" AS tamanho_bytes,
       "$file_modified_time" AS modificado_em
FROM atividade2.raw_pedidos
LIMIT 20;
```

### Conciliação de integridade (Raw = Silver + Quarentena)

```sql
SELECT
  (SELECT COUNT(*) FROM atividade2.raw_pedidos)        AS total_raw,
  (SELECT COUNT(*) FROM atividade2.silver_fato_vendas) AS total_silver,
  (SELECT COUNT(*) FROM atividade2.raw_pedidos)
    - (SELECT COUNT(*) FROM atividade2.silver_fato_vendas) AS total_quarentena_esperado;
```

## Evidências (prints)

- [ ] Execução da ingestão e processamento (terminal)
- [ ] Estrutura de pastas no console S3
- [ ] Query de metadados com `$path` e `$file_size` no Athena
- [ ] Resultado do SELECT de conciliação no Athena
- [ ] Conteúdo do JSON de quarentena
