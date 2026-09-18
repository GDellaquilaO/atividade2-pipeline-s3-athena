# -*- coding: utf-8 -*-
"""
ingestao.py - Atividade 2
Gera massa de dados simulada (COM anomalias propositais) e sobe para o S3
na camada RAW, particionada por data de ingestao (Hive Style).

Estrutura gerada:
s3://atividade2-guilherme-2026/raw/clientes/ingest_date=YYYY-MM-DD/clientes.csv
s3://atividade2-guilherme-2026/raw/produtos/ingest_date=YYYY-MM-DD/produtos.csv
s3://atividade2-guilherme-2026/raw/pedidos/ingest_date=YYYY-MM-DD/pedidos.csv
"""
import csv, io, random
from datetime import date
import boto3

BUCKET = "atividade2-guilherme-2026"
REGIAO = "us-east-2"
HOJE = date.today().isoformat()  # YYYY-MM-DD

random.seed(42)
s3 = boto3.client("s3", region_name=REGIAO)

# ---------- 1. Gerar CLIENTES (dimensao - sem anomalias) ----------
clientes = [["cliente_id", "nome", "cidade", "estado"]]
nomes = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Gabi", "Hugo",
         "Iris", "Joao", "Karina", "Lucas", "Maria", "Nina", "Otavio",
         "Paula", "Rafa", "Sofia", "Tiago", "Vera"]
cidades = [("Sao Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Curitiba", "PR"),
           ("Porto Alegre", "RS"), ("Recife", "PE")]
for i in range(1, 21):
    cid, uf = random.choice(cidades)
    clientes.append([i, f"{nomes[i-1]} Silva", cid, uf])

# ---------- 2. Gerar PRODUTOS (dimensao - sem anomalias) ----------
produtos = [["product_id", "produto", "categoria", "preco"]]
itens = [("Notebook", "Informatica", 3500.00), ("Mouse", "Informatica", 80.00),
         ("Teclado", "Informatica", 150.00), ("Monitor", "Informatica", 900.00),
         ("Cadeira Gamer", "Moveis", 1200.00), ("Mesa", "Moveis", 600.00),
         ("Headset", "Audio", 250.00), ("Caixa de Som", "Audio", 180.00),
         ("Webcam", "Informatica", 300.00), ("Impressora", "Informatica", 800.00)]
for i, (nome, cat, preco) in enumerate(itens, start=1):
    produtos.append([i, nome, cat, preco])

# ---------- 3. Gerar PEDIDOS (fato - COM ANOMALIAS PROPOSITAIS) ----------
pedidos = [["pedido_id", "cliente_id", "product_id", "quantidade", "data_pedido"]]
anomalias = 0
for i in range(1, 51):
    cliente_id = random.randint(1, 20)
    product_id = random.randint(1, 10)
    quantidade = random.randint(1, 5)

    # ~20% dos pedidos com anomalia
    r = random.random()
    if r < 0.08:
        quantidade = -random.randint(1, 3)      # ANOMALIA 1: quantidade negativa
        anomalias += 1
    elif r < 0.12:
        quantidade = 0                          # ANOMALIA 1b: quantidade zero
        anomalias += 1
    elif r < 0.16:
        cliente_id = 999                        # ANOMALIA 2: cliente inexistente
        anomalias += 1
    elif r < 0.20:
        product_id = 777                        # ANOMALIA 2b: produto inexistente
        anomalias += 1

    pedidos.append([i, cliente_id, product_id, quantidade, HOJE])

print(f"Pedidos gerados: {len(pedidos)-1} | Anomalias inseridas de proposito: {anomalias}")

# ---------- 4. Upload para o S3 (camada RAW, Hive Style) ----------
def enviar_csv(nome_tabela, linhas):
    buf = io.StringIO()
    csv.writer(buf).writerows(linhas)
    chave = f"raw/{nome_tabela}/ingest_date={HOJE}/{nome_tabela}.csv"
    s3.put_object(Bucket=BUCKET, Key=chave, Body=buf.getvalue().encode("utf-8"))
    print(f"Enviado: s3://{BUCKET}/{chave}")

enviar_csv("clientes", clientes)
enviar_csv("produtos", produtos)
enviar_csv("pedidos", pedidos)
print("Ingestao concluida com sucesso!")
