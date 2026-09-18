# -*- coding: utf-8 -*-
"""
processamento.py - Atividade 2
Pipeline: Raw -> Data Quality -> Quarentena (JSON) -> Silver -> Gold

1. Le os CSVs da camada RAW no S3
2. Aplica regras de qualidade:
   - quantidade <= 0                 -> rejeita
   - cliente_id/product_id invalidos -> rejeita
3. Grava rejeitados COM MOTIVO em JSON na quarentena
4. Silver: JOIN pedidos validos + clientes + produtos, com valor_total derivado
5. Gold: agregacoes analiticas (receita por cliente e por produto)
"""
import csv, io, json
from datetime import date
import boto3

BUCKET = "atividade2-guilherme-2026"
REGIAO = "us-east-2"
HOJE = date.today().isoformat()

s3 = boto3.client("s3", region_name=REGIAO)

# ---------- Leitura da camada RAW ----------
def ler_csv_raw(tabela):
    chave = f"raw/{tabela}/ingest_date={HOJE}/{tabela}.csv"
    obj = s3.get_object(Bucket=BUCKET, Key=chave)
    linhas = list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8"))))
    print(f"RAW lida: {tabela} -> {len(linhas)} registros")
    return linhas

clientes = ler_csv_raw("clientes")
produtos = ler_csv_raw("produtos")
pedidos  = ler_csv_raw("pedidos")

ids_clientes = {c["cliente_id"] for c in clientes}
ids_produtos = {p["product_id"] for p in produtos}
mapa_clientes = {c["cliente_id"]: c for c in clientes}
mapa_produtos = {p["product_id"]: p for p in produtos}

# ---------- Data Quality + Quarentena ----------
validos, rejeitados = [], []
for p in pedidos:
    motivos = []
    if int(p["quantidade"]) <= 0:
        motivos.append("quantidade_invalida (<= 0)")
    if p["cliente_id"] not in ids_clientes:
        motivos.append("cliente_id inexistente na dimensao clientes")
    if p["product_id"] not in ids_produtos:
        motivos.append("product_id inexistente na dimensao produtos")
    if motivos:
        rejeitados.append({**p, "motivo_rejeicao": "; ".join(motivos)})
    else:
        validos.append(p)

print(f"Validos: {len(validos)} | Rejeitados (quarentena): {len(rejeitados)}")

# ---------- Gravar QUARENTENA (JSON com motivo) ----------
corpo_json = "\n".join(json.dumps(r, ensure_ascii=False) for r in rejeitados)
chave_q = f"quarantine/pedidos_rejeitados/data={HOJE}/rejeitados.json"
s3.put_object(Bucket=BUCKET, Key=chave_q, Body=corpo_json.encode("utf-8"))
print(f"Quarentena: s3://{BUCKET}/{chave_q}")

# ---------- Camada SILVER: JOIN + campo derivado ----------
cabecalho = ["pedido_id", "data_pedido", "cliente_id", "nome_cliente",
             "cidade", "estado", "product_id", "produto", "categoria",
             "preco", "quantidade", "valor_total"]
buf = io.StringIO()
w = csv.writer(buf)
w.writerow(cabecalho)
for p in validos:
    c = mapa_clientes[p["cliente_id"]]
    pr = mapa_produtos[p["product_id"]]
    qtd = int(p["quantidade"])
    preco = float(pr["preco"])
    w.writerow([p["pedido_id"], p["data_pedido"], c["cliente_id"], c["nome"],
                c["cidade"], c["estado"], pr["product_id"], pr["produto"],
                pr["categoria"], preco, qtd, round(qtd * preco, 2)])
chave_s = f"processed/fato_vendas/ingest_date={HOJE}/fato_vendas.csv"
s3.put_object(Bucket=BUCKET, Key=chave_s, Body=buf.getvalue().encode("utf-8"))
print(f"Silver: s3://{BUCKET}/{chave_s}")

# ---------- Camada GOLD: agregacoes ----------
receita_cliente, receita_produto = {}, {}
for p in validos:
    qtd = int(p["quantidade"])
    preco = float(mapa_produtos[p["product_id"]]["preco"])
    total = qtd * preco
    nome = mapa_clientes[p["cliente_id"]]["nome"]
    prod = mapa_produtos[p["product_id"]]["produto"]
    receita_cliente[nome] = receita_cliente.get(nome, 0) + total
    receita_produto[prod] = receita_produto.get(prod, 0) + total

def gravar_gold(nome, cab, linhas):
    b = io.StringIO()
    w = csv.writer(b)
    w.writerow(cab)
    w.writerows(linhas)
    chave = f"gold/{nome}/ingest_date={HOJE}/{nome}.csv"
    s3.put_object(Bucket=BUCKET, Key=chave, Body=b.getvalue().encode("utf-8"))
    print(f"Gold: s3://{BUCKET}/{chave}")

gravar_gold("receita_por_cliente",
            ["nome_cliente", "receita_total"],
            sorted([[k, round(v, 2)] for k, v in receita_cliente.items()],
                   key=lambda x: -x[1]))
gravar_gold("receita_por_produto",
            ["produto", "receita_total"],
            sorted([[k, round(v, 2)] for k, v in receita_produto.items()],
                   key=lambda x: -x[1]))

# ---------- Resumo para conciliacao ----------
print("\n=== CONCILIACAO ===")
print(f"Total RAW: {len(pedidos)} = Silver {len(validos)} + Quarentena {len(rejeitados)}")
print("Pipeline concluido com sucesso!")
