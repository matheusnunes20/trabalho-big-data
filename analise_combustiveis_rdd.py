from pyspark import SparkContext
import csv
from statistics import mean, median, stdev

# Caminho do arquivo de exemplo (ajuste conforme necessário)
caminho_csv = "C:\\Users\\mateu\\dados_combustiveis\\ca-2004-01.csv"

# Configuração do Spark com correção para Windows (NativeIO)
sc = SparkContext(appName="AnaliseCombustiveis")

# Leitura dos arquivos como texto
linhas = sc.textFile(caminho_csv)

# Pega a primeira linha como cabeçalho
header = linhas.first()

# Função que converte linha de texto CSV para lista de campos
def parse_csv(linha):
    try:
        return next(csv.reader([linha], delimiter=';'))
    except Exception:
        print(f"Erro ao processar linha: {linha}. Erro: {e}")
        return []

# Remove cabeçalho e aplica parse
dados = (
    linhas.filter(lambda l: l != header)
          .map(parse_csv)
          .filter(lambda row: len(row) >= 13 and row[12])  # Garante que tem "Valor de Venda"
)

# Lista de produtos que vamos analisar
produtos = ["GASOLINA", "ETANOL", "DIESEL", "DIESEL S10"]

# Função que analisa estatísticas para um produto
def analisar_produto(nome_produto):
    # precos = (
    #     dados.filter(lambda row: row[10].strip().upper() == nome_produto)
    #          .map(lambda row: float(row[12].replace(",", ".")))  # Convertendo para float
    #          .collect()
    # )

    precos = (
    dados.filter(lambda row: row[10].strip().upper() == nome_produto)
         .map(lambda row: float(row[12].replace(",", ".")))
         .take(10)
    )
    
    print(f"{nome_produto} -> primeiros 10 preços: {precos}")

    if not precos:
        return f"{nome_produto}: sem dados"

    return {
        "produto": nome_produto,
        "media": round(mean(precos), 3),
        "mediana": round(median(precos), 3),
        "desvio_padrao": round(stdev(precos), 3) if len(precos) > 1 else 0.0,
        "total_registros": len(precos)
    }

# Executa análise para cada produto
for p in produtos:
    resultado = analisar_produto(p)
    print(resultado)
