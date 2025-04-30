import logging
logging.getLogger("py4j").setLevel(logging.WARN)

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

conf = SparkConf().setAppName("Analises Combustiveis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Leitura e preparação dos dados
df = spark.read.option("header", True).option("delimiter", ";") \
    .csv("C:/Users/mateu/dados_combustiveis/ca-2004-01.csv")

# Renomeia colunas que vamos usar
df = df.withColumnRenamed("Estado", "estado") \
       .withColumnRenamed("Produto", "produto") \
       .withColumnRenamed("Valor de Venda", "preco_venda") \
       .withColumnRenamed("Revenda", "revenda") \
       .withColumnRenamed("Bairro", "bairro") \
       .withColumnRenamed("Municipio", "municipio") \
       .withColumnRenamed("Bandeira", "bandeira")

# Converte valores da coluna preco_venda para float (trocando vírgula por ponto)
df = df.withColumn("preco_venda", regexp_replace("preco_venda", ",", ".").cast("float"))

# Registra como tabela temporária para usar SQL
df.createOrReplaceTempView("combustiveis")

# Consulta SQL para calcular a média de preço do Diesel em SP
resultado = spark.sql("""
    SELECT 
        estado, 
        produto, 
        ROUND(AVG(preco_venda), 3) AS preco_medio
    FROM combustiveis
    WHERE 
        estado = 'SP'
        AND produto = 'DIESEL'
        AND preco_venda IS NOT NULL
    GROUP BY estado, produto
""")

# Mostra o resultado no console
resultado.show()

# Consulta SQL para calcular média, mediana e desvio padrão dos preços por produto
resultado = spark.sql("""
    SELECT 
        produto,
        ROUND(AVG(preco_venda), 3) AS media,
        ROUND(STDDEV(preco_venda), 3) AS desvio_padrao
    FROM combustiveis
    WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL', 'DIESEL S10') AND preco_venda IS NOT NULL
    GROUP BY produto
""")
resultado.show()

# Para mediana, pode-se usar approxQuantile fora do SQL:
produtos = ['GASOLINA', 'ETANOL', 'DIESEL']
for p in produtos:
    valores = df.filter((col("produto") == p) & (col("preco_venda").isNotNull())) \
                .approxQuantile("preco_venda", [0.5], 0.01)
    print(f"Mediana de {p}: {valores[0]}")

# Top 3 revendas de SP com maior média de GASOLINA, ETANOL e DIESEL
resultado = spark.sql("""
    SELECT estado, municipio, revenda, produto, ROUND(AVG(preco_venda), 3) AS media_preco
    FROM combustiveis
    WHERE estado = 'SP' AND produto IN ('GASOLINA', 'ETANOL', 'DIESEL')
    GROUP BY estado, municipio, revenda, produto
""")
resultado.createOrReplaceTempView("medias_revenda")

top3 = spark.sql("""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY produto ORDER BY media_preco DESC) as rank
        FROM medias_revenda
    ) WHERE rank <= 3
""")
top3.show()

# Maior valor de venda por bandeira no estado de SP
resultado = spark.sql("""
    SELECT bandeira, MAX(preco_venda) AS preco_max
    FROM combustiveis
    WHERE estado = 'SP' AND preco_venda IS NOT NULL
    GROUP BY bandeira
""")
resultado.show()

# Município com maior e menor preço médio de DIESEL
resultado = spark.sql("""
    SELECT municipio, ROUND(AVG(preco_venda), 3) AS media
    FROM combustiveis
    WHERE produto = 'DIESEL' AND preco_venda IS NOT NULL
    GROUP BY municipio
    ORDER BY media DESC
""")
resultado.show(1)  # Maior

resultado.orderBy("media").show(1)  # Menor

# Top 3 bairros de Recife com maior média de DIESEL e DIESEL S10
resultado = spark.sql("""
    SELECT bairro, produto, ROUND(AVG(preco_venda), 3) AS media
    FROM combustiveis
    WHERE municipio = 'RECIFE' AND produto IN ('DIESEL', 'DIESEL S10') AND preco_venda IS NOT NULL
    GROUP BY bairro, produto
""")
resultado.createOrReplaceTempView("medias_bairros_recife")

top3 = spark.sql("""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY produto ORDER BY media DESC) as rank
        FROM medias_bairros_recife
    ) WHERE rank <= 3
""")
top3.show()


# Salva o resultado em CSV
resultado.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/mateu/resultado_sql_diesel_sp")
