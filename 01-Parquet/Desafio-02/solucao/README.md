# Apache Spark Challenges
Author: Prof. Barbosa<br>
Contact: infobarbosa@gmail.com<br>
Github: [infobarbosa](https://github.com/infobarbosa)

# Arquivos Parquet

```
df = spark \
    .read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep",";") \
    .load("./assets/202311_NovoBolsaFamilia_SP.csv.gz")
```

```
df.printSchema()
```

```
>>> df.printSchema()
root
 |-- MES_COMPETENCIA: string (nullable = true)
 |-- MES_REFERENCIA: string (nullable = true)
 |-- UF: string (nullable = true)
 |-- CODIGO_MUNICIPIO_SIAFI: string (nullable = true)
 |-- MUNICIPIO: string (nullable = true)
 |-- CPF: string (nullable = true)
 |-- NIS: string (nullable = true)
 |-- FAVORECIDO: string (nullable = true)
 |-- VALOR: string (nullable = true)

```

```
df.write.parquet("./output/beneficiarios.parquet", mode="overwrite")
```

