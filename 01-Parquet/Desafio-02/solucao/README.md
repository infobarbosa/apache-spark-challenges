# Apache Spark Challenges
Author: Prof. Barbosa<br>
Contact: infobarbosa@gmail.com<br>
Github: [infobarbosa](https://github.com/infobarbosa)

# Arquivos Parquet

```
from pyspark.sql.types import *

schema = StructType([ \
    StructField("MES_COMPETENCIA", IntegerType(), True), \
    StructField("MES_REFERENCIA", IntegerType(), True),\
    StructField("UF", StringType(), True),
    StructField("CODIGO_MUNICIPIO_SIAFI", StringType(), True),\
    StructField("MUNICIPIO", StringType(), True),\
    StructField("CPF", StringType(), True),\
    StructField("NIS", LongType(), True),\
    StructField("FAVORECIDO", StringType(), True),\
    StructField("VALOR", FloatType(), True)])

```

```
df = spark \
    .read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .option("sep",";") \
    .schema(schema) \
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

