# Apache Spark Challenges
Author: Prof. Barbosa<br>
Contact: infobarbosa@gmail.com<br>
Github: [infobarbosa](https://github.com/infobarbosa)

# Arquivo Parquet

Crie um dataframe a partir dos dados a seguir e na sequência escreva em um arquivo parquet armazenado em data lake.

Dados:
```
1,Barbosa,infobarbosa@gmail.com
2,Luciana,luciana@gmail.com
3,Roberto,roberto@gmail.com
4,Aristides,aristides@yahoo.com
```
Onde:
1. Coluna 1=id
2. Coluna 2=nome
3. Coluna 3=e-mail

Considere:
1. Arquivo destino: ./clientes/cliente.parquet

## Solução

Os dados:
```
dados = [("1","Barbosa","infobarbosa@gmail.com"),
        ("2", "Luciana", "luciana@gmail.com"),
        ("3", "Roberto", "roberto@gmail.com"),
        ("4", "Aristides", "aristides@yahoo.com")]
```

O schema:
```
schema = ["id", "nome", "email"]
```

O dataframe:
```
df = spark.createDataFrame(data=dados, schema=schema)
```

```
df.printSchema()
```

Output esperado:
```
>>> df.printSchema()
root
 |-- id: string (nullable = true)
 |-- nome: string (nullable = true)
 |-- email: string (nullable = true)
```

Salvando o dataframe:
```
pasta_destino="./clientes/cliente.parquet"

df.write.parquet(pasta_destino, mode="overwrite")
```
