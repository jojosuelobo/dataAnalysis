'''
Descrição das colunas, veja a seção “Attribute information” em: https://www.kaggle.com/datasets/fedesoriano/stroke-prediction-dataset
Documentação Apache Spark: https://spark.apache.org/docs/latest/sql-getting-started.html
Documentação ML, arvóre de decisão: https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier
'''

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Atividade Módulo 2") \
        .getOrCreate()

data_df = spark.read.csv('stroke_data.csv', header='True', inferSchema='True')

'''
Pergunta 1: Quantos registros existem no arquivo? 
'''
print("Número de registros no arquivo:", data_df.count())
# Resposta: 67135

'''
Pergunta 2: Quantas colunas existem no arquivo? Quantas são numéricas? 
Ao ler o arquivo com spark.read.csv, habilite inferSchema=True. Use a função printSchema() da API de Dataframes.
'''
data_df.printSchema()
print("Número total de colunas:", len(data_df.columns))
colunas_numericas = [col[0] for col in data_df.dtypes if col[1] in ['int', 'double']]
num_colunas_numericas = len(colunas_numericas)
print("Número de colunas numéricas:", num_colunas_numericas)
# Resposta: 12 e 7

'''
Pergunta 3: No conjunto de dados, quantos pacientes sofreram e não sofreram derrame (stroke), respectivamente?
'''
stroke_0 = data_df.filter(data_df.stroke == 0).count()
stroke_1 = data_df.filter(data_df.stroke == 1).count()
print("Número de registros com stroke 0:", stroke_0)
print("Número de registros com stroke 1:", stroke_1)
# Resposta: 26848 e 40287

'''
Pergunta 4: A partir do dataframe, crie uma tabela temporária usando df.createOrReplaceTempView('table') e a seguir use spark.sql para escrever 
uma consulta SQL que obtenha quantos pacientes tiveram derrame por tipo de trabalho (work_type). Quantos pacientes sofreram derrame e trabalhavam
respectivamente, no setor privado, de forma independente, no governo e quantas são crianças?
'''
data_df.createOrReplaceTempView('table')

spark.sql("SELECT * FROM table").show(10)  # Exibir as primeiras 10 linhas

numPrivateStrokes = spark.sql("SELECT * FROM TABLE  WHERE work_type = 'Private' AND stroke = '1'").count()
print('Número de pessoas do setor privado que tiveram derrame', numPrivateStrokes)

numSelfEmployedStrokes = spark.sql("SELECT * FROM TABLE  WHERE work_type = 'Self-employed' AND stroke = '1'").count()
print('Número de pessoas do setor independente que tiveram derrame', numSelfEmployedStrokes)

numGovtJobStrokes = spark.sql("SELECT * FROM TABLE  WHERE work_type = 'Govt_job' AND stroke = '1'").count()
print('Número de pessoas do setor público que tiveram derrame', numGovtJobStrokes)

numChildrenStrokes = spark.sql("SELECT * FROM TABLE  WHERE work_type = 'children' AND stroke = '1'").count()
print('Número de crianças que tiveram derrame', numChildrenStrokes)
# Resposta: 23711, 10807, 5164, 520

'''
Pergunta 5: Escreva uma consulta com spark.sql para determinar a proporção, por gênero, de participantes do estudo. A maioria dos participantes é:
'''
numMales = spark.sql("SELECT * FROM TABLE  WHERE gender = 'Male'").count()
print('Número de homens', numMales)

numFemales = spark.sql("SELECT * FROM TABLE  WHERE gender = 'Female'").count()
print('Número de mulheres', numFemales)
# Resposta: Feminina

'''
Pergunta 6: Escreva uma consulta com spark.sql para determinar quem tem mais probabilidade de sofrer derrame: 
hipertensos ou não-hipertensos. Você pode escrever uma consulta para cada grupo. A partir das probabilidades 
que você obteve, você conclui que:
'''
numHypertensionStrokes = spark.sql("SELECT * FROM TABLE  WHERE hypertension = 1 AND stroke = '1'").count()
print('Número de hipertensos que tiveram derrame', numHypertensionStrokes)

numNonHypertensionStrokes = spark.sql("SELECT * FROM TABLE  WHERE hypertension = 0 AND stroke = '1'").count()
print('Número de não hipertensos que tiveram derrame', numNonHypertensionStrokes)
# Resposta: ???

'''
Pergunta 7: Escreva uma consulta com spark.sql que determine o número de pessoas que sofreram derrame por idade. 
Com qual idade o maior número de pessoas do conjunto de dados sofreu derrame?
'''
resultado = spark.sql("""
    SELECT age, COUNT(*) AS num_derrame
    FROM table
    WHERE stroke = 1
    GROUP BY age
    ORDER BY num_derrame DESC
""")

# Mostrar o resultado
resultado.show()
# Obter a idade com o maior número de pessoas que sofreram derrame
idade_maior_derrame = resultado.collect()[0]['age']
num_maior_derrame = resultado.collect()[0]['num_derrame']
print("Idade com o maior número de derrames:", idade_maior_derrame)
print("Número de pessoas dessa idade que sofreram derrame:", num_maior_derrame)
# Resposta: 79 anos

'''
Pergunta 8: Usando a API de dataframes, determine quantas pessoas sofreram derrames após os 50 anos.
'''
numStrokes = data_df.where((data_df["stroke"] == 1) & (data_df["age"] > 50)).count()
print('Número de pessoas que tiveram derrame após os 50 anos', numStrokes)
# Respota: 28938

'''
Pergunta 9: Usando spark.sql, determine qual o nível médio de glicose para
pessoas que, respectivamente, sofreram e não sofreram derrame
'''
# Consulta SQL para obter o nível médio de glicose para pessoas que sofreram e não sofreram derrame
resultado = spark.sql("""
    SELECT stroke,
           AVG(avg_glucose_level) AS nivel_medio_glicose
    FROM table
    GROUP BY stroke
""")

# Exibir o resultado
resultado.show()

# Obter o nível médio de glicose para cada grupo
nivel_medio_glicose_sem_derrame = resultado.filter(resultado.stroke == 0).collect()[0]['nivel_medio_glicose']
nivel_medio_glicose_com_derrame = resultado.filter(resultado.stroke == 1).collect()[0]['nivel_medio_glicose']

print("Nível médio de glicose para pessoas que não sofreram derrame:", nivel_medio_glicose_sem_derrame)
print("Nível médio de glicose para pessoas que sofreram derrame:", nivel_medio_glicose_com_derrame)
# Resposta: 119 e 103

'''
Pergunta 10: Qual é o BMI (IMC = índice de massa corpórea) médio de quem sofreu e não sofreu derrame?

strokes = data_df.filter(data_df["stroke"] == 1)
strokeImcMean = strokes.agg({'bmi': 'mean'}).collect()[0][0]
print('IMC médio das pessoas que tiveram derrame', strokeImcMean)

nonStrokes = data_df.filter(data_df["stroke"] == 0)
nonStrokeImcMean = nonStrokes.agg({'bmi': 'mean'}).collect()[0][0]
print('IMC médio das pessoas que não tiveram derrame', nonStrokeImcMean)

gender_indexer = StringIndexer(inputCol='gender', outputCol='genderIndexer')
gender_indexer.fit(data_df).transform(data_df).select("age", "gender","genderIndexer").show(10)

gender_encoder = OneHotEncoder(inputCol='genderIndexer', outputCol='genderVector')

gender_indexer_model = gender_indexer.fit(data_df).transform(data_df)
# Resposta: 29,94 e 27,99

'''

'''
Pergunta 11: Crie um modelo de árvore de decisão que prevê a chance de derrame (stroke) a partir das variáveis contínuas/categóricas: 
idade, BMI, hipertensão, doença do coração, nível médio de glicose. Use o conteúdo da segunda aula interativa para criar e avaliar o modelo.
Qual a acurácia de um modelo construído?
'''
# Resposta: Menor que 75%

'''
Pergunta 12: Adicione ao modelo as variáveis categóricas: gênero e status de fumante. 
Use o conteúdo da aula interativa para lidar com as variáveis categóricas. A acurácia (qualidade) do modelo aumentou para:
'''
# Resposta: Acima de 80%

'''
Pergunta 13: Adicione ao modelo as variáveis categóricas: gênero e status de fumante. 
Use o conteúdo da aula interativa para lidar com as variáveis categóricas. Qual dessas variáveis é mais importante 
no modelo de árvore de decisão que você construiu?
'''
# Resposta: Status sobre fumo.

'''
Pergunta 14: Adicione ao modelo as variáveis categóricas: gênero e status de
fumante. Use o conteúdo da aula interativa para lidar com as variáveis categóricas. 
Qual a profundidade da árvore de decisão?
'''
# Resposta: Entre 2 e 7.

'''
Pergunta 15: Quantos nodos a árvore de decisão possui?
'''
# Resposta: Mais que 5