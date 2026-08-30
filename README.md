# Tech Challenge - Fase 2
## Pipeline Híbrido para Análise da Alfabetização no Brasil

Projeto da Fase 2 do curso, que consiste em construir uma pipeline de dados na nuvem (usamos Azure Databricks) para integrar dados públicos sobre alfabetização infantil no Brasil, usando a base do Indicador Criança Alfabetizada disponível na plataforma Base dos Dados.

## Contexto

O Compromisso Nacional Criança Alfabetizada é uma política pública que busca garantir que todas as crianças brasileiras estejam alfabetizadas até o fim do 2º ano do ensino fundamental. Em 2023 o INEP fez uma pesquisa (Alfabetiza Brasil) e definiu que uma criança é considerada alfabetizada quando atinge 743 pontos na escala de proficiência do Saeb. A partir disso foi criado o Indicador Criança Alfabetizada, que mostra o percentual de alunos que atingem esse patamar, com meta de chegar a 100% até 2030.

Pra entender melhor o que influencia esse indicador, é preciso juntar várias bases diferentes: metas nacionais, estaduais e municipais, dados de território, microdados dos alunos e os resultados em si. É isso que essa pipeline faz.

## Arquitetura

Seguimos o modelo de arquitetura medalhão (bronze, silver, gold), com ingestão batch + streaming simulado.


### Bronze

Dados brutos, sem transformação. Ingerimos as tabelas de UF, município, as três tabelas de meta (Brasil/UF/município), a tabela de alunos (que tem quase 3,8 milhões de linhas) e uma tabela de dicionário que usamos depois pra decodificar alguns códigos. Também trouxemos a tabela de diretório de municípios (nome, região, coordenadas) pra poder enriquecer os dados depois.

### Silver

Aqui limpamos os dados: removemos duplicatas, padronizamos texto (tirando espaço em branco, deixando sigla de UF em maiúsculo), e um problema que demos bastante trabalho pra resolver foi que a coluna `rede` vinha como número em algumas tabelas (0, 2, 3, 5...) e como texto em outras ("Municipal", etc). Resolvemos usando a própria tabela de dicionário da Base dos Dados pra fazer esse de-para, em vez de simplesmente chutar o significado dos números.

Também fizemos validação de chave (conferir se todo `id_municipio` das tabelas de resultado existe na tabela de diretório) e investigamos os casos de dado ausente. Por exemplo, descobrimos que 148 municípios nunca tiveram meta municipal publicada, concentrados principalmente em SC e RS - provavelmente porque são municípios com rede municipal pequena demais pra ter uma meta calculada com significância estatística. Em vez de simplesmente deixar essas linhas com nulo sem explicação, criamos uma coluna (`possui_meta_municipal`) que marca isso de forma explícita.

Também recalculamos a taxa de alfabetização a partir dos microdados dos alunos (usando o peso amostral) só pra conferir se batia com a taxa oficial - bateu na maioria dos casos, com uma pequena divergência esperada de arredondamento.

### Gold

Três tabelas principais, que são os três exemplos que o próprio enunciado do desafio cita:

- `indicador_alfabetizacao_municipio`: indicador por município, já com a distância até a meta de 2030 calculada
- `comparativo_meta_resultado`: compara meta com resultado, juntando Brasil, UF e município numa tabela só
- `evolucao_temporal_indicador`: série histórica (o que já aconteceu) junto com as metas futuras, no mesmo eixo do tempo

## Sobre o streaming

Como a Base dos Dados não tem uma fonte de dados em tempo real (é tudo publicado em lote), a parte de streaming do projeto é simulada: um notebook pega uma amostra da tabela de alunos e "solta" um lote de dados de tempos em tempos, como se fossem novas medições chegando. Outro processo (usando Auto Loader do Databricks) fica processando isso conforme chega, usando checkpoint pra não processar o mesmo arquivo duas vezes.

O motivo de simular em vez de usar um serviço de streaming de verdade (tipo Kafka ou Event Hub) é custo: pro volume de dados e pro propósito do projeto, não faria sentido manter uma infraestrutura de streaming rodando o tempo todo. O Auto Loader processando arquivos em intervalos (usamos o trigger `availableNow`, que processa tudo que tem e desliga) já demonstra o mesmo padrão de ingestão em tempo quase real, sem esse custo.

As análises da camada gold usam só os dados do batch (a tabela completa de alunos), não o streaming - o streaming é só pra mostrar que o pipeline consegue lidar com esse tipo de ingestão.

## Tecnologias usadas

- **Azure Databricks**: onde roda o processamento (Spark) e a orquestração dos jobs
- **Delta Lake**: formato de armazenamento das tabelas, dá versionamento e transação
- **Unity Catalog / Volumes**: usamos pra guardar os arquivos da landing zone do streaming e os checkpoints (o DBFS antigo vem desabilitado por padrão em workspace novo)
- **google-cloud-bigquery (Python)**: pra consultar a Base dos Dados. Testamos o pacote `basedosdados` primeiro mas ele tenta abrir um navegador pra autenticar, o que não funciona dentro do Databricks - então usamos a biblioteca do Google direto, passando a credencial de forma explícita
- **Databricks Secret Scopes**: pra guardar a chave de acesso do Google Cloud sem deixar ela em texto plano em nenhum arquivo do repositório

## Decisões e trade-offs

**Batch vs streaming**: como já expliquei acima, usamos batch pra tudo que é carga real de análise, e streaming simulado só como prova de conceito.

**Data lake vs data warehouse**: optamos por manter tudo em Delta Lake (data lake) em vez de subir pra um data warehouse tradicional, porque os dados são bem heterogêneos (tabela agregada, microdados, dicionário) e o Delta já dá boa parte das garantias de um warehouse sem precisar de uma modelagem mais rígida.

**Custo vs performance**: particionamos a tabela de alunos por ano, já que a maioria das análises filtra por ano. Também evitamos rodar a extração do BigQuery mais de uma vez por tabela (fizemos um select, salvamos, e trabalhamos em cima do que já foi salvo), pra não gerar custo de consulta repetido no Google Cloud.

## Qualidade de dados

Fizemos um script (`scripts/data_quality_checks.py`) com 4 tipos de verificação: duplicidade, valores ausentes, chave de relacionamento (município órfão) e consistência entre tabelas (taxa oficial vs recalculada).

Na última execução deu 6 verificações OK e 3 com aviso - mas as 3 já eram esperadas, porque investigamos cada uma:
- ~310 municípios sem meta municipal em uma das colunas (os mesmos 148 municípios de SC/RS que mencionei antes, contando as duas colunas de meta separadamente)
- 57 linhas (0,24%) de diferença entre a taxa oficial e a recalculada, dentro de uma margem aceitável de arredondamento

## Monitoramento e custo (FinOps)

O resultado do script de qualidade fica salvo como uma tabela Delta (`gold.data_quality_report`), então dá pra acompanhar isso ao longo do tempo, não só rodar uma vez e esquecer.

Pra controlar custo: os dados ficam particionados por ano, o streaming roda como job agendado (não fica ligado direto, o que custaria mais), e não usamos nenhum serviço de mensageria pago tipo Event Hub - o Auto Loader do próprio Databricks já resolve. Os jobs agendados também podem ser pausados quando não estamos trabalhando ativamente no projeto.

## Aplicação em IA

A camada gold já foi pensada pra servir de base pra alguns usos de IA/ML no futuro:

- Modelo pra prever quais municípios têm risco de não bater a meta de 2030, usando as variáveis territoriais que trouxemos (região, coordenadas, etc)
- Análise de desigualdade educacional comparando região por região, usando a tabela de comparativo
- Modelo de série temporal pra projetar a evolução do indicador e ajudar a priorizar onde investir

## Estrutura do repositório

```
dashboards/
  media_estado.png
  media_regiao.png
notebooks/
  bronze/
    batch_ingestao.ipynb
    streaming_alunos.ipynb
  silver/
    tratamento_integracao.ipynb
  gold/
    gold_transform.ipynb
scripts/
  data_quality_checks.py
README.md
```

## Como rodar

1. Ter um workspace Databricks no Azure com Unity Catalog habilitado
2. Ter um projeto no Google Cloud com uma service account (papel de BigQuery Job User + Data Viewer) e a chave salva num secret scope do Databricks
3. Rodar os notebooks na ordem: bronze (batch e depois streaming) -> silver -> gold
4. Rodar o script de qualidade de dados depois da silver
