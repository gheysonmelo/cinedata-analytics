# CineData Analytics — Pipeline de Dados End-to-End

Projeto da disciplina de Engenharia de Dados (Rocket Lab 2026.2 — Visagio), implementando um
pipeline completo de ETL em **Databricks + PySpark + SQL**, seguindo a **Arquitetura Medalhão**
(Bronze → Silver → Gold), a partir de uma base combinada TMDB/IMDb entregue intencionalmente
suja e fragmentada.

## Estrutura do repositório

| Arquivo | Descrição |
|---|---|
| `00_Organizacao_do_Ambiente.ipynb` | Notebook de configuração compartilhada (catalog, schemas, landing path) — reaproveitado pelos demais via `%run`. |
| `01_Landing_to_Bronze.ipynb` | Ingestão bruta dos 5 CSVs + cotação do dólar (API do Banco Central) em Delta, modo Append. |
| `02_Bronze_to_Silver.ipynb` | Limpeza, tipagem, deduplicação e tradução das 7 tabelas Silver. |
| `03_Silver_To_Gold.ipynb` | Star Schema (fato + dimensões + bridges) e tabela de contexto para o assistente de IA. |
| `Analytics.ipynb` | As 6 consultas de negócio da Fase 4 do enunciado. |
| `job.yaml` | Definição exportada do Databricks Workflow (3 tasks encadeadas + agendamento). |
| `job_execucao_sucesso.png` | Print da execução bem-sucedida do Job, mostrando as dependências entre tasks. |

## Arquitetura

```
Landing (Volume)  →  Bronze (append, cópia fiel)  →  Silver (limpo, tipado, PT-BR)  →  Gold (Star Schema)
```

- **Bronze**: uma tabela Delta por arquivo de origem, sem nenhuma transformação de negócio, com coluna
  `ingestion_datetime`. Inclui `bronze.tb_cotacao_dolar`, obtida via API PTAX do Banco Central.
- **Silver**: 7 tabelas (`tb_info_filmes`, `tb_financeiro_filmes`, `tb_metricas_engajamento`,
  `tb_avaliacoes_usuarios`, `tb_generos`, `tb_pessoas_empresas`, `tb_cotacao_dolar`), com colunas
  em português, tipagem corrigida, deduplicação e as regras de negócio do enunciado.
- **Gold**: Star Schema (`dim_movies`, `dim_genres`, `dim_people`, `dim_companies`, `dim_reviews`,
  `fact_movies_performance` + 3 bridge tables) e `gold_genai_movies_context`, o documento textual
  consolidado que alimenta o Vector Search do time de IA.

## Orquestração

Databricks Workflow (`job.yaml`) com 3 tasks — `to_Bronze` → `to_Silver` → `to_Gold` — com
dependência explícita entre elas e agendamento configurado, simulando uma rotina de atualização
diária em produção.

## Decisões de engenharia e limitações conhecidas de dados

A base foi entregue intencionalmente suja; abaixo estão documentadas as decisões tomadas diante
de sujeiras que iam além do que o enunciado descrevia explicitamente, para transparência com quem
avaliar o projeto.

- **Coluna `idioma_original` (tb_info_filmes)**: além do já esperado, a coluna também sofria de
  *column shift* (nomes de pessoas, fragmentos de sinopse e até caminhos de imagem do TMDB
  misturados aos códigos de idioma). Tratada validando o formato `^[a-z]{2}$` (padrão ISO 639-1);
  qualquer valor fora disso vira `NULL`.
- **Nomes de idioma vazando em `cast`/`directors`/`writers`** (tb_pessoas_empresas): nomes de
  idioma por extenso (ex.: "English") apareciam ocasionalmente como se fossem diretor/roteirista/ator,
  também por column shift no arquivo `credits_and_tags`. Adicionados à lista de tokens excluídos
  na função de extração de entidades.
- **Gêneros (`tb_generos`)**: a sujeira de column shift nessa coluna era severa (mais de 2.700
  valores distintos de lixo — caminhos de imagem, fragmentos de sinopse, números de popularidade
  vazados). Resolvida com uma **whitelist** dos 19 gêneros oficiais do TMDB, em vez de tentar
  descrever o formato do lixo via regex (blacklist), abordagem mais robusta dado um domínio fechado
  e conhecido.
- **Pessoas/empresas (`tb_pessoas_empresas`)**: como nome de pessoa/empresa é um domínio aberto
  (sem whitelist possível), a limpeza usou heurísticas: tamanho máximo de 60 caracteres, não terminar
  em ponto final, começar com maiúscula/número. Validado empiricamente que aumentar o limite de
  tamanho não melhora a relação sinal/ruído (mistura ~50/50 de nome legítimo longo vs. lixo acima de
  60 caracteres). Uma fração residual de sujeira ambígua permanece — inerente a esse tipo de dado sem
  recorrer a NLP, o que foi considerado fora do escopo da atividade.
- **Orçamento/receita (`tb_financeiro_filmes`)**: ~92% dos valores de orçamento/receita viraram
  `NULL` na Silver. Isso não é um bug — é uma característica real da base: mais de 85% dos filmes
  têm `budget`/`revenue` = `"0"` na origem (comum em datasets TMDB/IMDb, que cobrem também produções
  pequenas/obscuras sem dado financeiro reportado). Validado categorizando 100% dos valores brutos
  antes de aceitar essa taxa como correta.
- **Outlier de margem de lucro**: o filme com `id_filme=787459` tem orçamento de `$128` (positivo,
  mas implausível) e uma margem de lucro resultante de ~13.383.094%. Mantido como está — o PDF só
  define zero/negativo como critério de ausência para orçamento/receita, não valores implausivelmente
  baixos, e alterar essa regra seria extrapolar o escopo definido.
- **Outlier de popularidade**: 3 dos 5 filmes mais "populares" (Fase de Analytics, pergunta 2) têm
  popularidade suspeitosamente redonda (`2018`, `2019`, `2020` — parecem anos, não índices de
  popularidade). Possível resíduo de column shift não capturado. Mantido como está, pois o enunciado
  define apenas que valores **negativos** de popularidade devem virar ausentes, sem definir limite
  superior.
- **Ingestão da API do Banco Central**: o ambiente Databricks Free Edition usado inicialmente
  restringe egress de rede a uma allowlist de domínios (limitação documentada da própria plataforma,
  não do enunciado), impedindo a chamada direta à API do Bacen a partir do compute serverless. O
  problema foi contornado utilizando uma conta com acesso liberado; o notebook `Landing_to_Bronze`
  reflete a chamada real e funcional à API.

## Perguntas de negócio (Analytics.ipynb)

As 6 consultas exigidas (receita total, top 5 popularidade, filmes por gênero, top 10 receita com
`RANK()`, ator com mais participações nos últimos 2 anos, produtora com maior lucro nos últimos 5
anos) estão implementadas em `Analytics.ipynb`, consumindo exclusivamente as tabelas da camada Gold.
