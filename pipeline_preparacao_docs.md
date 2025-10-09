# Pipeline de Preparação de Dados - Base VOD

## Contexto Geral

Este pipeline faz parte de um projeto maior de deduplicação e matching de catálogos de VOD (Video on Demand). O objetivo final é construir uma base consolidada anual (t1) com:
- Obras deduplicadas internamente
- Enriquecimento via matching com base Ancine (CPBs)
- Rastreamento temporal (colunas IN/OUT por obra)
- Integração com base histórica do ano anterior (t0)

**Situação atual:**
- **Base t0** (ano passado): Já existe, em formato XLSX, já possui IN/OUT e matching com CPBs
- **Base t1** (este ano): Em construção - estes notebooks preparam os dados históricos
- **Snapshots semanais**: Julho 2024 - Agosto 2025

**Importante:** Os notebooks 1-3 documentados aqui eram originalmente uma preparação para processos de deduplicação com Splink (notebooks 4-5, não documentados). Essa abordagem foi abandonada devido a limitações técnicas. A solução atual de matching é determinística e está documentada em `pipeline_cpb_uid.md`, trabalhando apenas com o snapshot mais recente.

---

## Referência Rápida: Artefatos Principais

### Arquivos de Input

| Arquivo | Formato | Localização | Descrição | Atualização |
|---------|---------|-------------|-----------|-------------|
| `BB Media - YYYY-MM-DD.xlsx` | Excel | `dados_catalogo/raw_xlsx/` | Snapshots semanais de catálogos | Semanal (manual) |
| `mapeamento_colunas.json` | JSON | Raiz do projeto | Schema de importação (45 colunas) | Ocasional |
| `plataformas_controle.xlsx` | Excel | Raiz do projeto | Lista de plataformas a processar | Quando aparece nova plataforma |

### Arquivos Intermediários

| Arquivo | Formato | Localização | Linhas | Colunas | Gerado por |
|---------|---------|-------------|--------|---------|------------|
| `BB_Media_YYYY-MM-DD.parquet` | Parquet | `dados_catalogo/staging/` | ~80-90K | 46 | Notebook 1 |
| `manifesto_xlsx.json` | JSON | `dados_catalogo/` | N/A | N/A | Notebook 1 |
| `relatorio_conversao_TIMESTAMP.csv` | CSV | `dados_catalogo/logs/` | Variável | 7 | Notebook 1 |
| `relatorio_timeline_plataformas_TIMESTAMP.csv` | CSV | `dados_catalogo/processed/` | ~134 | ~57 | Notebook 2 |
| `relatorio_timeline_plataformas_TIMESTAMP.xlsx` | Excel | `dados_catalogo/processed/` | ~134 | ~57 | Notebook 2 |
| `dados_consolidados_filtrados.parquet` | Parquet | `dados_catalogo/processed/` | ~2-3M | 46 | Notebook 2 |

### Arquivos Finais (Outputs)

| Arquivo | Formato | Localização | Linhas | Colunas | Gerado por | Status Uso |
|---------|---------|-------------|--------|---------|------------|------------|
| `tabela_dimensao_limpa_para_splink.parquet` | Parquet | `dados_catalogo/processed/` | ~225K | 15 | Notebook 3 | ⚠️ Não usado |
| `tabela_dimensao_com_plataformas.parquet` | Parquet | `dados_catalogo/processed/` | ~225K | 16 | Notebook 3 | ⚠️ Não usado |

---

## Glossário Técnico

### Termos de Negócio

| Termo | Definição |
|-------|-----------|
| **VOD** | Video on Demand - Serviços de streaming de vídeo |
| **Snapshot semanal** | Fotografia do catálogo de uma plataforma em determinada data |
| **Crunch temporal** | Processo de eliminar duplicatas históricas, mantendo apenas o estado mais recente |
| **Gap de tolerância** | Período de ausência (22 dias) que não caracteriza saída definitiva de catálogo |
| **Base t0** | Base consolidada do ano anterior (referência histórica) |
| **Base t1** | Base em construção para o ano atual |
| **IN/OUT** | Datas de entrada e saída de obras do catálogo |

### Termos Técnicos

| Termo | Definição |
|-------|-----------|
| **BB_UID** | Binge Box Unique Identifier - PK da obra no sistema de origem |
| **data_ref** | Data de referência do snapshot (formato YYYY-MM-DD) |
| **Perfil único** | Registro único por obra após eliminação de duplicatas temporais |
| **Duplicata temporal** | Mesma obra aparecendo em múltiplos snapshots semanais |
| **DuckDB** | Sistema de banco de dados analítico otimizado para queries em arquivos (CSV, Parquet) |
| **Parquet** | Formato de arquivo colunar comprimido, otimizado para análise de dados |
| **Manifesto** | Arquivo de controle que registra arquivos já processados |

### Campos-Chave

| Campo | Tipo | Descrição | Origem |
|-------|------|-----------|--------|
| `BB_UID` | string | Identificador único da obra | Binge Box (fornecedor) |
| `Platform_Name` | string | Nome da plataforma de streaming | Binge Box |
| `Type` | string | Tipo de conteúdo ("Movie" ou "Series") | Binge Box |
| `BB_Title` | string | Título normalizado pela Binge Box | Binge Box (normalizado) |
| `BB_Year` | Int64 | Ano de lançamento | Binge Box |
| `BB_Directors` | string | Diretores (comma-separated) | Binge Box |
| `BB_Cast` | string | Elenco principal (comma-separated) | Binge Box |
| `IMDb_ID` | string | Identificador IMDb (formato: tt0123456) | Binge Box/IMDb |
| `TMDB_ID` | float | Identificador The Movie Database | Binge Box/TMDB |
| `data_ref` | string | Data de referência do snapshot | Criado pelo pipeline (Notebook 1) |

---

## Detalhes Técnicos de Implementação

### DuckDB - Otimização de Memória

**Por que usar DuckDB?**
- Queries SQL diretas em arquivos Parquet/CSV sem carregar em memória
- Processamento vetorizado (SIMD) para operações analíticas rápidas
- Suporte nativo a tipos complexos (arrays, structs)
- Integração transparente com Pandas

**Exemplo de uso no pipeline:**

```python
# ❌ Abordagem ingênua (alto consumo de memória):
df = pd.read_parquet('staging/*.parquet')  # Carrega ~4M linhas na RAM
df_filtrado = df[df['Platform_Name'].isin(plataformas)]
df_filtrado.to_parquet('output.parquet')

# ✅ Abordagem otimizada (DuckDB):
con = duckdb.connect(database=':memory:')
con.execute(f"""
    COPY (
        SELECT * FROM read_parquet('staging/*.parquet')
        WHERE Platform_Name IN {tuple(plataformas)}
    ) TO 'output.parquet' (FORMAT PARQUET, COMPRESSION 'snappy')
""")
con.close()
# Processamento em streaming, sem carregar tudo na RAM
```

### Parquet - Formato de Armazenamento

**Características:**
- **Colunar**: Dados organizados por coluna (não por linha)
  - Benefício: Queries que leem poucas colunas são muito rápidas
- **Compressão**: Snappy (rápida) ou Gzip (maior compressão)
  - Redução típica: 70-80% do tamanho original
- **Schema integrado**: Metadados de tipos embutidos no arquivo
- **Particionamento**: Suporta divisão por valores (ex: por data_ref)

**Comparação de tamanhos:**
```
Arquivo XLSX original:     ~150 MB
Parquet (Snappy):          ~15-25 MB  (compressão ~90%)
CSV equivalente:           ~200 MB
```

### Multiprocessing - Conversão Paralela

**Implementação no Notebook 1:**
```python
from multiprocessing import Pool, cpu_count

num_processos = cpu_count()  # Ex: 8 núcleos
with Pool(processes=num_processos) as pool:
    resultados = pool.imap_unordered(processar_arquivo, lista_de_arquivos)
```

**Ganho de performance:**
- 1 núcleo: ~4 min para 57 arquivos
- 8 núcleos: ~35 segundos (speedup ~6.8x)

**Trade-off:**
- Usa mais memória (um processo por núcleo)
- Ideal para CPU-bound tasks (leitura/escrita de arquivos)

## Objetivo dos Notebooks 1-3

Estes notebooks preparam o histórico semanal de catálogos, realizando:
1. Conversão de formato (XLSX → Parquet eficiente)
2. Validação de qualidade e consistência temporal
3. Consolidação e filtragem de plataformas relevantes
4. Preparação de tabela dimensão com perfis únicos

**O que falta (a ser implementado):**
- Crunch temporal para gerar colunas IN/OUT com tolerância de 22 dias
- Integração com base t0
- Expansão do XLSX mais recente com obras que saíram de catálogo

---

## Estrutura de Diretórios

```
projeto/
├── dados_catalogo/
│   ├── raw_xlsx/              # Arquivos Excel originais (input)
│   ├── staging/               # Parquets convertidos (intermediário)
│   ├── processed/             # Dados consolidados e limpos (output)
│   └── logs/                  # Relatórios de execução
├── mapeamento_colunas.json    # Configuração de colunas a importar
├── plataformas_controle.xlsx  # Lista de plataformas válidas (input manual)
├── processador.py             # Módulo auxiliar de conversão
├── pipeline_cpb_uid.md        # Documentação do matching atual (SEPARADO)
└── [notebooks 1, 1.1, 2, 3]   # Pipeline de preparação histórica
```

**⚠️ Importante:** Os outputs dos notebooks 1-3 **não estão sendo usados** no processo de matching atual (documentado em `pipeline_cpb_uid.md`). A solução atual trabalha diretamente com o XLSX mais recente, sem usar o histórico em Parquet.

---

## Fluxo do Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                         INPUTS                                   │
├─────────────────────────────────────────────────────────────────┤
│  • raw_xlsx/*.xlsx (57 arquivos, ~150MB cada)                  │
│  • mapeamento_colunas.json (45 colunas)                         │
│  • plataformas_controle.xlsx (134 plataformas, 89 com USAR='S')│
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  [NOTEBOOK 1] Conversão XLSX → Parquet                          │
│  • Multiprocessing (8 núcleos)                                  │
│  • 57 arquivos → 57 Parquets                                    │
│  • Adiciona campo data_ref                                      │
│  • Tempo: ~35 segundos                                          │
├─────────────────────────────────────────────────────────────────┤
│  OUTPUT: staging/*.parquet                                      │
│  • ~4-5M registros totais                                       │
│  • 46 colunas (45 + data_ref)                                   │
│  • ~1.2 GB total (comprimido)                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
          ┌───────────────────┴───────────────────┐
          ↓                                       ↓
┌─────────────────────────┐          ┌───────────────────────────┐
│ [NOTEBOOK 1.1] AUXILIAR │          │ [NOTEBOOK 2] Validação    │
│ Análise de Plataformas  │          │ e Filtragem               │
│                         │          │                           │
│ • Agregações DuckDB     │          │ • Análise temporal        │
│ • Média registros/sem   │          │ • Filtro de plataformas   │
│                         │          │ • DuckDB streaming        │
├─────────────────────────┤          ├───────────────────────────┤
│ OUTPUT:                 │          │ OUTPUT:                   │
│ relatorio_plataformas   │          │ dados_consolidados_       │
│ .csv                    │          │ filtrados.parquet         │
│ • 134 plataformas       │          │ • ~2-3M registros         │
│                         │          │ • 46 colunas              │
└─────────────────────────┘          │ • ~800 MB                 │
                                     └───────────────────────────┘
                                                 ↓
                              ┌──────────────────┴──────────────────┐
                              ↓                                     ↓
              ┌───────────────────────────────┐   ┌─────────────────────────────┐
              │ [NOTEBOOK 3] Preparação       │   │ Análise Exploratória        │
              │ Tabela Dimensão               │   │ • Completude (% não-nulos)  │
              │                               │   │ • Estabilidade (% mudanças) │
              │ • Crunch temporal             │   └─────────────────────────────┘
              │ • Elimina duplicatas          │
              │ • 2.5M → 225K registros       │
              │ • Normaliza strings           │
              │ • Ordena listas               │
              ├───────────────────────────────┤
              │ OUTPUT PRINCIPAL:             │
              │ tabela_dimensao_limpa_        │
              │ para_splink.parquet           │
              │ • 225K registros (únicos)     │
              │ • 15 colunas                  │
              │ • ~45 MB                      │
              │                               │
              │ OUTPUT ENRIQUECIDO:           │
              │ tabela_dimensao_com_          │
              │ plataformas.parquet           │
              │ • 225K registros              │
              │ • 16 colunas (+Plataformas)   │
              │ • ~46 MB                      │
              └───────────────────────────────┘
                              ↓
              ⚠️  Outputs preparados mas NÃO usados
              ⚠️  Tentativa com Splink abandonada
              ⚠️  Solução atual: matching determinístico
              ⚠️  (documentado em pipeline_cpb_uid.md)
```

**Legenda de volumes:**
- **4-5M registros** = Todos os snapshots × todas as plataformas × todas as semanas
- **2-3M registros** = Apenas plataformas selecionadas × todas as semanas
- **225K registros** = 1 perfil por obra (sem duplicatas temporais)

---

## Exemplos de Uso dos Outputs

### Carregar Tabela Dimensão Principal

```python
import pandas as pd

# Carrega a tabela de perfis únicos
df = pd.read_parquet('dados_catalogo/processed/tabela_dimensao_limpa_para_splink.parquet')

print(f"Total de obras únicas: {len(df):,}")
# Output: Total de obras únicas: 225,117

# Distribuição por tipo
print(df['Type'].value_counts())
# Output:
# Movie     ~150K
# Series    ~70K
# Tv Show   ~5K

# Obras com IMDb ID
print(f"Obras com IMDb: {df['IMDb_ID'].notna().sum():,} ({df['IMDb_ID'].notna().mean():.1%})")
# Output: Obras com IMDb: 136,651 (60.7%)
```

### Consultar Histórico Temporal (DuckDB)

```python
import duckdb

con = duckdb.connect(database=':memory:')

# Quantas vezes cada obra apareceu no histórico?
query = """
SELECT 
    BB_UID,
    BB_Title,
    COUNT(DISTINCT data_ref) as semanas_presentes,
    MIN(data_ref) as primeira_semana,
    MAX(data_ref) as ultima_semana
FROM read_parquet('dados_catalogo/processed/dados_consolidados_filtrados.parquet')
GROUP BY BB_UID, BB_Title
HAVING COUNT(DISTINCT data_ref) > 1
ORDER BY semanas_presentes DESC
LIMIT 10
"""

df_historico = con.execute(query).fetch_df()
print(df_historico)

# Output (exemplo):
#   BB_UID          BB_Title           semanas_presentes  primeira_semana  ultima_semana
#   abc123...       Breaking Bad       57                 2024-07-11       2025-08-28
#   xyz789...       The Office         57                 2024-07-11       2025-08-28
```

### Analisar Disponibilidade por Plataforma

```python
# Carrega versão com agregação de plataformas
df_plat = pd.read_parquet('dados_catalogo/processed/tabela_dimensao_com_plataformas.parquet')

# Quantas plataformas por obra?
df_plat['num_plataformas'] = df_plat['Plataformas'].str.count(',') + 1
df_plat['num_plataformas'] = df_plat['num_plataformas'].fillna(0).astype(int)

print(df_plat['num_plataformas'].value_counts().sort_index())
# Output:
# 1 plataforma:  ~180K obras (80%) - exclusivas
# 2 plataformas: ~35K obras (15%)
# 3+ plataformas: ~10K obras (5%) - amplamente disponíveis

# Obras em mais de 5 plataformas
df_multiplas = df_plat[df_plat['num_plataformas'] >= 5].sort_values('num_plataformas', ascending=False)
print(df_multiplas[['BB_Title', 'BB_Year', 'Plataformas', 'num_plataformas']].head(10))
```

### Validar Normalização de Strings

```python
# Verificar se normalização foi aplicada
print(df['BB_Title'].head(10))
# Output esperado: tudo em lowercase, sem acentos
# dark
# encanto
# friends
# breaking bad
# ...

# Verificar se listas foram ordenadas
import ast

def verificar_ordenacao(lista_str):
    if pd.isna(lista_str):
        return True
    nomes = lista_str.split(',')
    return nomes == sorted(nomes)

print(f"% de BB_Cast ordenado: {df['BB_Cast'].apply(verificar_ordenacao).mean():.1%}")
# Output esperado: 100.0%
```

## NOTEBOOK 1: Conversão XLSX → Parquet

### Arquivo: `1_Conversão_xlsx-parquet_otimizado.ipynb`

### Objetivo
Converter arquivos Excel semanais para formato Parquet otimizado, aplicando mapeamento de colunas e processamento paralelo.

### Inputs
- **Arquivos**: `dados_catalogo/raw_xlsx/*.xlsx`
  - Nomenclatura: `BB Media - YYYY-MM-DD.xlsx` (data = semana de referência)
- **Configuração**: `mapeamento_colunas.json`
  - Define quais colunas importar de cada arquivo
  - Evita carregar dados desnecessários

### Dependências
- **Módulo externo**: `processador.py` (funções de conversão)
- Bibliotecas: pandas, openpyxl, pyarrow, pathlib, multiprocessing, tqdm

### Processamento
1. Descobre arquivos `.xlsx` em `raw_xlsx/`
2. Verifica manifesto para evitar reprocessamento
3. **Processamento paralelo** (utiliza todos os núcleos CPU disponíveis)
4. Para cada arquivo:
   - Lê apenas colunas mapeadas
   - Extrai `data_ref` do nome do arquivo (formato YYYY-MM-DD)
   - Salva como Parquet com compressão Snappy

### Outputs

#### Parquets Semanais
**Localização:** `dados_catalogo/staging/BB_Media_YYYY-MM-DD.parquet`

**Esquema:** 46 colunas (45 do mapeamento + `data_ref`)

**Coluna adicional criada:**
- `data_ref` (string, formato 'YYYY-MM-DD'): Extraída do nome do arquivo, representa a semana de referência do snapshot

**Exemplo de nomenclatura:**
```
BB_Media_2024-07-11.parquet  → data_ref = '2024-07-11'
BB_Media_2024-08-28.parquet  → data_ref = '2024-08-28'
```

**Formato técnico:**
- Engine: PyArrow
- Compressão: Snappy
- Tamanho médio: 15-25 MB por arquivo (comprimido)
- Linhas por arquivo: 80K-90K registros (varia por semana)

**Exemplo de registros:**
```
BB_UID                              | Platform_Name | BB_Title           | Type   | data_ref
------------------------------------|---------------|--------------------|--------|------------
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6 | Netflix       | Stranger Things    | Series | 2024-07-11
q9r8s7t6u5v4w3x2y1z0a9b8c7d6e5f4 | Disney+       | The Mandalorian    | Series | 2024-07-11
```

#### Manifesto de Controle
**Arquivo:** `dados_catalogo/manifesto_xlsx.json` (detalhado acima)

#### Relatório de Conversão
**Localização:** `dados_catalogo/logs/relatorio_conversao_YYYY-MM-DD_HH-MM-SS.csv`

**Esquema:**
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `arquivo` | string | Nome do arquivo XLSX processado |
| `status` | string | 'SUCESSO' ou 'ERRO' |
| `timestamp` | datetime | Momento da conversão |
| `linhas` | int | Total de registros convertidos |
| `colunas` | int | Total de colunas importadas |
| `colunas_nao_mapeadas` | list[string] | Colunas encontradas mas não no JSON |
| `detalhes` | string | Mensagem de erro (se status = ERRO) |

**Exemplo:**
```csv
arquivo;status;timestamp;linhas;colunas;colunas_nao_mapeadas;detalhes
BB Media - 2024-07-11.xlsx;SUCESSO;2025-01-15 14:32:10;82340;45;['BB Age', 'Platform Rating'];
BB Media - 2024-07-18.xlsx;ERRO;2025-01-15 14:33:22;0;0;[];File not found
```

### DataFrames Intermediários

**Nenhum DataFrame intermediário relevante** - o processamento é direto de XLSX para Parquet via função do módulo `processador.py`.

---

## NOTEBOOK 1.1: Histórico de Plataformas (AUXILIAR)

### Arquivo: `1_1historico_de_plataformas.ipynb`

### Objetivo
Análise exploratória do histórico de plataformas presentes nos dados. **Não é essencial para o fluxo principal** - serve como ferramenta de validação e entendimento dos dados.

### Input
- **Parquets**: `dados_catalogo/staging/*.parquet`

### Dependências
- Bibliotecas: pandas, pathlib, duckdb

### Processamento
- Usa DuckDB para agregação rápida
- Para cada plataforma, calcula:
  - Total de registros
  - Semanas presentes
  - Primeira e última aparição
  - **Média de registros por semana** (métrica principal)

### Output
- **Relatório**: `relatorio_plataformas.csv`
  - Ordenado por média de registros/semana (plataformas mais ativas primeiro)
  - Formato Excel-BR (sep=';', decimal=',')

### Quando Usar
- Após rodar o Notebook 1
- Para entender quais plataformas têm dados mais consistentes
- Para identificar plataformas com gaps ou sazonalidade
- **Pode rodar a qualquer momento**, não bloqueia o fluxo principal

---

## NOTEBOOK 2: Validação e Filtragem

### Arquivo: `2_validacao_e_filtragem-Copy1.ipynb`

### Objetivo
Validar dados históricos, identificar mudanças semanais e filtrar apenas plataformas de interesse, consolidando em um único arquivo.

### Inputs
- **Parquets**: `dados_catalogo/staging/*.parquet`
- **Controle Manual**: `plataformas_controle.xlsx`
  - Colunas obrigatórias: `Plataforma`, `USAR`
  - `USAR = 'S'` → plataforma será mantida
  - `USAR = 'N'` → plataforma será excluída

### Dependências
- Bibliotecas: pandas, pathlib, duckdb, openpyxl, datetime, IPython.display

### Processamento

#### Bloco 1: Leitura do Controle
- Carrega `plataformas_controle.xlsx`
- Valida presença das colunas obrigatórias (`Plataforma`, `USAR`)
- Cria lista Python: `PLATAFORMAS_PARA_MANTER = ['Netflix', 'Disney+', ...]`
- Se validação falhar, pipeline não prossegue (flag `plataformas_controle_ok = False`)

#### Bloco 2: Relatório de Validação

**DataFrames criados:**

1. **df_timeline_raw**
   - Query SQL agregando registros por `(data_ref, Platform_Name)`
   - Colunas: `data_ref`, `Platform_Name`, `total_registros`
   - Exemplo:
   ```
   data_ref   | Platform_Name | total_registros
   -----------|---------------|----------------
   2024-07-11 | Netflix       | 8432
   2024-07-11 | Disney+       | 3210
   2024-07-18 | Netflix       | 8501
   ```

2. **df_timeline_pivot**
   - Tabela pivot de `df_timeline_raw`
   - Index: `Platform_Name`
   - Colunas: Todas as datas (`data_ref`) ordenadas cronologicamente
   - Valores: `total_registros` (int, preenchido com 0 onde não há dados)
   - Exemplo:
   ```
                    | 2024-07-11 | 2024-07-18 | 2024-07-25 | ...
   -----------------|------------|------------|------------|----
   Netflix          | 8432       | 8501       | 8390       | ...
   Disney+          | 3210       | 3198       | 3245       | ...
   Amazon Prime     | 12450      | 0          | 12890      | ... (gap na semana 18)
   Pluto TV         | 4532       | 4621       | 4580       | ...
   ```

**Análise semana a semana:**

Para cada semana (coluna do pivot), o notebook calcula:

1. **Novas Plataformas** (`entradas`)
   - Lógica: `registros_atuais > 0 AND registros_anteriores == 0 AND nunca_apareceu_antes`
   - DataFrame temporário: `[{'Plataforma': str, 'Registros': int}]`

2. **Plataformas que Retornaram** (`gaps`)
   - Lógica: `registros_atuais > 0 AND registros_anteriores == 0 AND já_apareceu_antes`
   - DataFrame temporário: `[{'Plataforma': str, 'Registros Atuais': int}]`

3. **Plataformas que Saíram** (`saidas`)
   - Lógica: `registros_anteriores > 0 AND registros_atuais == 0`
   - DataFrame temporário: `[{'Plataforma': str, 'Registros Anteriores': int}]`

4. **Variações de Catálogo** (`variacoes`)
   - Lógica: `registros_anteriores > 0 AND registros_atuais > 0 AND |variacao| > 5%`
   - Cálculo: `variacao = (registros_atuais - registros_anteriores) / registros_anteriores`
   - DataFrame temporário: `[{'Plataforma': str, 'Variação': float, 'Registros Anteriores': int, 'Registros Atuais': int}]`

**Outputs do Bloco 2:**

1. **Tabela Pivot Completa**
   - `dados_catalogo/processed/relatorio_timeline_plataformas_TIMESTAMP.csv`
   - `dados_catalogo/processed/relatorio_timeline_plataformas_TIMESTAMP.xlsx`
   - Formato: Plataformas × Semanas, valores = total de registros
   - Útil para análise visual em Excel (identificar padrões, gaps, crescimento)

2. **Relatório Visual no Notebook**
   - Renderizado em HTML com formatação condicional
   - Barras de progresso, gradientes de cor, tabelas estilizadas
   - Seções por semana mostrando mudanças (entradas/saídas/variações)

#### Bloco 3: Consolidação e Filtragem Final

**Query SQL executada:**
```sql
COPY (
    SELECT *
    FROM read_parquet('dados_catalogo/staging/*.parquet', union_by_name=true)
    WHERE Platform_Name IN ('Netflix', 'Disney+', 'Amazon Prime Video', ...)
) TO 'dados_catalogo/processed/dados_consolidados_filtrados.parquet' 
(FORMAT PARQUET, COMPRESSION 'snappy');
```

**Operações realizadas:**
1. União de todos os Parquets da pasta `staging/` (56-57 arquivos semanais)
2. Filtro aplicado diretamente no SQL: apenas plataformas em `PLATAFORMAS_PARA_MANTER`
3. Escrita direta em Parquet (sem carregar DataFrame completo em memória)

**Otimização crítica:**
- ❌ **Sem otimização**: Carregar 4-5M registros → DataFrame Pandas → Filtrar → Salvar (>10GB RAM)
- ✅ **Com DuckDB**: Ler → Filtrar → Escrever em streaming (< 2GB RAM)

### Output Principal

**Arquivo:** `dados_catalogo/processed/dados_consolidados_filtrados.parquet`

**Esquema:** 46 colunas (mesmo esquema dos parquets semanais)

**Características:**
- Registros: ~2-3M linhas (após filtro de plataformas)
- Período: Jul/2024 - Ago/2025 (todos os snapshots semanais concatenados)
- Campo `data_ref` preservado (permite análise temporal)
- Apenas plataformas com `USAR = 'S'`

**Distribuição aproximada por tipo:**
```
Type    | Registros | % do Total
--------|-----------|------------
Movie   | ~1.8M     | 65%
Series  | ~900K     | 32%
Outros  | ~80K      | 3%
```

**Exemplo de registros:**
```
BB_UID      | Platform_Name | BB_Title    | Type   | data_ref   | BB_Year
------------|---------------|-------------|--------|------------|--------
abc123...   | Netflix       | Dark        | Series | 2024-07-11 | 2017
abc123...   | Netflix       | Dark        | Series | 2024-07-18 | 2017  (duplicata temporal)
abc123...   | Netflix       | Dark        | Series | 2024-07-25 | 2017  (duplicata temporal)
xyz789...   | Disney+       | Encanto     | Movie  | 2024-07-11 | 2021
```

**⚠️ Importante:** Este arquivo contém **duplicatas temporais** (mesma obra em múltiplos snapshots). O Notebook 3 elimina essas duplicatas através do crunch temporal.

---

## NOTEBOOK 3: Preparação da Tabela Dimensão

### Arquivo: `3_preparacao_splink1.ipynb`

### Objetivo
Criar uma tabela dimensão limpa e otimizada com **perfis únicos por obra** (BB_UID), aplicando "crunch temporal" para eliminar duplicatas históricas e preparar dados para processos de deduplicação.

### Input
- **Arquivo**: `dados_catalogo/processed/dados_consolidados_filtrados.parquet`

### Dependências
- Bibliotecas: pandas, pathlib, duckdb, IPython.display, unidecode, textwrap

### Configurações de Negócio

#### Semanas Problemáticas (hard-coded)
```python
SEMANAS_A_EXCLUIR = ['2024-10-25', '2024-11-21']
```
- Semanas identificadas com dados inconsistentes ou incompletos
- Excluídas de toda análise e crunch temporal

#### Colunas Selecionadas
```python
COLUNAS_PARA_SPLINK = [
    "BB_UID",              # Identificador único da obra
    "Type",                # Movie ou Series
    "BB_Title",            # Título principal
    "BB_Year",             # Ano de lançamento
    "BB_Duration",         # Duração em minutos
    "BB_Genres",           # Gêneros
    "BB_Directors",        # Diretores
    "BB_Original_Title",   # Título original
    "IMDb_ID",            # Identificador IMDb
    "BB_Countries",        # Países de produção
    "BB_Primary_Country",  # País principal
    "BB_Production_Companies",
    "BB_Primary_Company",
    "TMDB_ID",            # Identificador TMDB
    "BB_Cast"             # Elenco
]
```

### Processamento

#### Bloco 1: Análise Exploratória de Completude

**Objetivo:** Entender a qualidade dos dados por coluna.

**DataFrame gerado:** `df_completude`

**Query SQL:**
```sql
-- Para cada coluna:
SELECT 
    100.0 * COUNT("coluna") / COUNT(*) as pct_preenchimento
FROM read_parquet('dados_consolidados_filtrados.parquet')
```

**Esquema do df_completude:**
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `Coluna` | string | Nome da coluna analisada |
| `Preenchimento (%)` | float | % de valores não-nulos |

**Exemplo:**
```
Coluna              | Preenchimento (%)
--------------------|------------------
BB_UID              | 100.00%
Platform_Name       | 100.00%
BB_Title            | 97.05%
BB_Year             | 77.21%
BB_Directors        | 72.15%
BB_Cast             | 68.48%
IMDb_ID             | 60.66%
BB_Launch_Date      | 15.90%
```

**Interpretação:**
- 100%: Campos obrigatórios, sempre presentes
- 70-95%: Campos bem preenchidos, confiáveis
- 50-70%: Campos com cobertura média
- <50%: Campos esparsos, uso cauteloso

**Output:** Visualização estilizada no notebook (barras de progresso)

#### Bloco 2: Análise de Estabilidade

**Objetivo:** Identificar campos que mudam ao longo do tempo para o mesmo BB_UID.

**DataFrames gerados:**

1. **df_contagens** (intermediário)
   - Query: Conta valores distintos por coluna, agrupado por BB_UID
   - Exemplo:
   ```
   BB_UID      | BB_Title | BB_Year | BB_Cast | Platform_Name | ...
   ------------|----------|---------|---------|---------------|----
   abc123...   | 1        | 1       | 3       | 5             | ...  (título estável, elenco variou 3x)
   xyz789...   | 2        | 1       | 1       | 2             | ...  (título mudou 2x!)
   ```

2. **df_estabilidade** (output final)
   - Calcula % de UIDs com >1 valor para cada coluna
   - Esquema:
   ```
   Coluna              | % de UIDs com >1 Valor
   --------------------|----------------------
   IMDb_ID             | 0.00%    (perfeitamente estável)
   BB_Year             | 0.73%    (quase estável)
   BB_Title            | 6.02%    (6% dos UIDs tiveram título alterado)
   BB_Cast             | 34.64%   (muito instável - elenco muda frequentemente)
   Platform_Country    | 78.14%   (altamente volátil)
   ```

**Interpretação:**
- <5%: Campo estável, mudanças são raras/correções
- 5-20%: Campo moderadamente estável
- 20-50%: Campo instável, variações comuns
- >50%: Campo altamente volátil (pode ser comportamento esperado, ex: Package, Platform_Country)

**Output:** Visualização com gradiente de cor (verde=estável, vermelho=instável)

#### Bloco 3: Seleção de Colunas para Tabela Dimensão

**Lista definida:** `COLUNAS_PARA_SPLINK` (15 colunas)

```python
COLUNAS_PARA_SPLINK = [
    # Identificação
    "BB_UID",           # PK
    "Type",             # Movie/Series
    
    # Metadados Principais
    "BB_Title",         # Título normalizado
    "BB_Year",          # Ano de lançamento
    "BB_Duration",      # Duração em minutos
    "BB_Genres",        # Gêneros (comma-separated)
    "BB_Directors",     # Diretores (comma-separated)
    "BB_Cast",          # Elenco (comma-separated)
    
    # Títulos Alternativos
    "BB_Original_Title", # Título original
    
    # Identificadores Externos
    "IMDb_ID",          # tt0123456
    "TMDB_ID",          # 12345
    
    # Origem/Produção
    "BB_Countries",     # Países (comma-separated)
    "BB_Primary_Country", # País principal
    "BB_Production_Companies", # Produtoras (comma-separated)
    "BB_Primary_Company" # Produtora principal
]
```

**Critérios de seleção:**
- Campos essenciais para matching (título, ano, diretor)
- Campos com boa completude (>50%)
- Campos relativamente estáveis
- Identificadores únicos externos (IMDb, TMDB)

#### Bloco 4: Crunch Temporal (Perfis Únicos)

**Query SQL executada:**
```sql
WITH dados_filtrados AS (
    SELECT [15 colunas selecionadas], data_ref
    FROM read_parquet('dados_consolidados_filtrados.parquet')
    WHERE data_ref NOT IN ('2024-10-25', '2024-11-21')  -- Semanas problemáticas
),
ranking_por_data AS (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY BB_UID ORDER BY data_ref DESC) as rn
    FROM dados_filtrados
)
SELECT [15 colunas selecionadas]
FROM ranking_por_data
WHERE rn = 1  -- Mantém apenas o registro mais recente por UID
```

**DataFrames intermediários:**

1. **dados_filtrados**
   - Remove semanas excluídas (`SEMANAS_A_EXCLUIR`)
   - ~2-3M registros

2. **ranking_por_data**
   - Adiciona coluna `rn` (rank por UID, ordenado por data decrescente)
   - Exemplo:
   ```
   BB_UID    | BB_Title | data_ref   | rn
   ----------|----------|------------|----
   abc123... | Dark     | 2025-08-28 | 1  ← mantido
   abc123... | Dark     | 2025-08-21 | 2
   abc123... | Dark     | 2024-07-11 | 56
   xyz789... | Encanto  | 2025-08-28 | 1  ← mantido
   xyz789... | Encanto  | 2024-07-18 | 55
   ```

3. **df_dim** (resultado final do crunch)
   - Filtro: `rn = 1` (apenas registro mais recente)
   - ~225K linhas (1 por BB_UID único)
   - 15 colunas selecionadas

**Redução alcançada:** ~2.5M → ~225K registros (eliminação de ~90% dos dados - duplicatas temporais)

#### Bloco 5: Limpeza e Normalização de Strings

**Funções aplicadas:**

1. **normalizar_string(texto)**
   ```python
   # Entrada: "Ação & Aventura!"
   # 1. Lowercase: "ação & aventura!"
   # 2. Remove acentos: "acao & aventura!"
   # 3. Strip: "acao & aventura!"
   # Saída: "acao & aventura!"
   ```

2. **normalizar_lista_em_string(texto, sep=',')**
   ```python
   # Entrada: "Tom Hanks, Meryl Streep, Tom Cruise"
   # 1. Split: ["Tom Hanks", " Meryl Streep", " Tom Cruise"]
   # 2. Normaliza cada: ["tom hanks", "meryl streep", "tom cruise"]
   # 3. Ordena: ["meryl streep", "tom cruise", "tom hanks"]
   # 4. Rejunta: "meryl streep,tom cruise,tom hanks"
   # Saída: "meryl streep,tom cruise,tom hanks"
   ```

**Campos transformados:**

| Campo Original | Transformação | Exemplo Antes | Exemplo Depois |
|----------------|---------------|---------------|----------------|
| `BB_Title` | normalizar_string | "Ação Total!" | "acao total!" |
| `BB_Original_Title` | normalizar_string | "Die Hard" | "die hard" |
| `BB_Primary_Country` | normalizar_string | "US" | "us" |
| `BB_Cast` | normalizar_lista_em_string | "Bruce Willis, Alan Rickman" | "alan rickman,bruce willis" |
| `BB_Directors` | normalizar_lista_em_string | "John McTiernan" | "john mctiernan" |
| `BB_Year` | pd.to_numeric + Int64 | "2021.0" (float) | 2021 (Int64) |

**DataFrame final:** `df_limpo`
- 225K linhas
- 15 colunas
- Strings normalizadas (lowercase, sem acentos)
- Listas ordenadas alfabeticamente

#### Bloco 6 (ADICIONAL): Agregação de Plataformas

**Objetivo:** Criar versão enriquecida com lista de todas as plataformas onde cada obra já esteve disponível.

**Query SQL:**
```sql
WITH plataformas_agregadas AS (
    SELECT 
        BB_UID,
        string_agg(DISTINCT Platform_Name, ', ') as Plataformas
    FROM read_parquet('dados_consolidados_filtrados.parquet')
    GROUP BY BB_UID
)
SELECT 
    p.*,  -- Todas as 15 colunas da tabela dimensão
    pa.Plataformas
FROM tabela_dimensao_limpa_para_splink AS p
LEFT JOIN plataformas_agregadas AS pa ON p.BB_UID = pa.BB_UID
```

**DataFrame gerado:** `df_enriquecido`
- 225K linhas
- 16 colunas (15 anteriores + `Plataformas`)

**Exemplo da coluna Plataformas:**
```
BB_UID    | BB_Title | Plataformas
----------|----------|------------------------------------------
abc123... | Dark     | Netflix
xyz789... | Encanto  | Disney+, Sky+, Vivo Play
qwe456... | Friends  | Max, Amazon Prime Video, Netflix, Globoplay
```

### Outputs

#### Output Principal
**Arquivo:** `dados_catalogo/processed/tabela_dimensao_limpa_para_splink.parquet`

**Esquema completo:**

| # | Coluna | Tipo | Nulos Permitidos | Descrição |
|---|--------|------|------------------|-----------|
| 1 | `BB_UID` | string | ❌ Não | PK - Identificador único da obra |
| 2 | `Type` | string | ❌ Não | "Movie" ou "Series" |
| 3 | `BB_Title` | string | ✅ Sim | Título normalizado (lowercase, sem acentos) |
| 4 | `BB_Year` | Int64 | ✅ Sim | Ano de lançamento |
| 5 | `BB_Duration` | float | ✅ Sim | Duração em minutos |
| 6 | `BB_Genres` | string | ✅ Sim | Gêneros separados por vírgula (original) |
| 7 | `BB_Directors` | string | ✅ Sim | Diretores normalizados, ordenados, sep vírgula |
| 8 | `BB_Original_Title` | string | ✅ Sim | Título original normalizado |
| 9 | `IMDb_ID` | string | ✅ Sim | Formato: tt0123456 |
| 10 | `BB_Countries` | string | ✅ Sim | Códigos ISO separados por vírgula |
| 11 | `BB_Primary_Country` | string | ✅ Sim | Código ISO normalizado (lowercase) |
| 12 | `BB_Production_Companies` | string | ✅ Sim | Produtoras separadas por vírgula (original) |
| 13 | `BB_Primary_Company` | string | ✅ Sim | Produtora principal (original) |
| 14 | `TMDB_ID` | float | ✅ Sim | ID numérico do TMDB |
| 15 | `BB_Cast` | string | ✅ Sim | Elenco normalizado, ordenado, sep vírgula |

**Linhas:** 225,117 perfis únicos

**Tamanho:** ~45 MB (comprimido com Snappy)

**Características:**
- 1 registro por BB_UID (sem duplicatas temporais)
- Snapshot do estado mais recente de cada obra
- Strings normalizadas para facilitar matching
- Listas ordenadas para comparação determinística

**Exemplo de registros:**
```
BB_UID      | Type   | BB_Title        | BB_Year | BB_Directors           | BB_Cast
------------|--------|-----------------|---------|------------------------|-------------------------
abc123...   | Series | dark            | 2017    | baran bo odar         | andreas pietschmann,karoline eichhorn,lisa vicari,louis hofmann
xyz789...   | Movie  | encanto         | 2021    | byron howard,jared bush| angie cepeda,diane guerrero,jessica darrow,stephanie beatriz
qwe456...   | Series | friends         | 1994    | david crane,marta kauffman| courteney cox,david schwimmer,jennifer aniston,lisa kudrow,matt leblanc,matthew perry
```

**Status de uso:** ⚠️ Preparado para Splink (notebooks 4-5 abandonados), não usado na solução atual

#### Output Enriquecido
**Arquivo:** `dados_catalogo/processed/tabela_dimensao_com_plataformas.parquet`

**Esquema:** 16 colunas (15 anteriores + `Plataformas`)

**Coluna adicional:**
| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `Plataformas` | string | Lista de plataformas onde a obra já esteve, sep por vírgula | "Netflix, Disney+, Amazon Prime Video" |

**Linhas:** 225,117

**Tamanho:** ~46 MB

**Uso potencial:** 
- Análise de disponibilidade multiplataforma
- Identificar obras exclusivas vs compartilhadas
- Rastrear migrações de catálogo

**Status de uso:** ⚠️ Útil para análise, mas não integrado ao matching atual

---

## Métricas de Redução

| Etapa | Registros | Descrição |
|-------|-----------|-----------|
| **Staging** (após Notebook 1) | ~4-5M | Todos os snapshots de todas as plataformas |
| **Filtrado** (após Notebook 2) | ~2-3M | Apenas plataformas selecionadas |
| **Tabela Dimensão** (após Notebook 3) | ~225K | Perfis únicos (1 registro por BB_UID) |

**Redução**: ~95% (4-5M → 225K)

**⚠️ Status:** Dados preparados, mas não integrados ao fluxo de matching atual. A solução em produção usa o XLSX mais recente (~450K registros sem redução temporal).

---

## Arquivos de Configuração

### mapeamento_colunas.json

**Propósito:** Define o esquema de importação dos arquivos XLSX, evitando carregar colunas desnecessárias e especificando tipos de dados.

**Estrutura:**
```json
{
  "BB_UID": "str",
  "Platform_Content_ID": "str",
  "BB_Hash_Unique": "str",
  "Platform_Name": "str",
  "Platform_Country": "str",
  "Package": "str",
  "Platform_Title": "str",
  "Type": "str",
  "Deeplink": "str",
  "Seasons": "float",
  "Is_Original": "str",
  "Is_Exclusive": "str",
  "Episodes": "float",
  "Platform_Year": "float",
  "Platform_Duration": "float",
  "Platform_Provider": "str",
  "Platform_Genres": "str",
  "Platform_Cast": "str",
  "Platform_Directors": "str",
  "Channel": "str",
  "BB_Title": "str",
  "BB_Original_Title": "str",
  "BB_Year": "float",
  "BB_Duration": "float",
  "BB_Primary_Genre": "str",
  "BB_Genres": "str",
  "BB_Scripted": "str",
  "BB_Primary_Country": "str",
  "BB_Countries": "str",
  "BB_Primary_Company": "str",
  "BB_Production_Companies": "str",
  "BB_Cast": "str",
  "BB_Directors": "str",
  "BB_Languages": "str",
  "BB_Keywords": "str",
  "BB_Votes": "float",
  "BB_Score": "float",
  "TMDB_ID": "float",
  "TVDB_ID": "float",
  "IMDb_ID": "str",
  "Season_Numbers": "str",
  "BB_Launch_Date": "str",
  "BB_Poster_Image": "str",
  "BB_Coverage_In_Days": "float"
}
```

**Total de colunas mapeadas:** 45 campos

**Convenções de nomenclatura:**
- `BB_*`: Metadados normalizados pela Binge Box (fonte dos dados)
- `Platform_*`: Metadados brutos fornecidos pelas plataformas
- Campos sem prefixo: Metadados estruturais (UID, Type, Package, etc.)

**Uso no pipeline:** 
- Notebook 1 lê este JSON e usa apenas as colunas listadas
- Colunas não mapeadas geram warning no relatório de conversão

---

### plataformas_controle.xlsx

**Propósito:** Controle manual de quais plataformas devem ser incluídas no processamento.

**Estrutura:**
| Coluna | Tipo | Valores Aceitos | Descrição |
|--------|------|-----------------|-----------|
| `Plataforma` | string | Nome exato da plataforma | Deve corresponder ao valor em `Platform_Name` |
| `USAR` | string | 'S', 'N' (case insensitive) | Flag de inclusão/exclusão |

**Exemplo:**
```
Plataforma          | USAR
--------------------|-----
Netflix             | S
Amazon Prime Video  | S
Disney+             | S
Pluto TV            | N
Tubi                | N
```

**Regra de negócio:**
- Apenas plataformas com `USAR = 'S'` passam para a etapa de consolidação
- Novas plataformas detectadas nos dados **não são processadas automaticamente**
- Notebook 2 gera alerta e interrompe se encontrar plataformas não catalogadas

---

### manifesto_xlsx.json

**Propósito:** Controle de versionamento e rastreabilidade dos arquivos processados. Evita reprocessamento desnecessário.

**Localização:** `dados_catalogo/manifesto_xlsx.json`

**Estrutura:**
```json
{
  "BB Media - 2024-07-11.xlsx": {
    "processado_em": "2025-01-15 14:32:10",
    "linhas": 82340,
    "colunas": 45
  },
  "BB Media - 2024-07-18.xlsx": {
    "processado_em": "2025-01-15 14:33:22",
    "linhas": 83127,
    "colunas": 45
  }
}
```

**Campos por arquivo:**
- `processado_em`: Timestamp da conversão
- `linhas`: Total de registros processados
- `colunas`: Total de colunas importadas

**Lógica de uso:**
- Notebook 1 verifica este arquivo antes de processar cada XLSX
- Arquivos já presentes no manifesto são ignorados (processamento incremental)
- Manifesto é atualizado após cada conversão bem-sucedida

---

## Troubleshooting e FAQ

### Problemas Comuns

#### "Colunas não mapeadas" no Notebook 1

**Sintoma:** Relatório de conversão lista colunas em `colunas_nao_mapeadas`

**Exemplo:**
```
colunas_nao_mapeadas: ['BB Age', 'Douban ID', 'Platform Rating']
```

**Causa:** 
- Novo campo apareceu nos arquivos Excel mais recentes
- Campo não está no `mapeamento_colunas.json`

**Solução:**
1. Avaliar relevância da coluna para análise futura
2. Se relevante: adicionar ao `mapeamento_colunas.json`
   ```json
   "BB_Age": "str",
   "Douban_ID": "str",
   "Platform_Rating": "float"
   ```
3. Reprocessar arquivos afetados:
   - Deletar entrada do `manifesto_xlsx.json`
   - Deletar Parquet correspondente em `staging/`
   - Rodar Notebook 1 novamente

**Prevenção:**
- Revisar relatório de conversão após cada execução
- Manter `mapeamento_colunas.json` atualizado

---

#### "Plataforma não encontrada" no Notebook 2

**Sintoma:** Notebook detecta plataforma nova e gera alerta

**Exemplo:**
```html
[!] NOVAS PLATAFORMAS ENCONTRADAS
- Apple TV+
- Paramount+
```

**Causa:**
- Nova plataforma começou a ser monitorada
- Plataforma não está no `plataformas_controle.xlsx`

**Solução:**
1. Abrir `plataformas_controle.xlsx`
2. Adicionar linha com nova plataforma:
   | Plataforma | USAR |
   |------------|------|
   | Apple TV+ | S |
   | Paramount+ | N |
3. Decidir se deseja incluir (`S`) ou excluir (`N`)
4. Reprocessar Notebook 2

**Atenção:**
- Se marcar `USAR = 'S'`, a plataforma será incluída no consolidado
- Isso afetará métricas e volumes de dados

---

#### Memória insuficiente durante processamento

**Sintoma:** 
- Kernel trava ou reinicia
- Mensagem: `MemoryError` ou `Killed`

**Causa:**
- DataFrame muito grande carregado na RAM
- Memória do sistema insuficiente

**Solução por Notebook:**

**Notebook 1:**
- Reduzir `num_processos` na linha:
  ```python
  num_processos = cpu_count() // 2  # Usa metade dos núcleos
  ```

**Notebook 2:**
- ✅ Já otimizado com DuckDB (não carrega na RAM)
- Se ainda der problema: aumentar swap do sistema

**Notebook 3:**
- Bloco de crunch temporal já usa DuckDB
- Se problema estiver no `df_limpo`:
  ```python
  # Processar em chunks
  colunas_texto = ['BB_Title', 'BB_Cast', 'BB_Directors']
  for col in colunas_texto:
      df_limpo[col] = df_limpo[col].apply(funcao_normalizacao)
      gc.collect()  # Força limpeza de memória
  ```

---

#### Valores estranhos na análise de estabilidade (Notebook 3)

**Sintoma:** Campo esperado aparece com alta instabilidade

**Exemplo:** `BB_Year` com 15% de instabilidade (esperado < 5%)

**Causas possíveis:**
1. **Correções de metadados ao longo do tempo**
   - Fornecedor atualizou informações incorretas
   - Normal até ~5%

2. **Mesclagem de obras diferentes com mesmo UID**
   - Bug no sistema de origem
   - Requer investigação manual

3. **Campos naturalmente voláteis**
   - `Package`, `Platform_Country`: esperado >50%
   - Não é problema, é comportamento normal

**Investigação:**
```python
# Identificar UIDs com variação em BB_Year
query = """
SELECT 
    BB_UID,
    BB_Title,
    STRING_AGG(DISTINCT CAST(BB_Year AS VARCHAR), ', ') as anos_encontrados,
    COUNT(DISTINCT BB_Year) as total_anos
FROM read_parquet('dados_consolidados_filtrados.parquet')
GROUP BY BB_UID, BB_Title
HAVING COUNT(DISTINCT BB_Year) > 1
"""

df_anos_variaveis = con.execute(query).fetch_df()
print(df_anos_variaveis.head(20))
```

---

### Perguntas Frequentes (FAQ)

#### 1. Por que usar Parquet em vez de CSV?

**Resposta:**
- **Tamanho:** 70-80% menor que CSV (compressão colunar)
- **Performance:** Leitura 10-50x mais rápida (skip de colunas não usadas)
- **Tipos de dados:** Schema integrado (sem ambiguidade float vs string)
- **Compatibilidade:** Pandas, DuckDB, Spark, Polars nativamente

**Comparação prática:**
```
dados_consolidados.csv:  ~2.8 GB, leitura ~45 segundos
dados_consolidados.parquet: ~800 MB, leitura ~3 segundos
```

---

#### 2. Posso processar apenas 1 arquivo novo sem reprocessar tudo?

**Sim!** O Notebook 1 é incremental:
1. Coloque novo XLSX em `raw_xlsx/`
2. Rode Notebook 1 normalmente
3. Manifesto detecta que outros arquivos já foram processados
4. Apenas o novo arquivo será convertido

Para Notebooks 2 e 3: **precisam rodar completos** (não são incrementais ainda)

---

#### 3. Como remover uma plataforma já processada?

**Solução:**
1. Editar `plataformas_controle.xlsx`: mudar `USAR` de `'S'` para `'N'`
2. Reprocessar Notebook 2 (irá filtrar a plataforma)
3. Reprocessar Notebook 3

**Nota:** Os Parquets em `staging/` mantêm os dados originais. Apenas o consolidado muda.

---

#### 4. Qual a diferença entre as duas tabelas dimensão?

| Arquivo | Colunas | Uso Recomendado |
|---------|---------|-----------------|
| `tabela_dimensao_limpa_para_splink.parquet` | 15 | Matching, deduplicação (dados limpos) |
| `tabela_dimensao_com_plataformas.parquet` | 16 (+Plataformas) | Análise de disponibilidade, relatórios |

Ambos têm as mesmas 225K linhas (perfis únicos), apenas diferem na coluna extra de plataformas.

---

#### 5. Posso adicionar mais colunas na tabela dimensão depois?

**Sim, mas requer reprocessamento:**
1. Editar lista `COLUNAS_PARA_SPLINK` no Notebook 3
2. Rodar Notebook 3 novamente (rápido, ~2-3 minutos)

**Não precisa** reprocessar Notebooks 1 e 2.

---

#### 6. Como atualizar a lista de semanas excluídas?

**Localização:** Notebook 3, início do código:
```python
SEMANAS_A_EXCLUIR = ['2024-10-25', '2024-11-21']
```

**Para adicionar nova semana:**
1. Identificar semana problemática (via análise do Notebook 2)
2. Adicionar à lista: `'YYYY-MM-DD'`
3. **Documentar o motivo:** comentário no código
   ```python
   SEMANAS_A_EXCLUIR = [
       '2024-10-25',  # Scraping incompleto - 30% dos dados faltando
       '2024-11-21',  # Mudança de schema - colunas inconsistentes
       '2025-01-15'   # Plataforma X reportou dados duplicados
   ]
   ```
4. Reprocessar Notebook 3

---

#### 7. O Notebook 3 demora muito. Como acelerar?

**Análise de gargalos:**
- Bloco 1 (Completude): ~10 segundos ✅ Rápido
- Bloco 2 (Estabilidade): ~2 minutos ⚠️ Pode ser lento
- Bloco 3 (Crunch): ~30 segundos ✅ Otimizado
- Bloco 4 (Normalização): ~1.5 minutos ⚠️ Pode ser lento

**Otimizações:**
1. **Pular análises exploratórias** (Blocos 1 e 2):
   - Não afetam o output final
   - Comenta ou não executa essas células

2. **Normalização vetorizada:**
   ```python
   # Mais rápido que .apply()
   df_limpo['BB_Title'] = df_limpo['BB_Title'].str.lower()
   ```

3. **Usar Polars** (biblioteca alternativa mais rápida que Pandas):
   - Requer refatoração do código
   - Speedup típico: 3-5x

**Tempo esperado total:** 
- Com análises: ~5 minutos
- Sem análises: ~2 minutos

---

#### 8. Como verificar se o pipeline rodou corretamente?

**Checklist de validação:**

```python
import pathlib

# 1. Verificar presença dos outputs
arquivos_esperados = [
    'dados_catalogo/manifesto_xlsx.json',
    'dados_catalogo/processed/dados_consolidados_filtrados.parquet',
    'dados_catalogo/processed/tabela_dimensao_limpa_para_splink.parquet',
]

for arquivo in arquivos_esperados:
    existe = pathlib.Path(arquivo).exists()
    print(f"{'✅' if existe else '❌'} {arquivo}")

# 2. Verificar volumes
import pandas as pd

df_consolidado = pd.read_parquet('dados_catalogo/processed/dados_consolidados_filtrados.parquet')
df_dimensao = pd.read_parquet('dados_catalogo/processed/tabela_dimensao_limpa_para_splink.parquet')

print(f"\n📊 Métricas:")
print(f"  Consolidado: {len(df_consolidado):,} registros")
print(f"  Dimensão: {len(df_dimensao):,} perfis únicos")
print(f"  Redução: {(1 - len(df_dimensao)/len(df_consolidado))*100:.1f}%")

# 3. Verificar normalização
amostras_normalizadas = df_dimensao['BB_Title'].head(10)
tem_maiuscula = any(s.isupper() for s in amostras_normalizadas if pd.notna(s))
print(f"  Normalização: {'❌ Falhou' if tem_maiuscula else '✅ OK'}")

# Valores esperados:
# Consolidado: 2.0-3.0M registros
# Dimensão: 220-230K perfis
# Redução: 90-92%
# Normalização: OK (tudo lowercase)
```

---

#### 9. Posso usar estes notebooks para outros projetos?

**Sim, com adaptações:**

**Estrutura reutilizável:**
- Notebook 1: Conversão XLSX → Parquet (genérico)
- Notebook 2: Validação temporal + filtros (adaptável)
- Notebook 3: Crunch temporal + limpeza (adaptável)

**O que precisa mudar:**
1. `mapeamento_colunas.json` → seu schema
2. `plataformas_controle.xlsx` → suas categorias de filtro
3. `COLUNAS_PARA_SPLINK` → suas colunas de interesse
4. Lógica de `data_ref` → sua dimensão temporal
5. Funções de normalização → suas regras de negócio

**Componentes 100% reutilizáveis:**
- Processamento paralelo (Notebook 1)
- Queries DuckDB (Notebooks 2 e 3)
- Análise de completude/estabilidade (Notebook 3)

---

## Dependências do Ambiente

```bash
# Principais bibliotecas
pandas>=2.0
openpyxl>=3.0
pyarrow>=10.0
duckdb>=0.9
unidecode>=1.3
tqdm>=4.60

# Jupyter
ipykernel
IPython
```

---

## Notas Importantes

1. **Ordem de Execução**: Notebooks devem ser rodados na sequência (1 → 2 → 3)
2. **Notebook 1.1**: Opcional, pode rodar após Notebook 1 sem afetar o fluxo
3. **Notebook 3.1 (Splink_Exploracao)**: ❌ **DESCARTADO** - lê arquivo errado (`dados_consolidados_filtrados.parquet` em vez de `tabela_dimensao_limpa_para_splink.parquet`), causando duplicatas temporais
4. **Incremental**: Notebook 1 usa manifesto para processar apenas arquivos novos
5. **Controle Manual**: Notebook 2 exige atualização manual de `plataformas_controle.xlsx`
6. **Semanas Excluídas**: Valores hard-coded no Notebook 3 - ajustar conforme necessário
   ```python
   SEMANAS_A_EXCLUIR = ['2024-10-25', '2024-11-21']
   ```
   - Documentar o motivo da exclusão (dados corrompidos/incompletos)

---

## Estado Atual do Projeto

### ✅ O que está funcionando:
- **Notebooks 1-3**: Preparação histórica completa e validada
- **Matching determinístico**: Implementado e documentado em `pipeline_cpb_uid.md`
- **Base t0**: Consolidada e enriquecida (ano passado)

### ⚠️ O que está em standby:
- **Histórico em Parquet**: Preparado mas não integrado ao matching
- **Notebooks 4-5 (Splink)**: Abandonados

### 🔨 O que falta implementar:
- **Crunch temporal**: Gerar IN/OUT com tolerância de 22 dias
- **Integração t0 + histórico**: Unificar bases de anos diferentes
- **Expansão do XLSX atual**: Adicionar obras que saíram + colunas temporais
- **Pipeline incremental**: Processar apenas novos snapshots semanais

---

## Resumo Executivo

### Para o "Você do Futuro"

**Se você está voltando a este projeto depois de meses:**

1. **Comece aqui:** Leia `pipeline_cpb_uid.md` - é o matching que está funcionando em produção
2. **Estes notebooks (1-3):** Preparam histórico de dados mas **não estão integrados** ao matching atual
3. **Status atual:** Base t1 em construção - falta implementar crunch temporal com IN/OUT

### O que Funciona

✅ **Conversão de dados:** Notebooks 1-3 rodando sem problemas  
✅ **Matching determinístico:** Implementado em outro pipeline (pipeline_cpb_uid.md)  
✅ **Base t0:** Consolidada e enriquecida (ano passado)

### O que Falta

🔨 **Crunch temporal com IN/OUT:** Gerar datas de entrada/saída com tolerância de 22 dias  
🔨 **Integração t0 + histórico:** Unificar base passada com dados atuais  
🔨 **Expansão do XLSX:** Adicionar obras que saíram + colunas temporais  
🔨 **Pipeline incremental:** Processar apenas novos snapshots

### Decisões Arquiteturais Importantes

1. **DuckDB para economia de memória** - Queries diretas em Parquet sem carregar na RAM
2. **Parquet como formato intermediário** - 90% menor que CSV, leitura 10-50x mais rápida
3. **Processamento paralelo** - Speedup de 6-8x na conversão XLSX
4. **Normalização agressiva** - Lowercase + sem acentos + listas ordenadas para matching
5. **Crunch temporal** - 1 registro por obra (elimina 90% das duplicatas históricas)

### Próxima Vez que For Trabalhar Nisto

**Se for continuar o matching determinístico:**
- Ignore estes notebooks
- Trabalhe direto no `pipeline_cpb_uid.md`

**Se for integrar o histórico temporal:**
1. Implemente crunch com IN/OUT (tolerância 22 dias)
2. Unifique esquemas t0 + t1
3. Expanda XLSX com obras que saíram
4. Integre ao matching determinístico

**Se for reativar Splink:**
- Use `tabela_dimensao_limpa_para_splink.parquet` (não `dados_consolidados_filtrados.parquet`)
- Cuidado com memória (225K é OK, 2.5M vai explodir)

---

## Contatos e Referências

### Documentação Relacionada
- **Matching atual:** `pipeline_cpb_uid.md` (pipeline determinístico em produção)
- **Base Ancine:** Documentação da estrutura SAD/CPB (se existir)

### Tecnologias Utilizadas
- **Pandas:** Manipulação de dados (https://pandas.pydata.org/)
- **DuckDB:** Query engine analítico (https://duckdb.org/)
- **PyArrow/Parquet:** Formato colunar (https://arrow.apache.org/)
- **Jupyter:** Ambiente de notebooks (https://jupyter.org/)

### Versões Testadas
```
pandas>=2.0.0
duckdb>=0.9.0
pyarrow>=10.0.0
openpyxl>=3.0.0
unidecode>=1.3.0
tqdm>=4.60.0
```

---

## Changelog do Pipeline

### 2025-01 - Estado Atual
- ✅ Notebooks 1-3 implementados e testados
- ✅ Processamento de 57 snapshots semanais (Jul/2024 - Ago/2025)
- ⚠️ Splink abandonado (notebooks 4-5 descartados)
- ✅ Matching determinístico funcionando (pipeline separado)
- 🔨 Integração temporal pendente (IN/OUT)

### Próximas Iterações (Planejado)
- Implementar crunch temporal com tolerância de 22 dias
- Unificar esquemas t0 e t1
- Pipeline incremental (processar apenas novos snapshots)
- Automação do matching semanal

---

**Documentação gerada para:** Pipeline de Preparação de Dados VOD  
**Última atualização:** Janeiro 2025  
**Versão dos notebooks:** 1.0 (estável)  
**Status:** Preparação completa | Integração pendente

---

## Contexto: Tentativas com Splink (Notebooks 4-5)

**Status:** ❌ Abandonados

Após os notebooks 1-3, foram desenvolvidos:
- **Notebook 4**: Tentativa de deduplicação interna usando Splink
- **Notebook 5**: Tentativa de matching com base Ancine usando Splink

**Por que foram abandonados:**
- Performance insatisfatória em grandes volumes
- Dificuldade de calibração dos modelos probabilísticos
- Necessidade de solução mais determinística e controlável

**Solução atual implementada:**
- Matching determinístico baseado em regras (Título + Ano + Diretor/Produtora)
- Documentado em: `pipeline_cpb_uid.md`
- **Trabalha apenas com o snapshot XLSX mais recente** (não usa o histórico em Parquet)
- Redução de escopo: foco no catálogo atual (mais importante para o negócio)

---

## Lacunas a Implementar

Os notebooks 1-3 preparam os dados históricos, mas **não estão sendo usados** na solução de matching atual. Para integrá-los ao fluxo final, será necessário:

### 1. Crunch Temporal com IN/OUT
**Objetivo:** A partir dos Parquets históricos, criar tabela com:
- `BB_UID`: Identificador único da obra
- `First_Seen`: Primeira data em que a obra apareceu (IN)
- `Last_Seen`: Última data em que a obra apareceu (OUT)
- **Regra de tolerância**: Gap de até 22 dias não caracteriza saída de catálogo
  - Motivo: Falhas no scraping podem causar ausências temporárias
  - Obras raramente ficam apenas 1 semana fora do catálogo

**Input:** `dados_catalogo/staging/*.parquet` (todos os snapshots semanais)

**Output:** Tabela temporal ainda não implementada

### 2. Integração com Base t0
**Objetivo:** Juntar base histórica (ano passado) com dados atuais

**Desafios conhecidos:**
- Esquemas diferentes entre t0 e dados atuais
- Campos com nomes/formatos distintos
- Necessidade de mapeamento entre as bases

### 3. Expansão do XLSX Mais Recente
**Objetivo:** Enriquecer snapshot atual com histórico temporal

**Operações necessárias:**
- Adicionar obras que saíram de catálogo (presentes no histórico, ausentes no atual)
- Imputar colunas IN/OUT em todas as obras
- Preparar para aplicação do matching determinístico (pipeline_cpb_uid.md)

---

## Próximos Passos (Roadmap)

### Curto Prazo (Construção da Base t1)
1. ✅ Preparar histórico semanal (Notebooks 1-3) - **CONCLUÍDO**
2. ⏳ Implementar crunch temporal com IN/OUT (tolerância 22 dias) - **PENDENTE**
3. ⏳ Integrar com base t0 - **PENDENTE**
4. ⏳ Expandir XLSX mais recente com temporal - **PENDENTE**
5. ✅ Aplicar matching determinístico - **CONCLUÍDO** (pipeline_cpb_uid.md)

### Médio Prazo (Após Base t1 Consolidada)
- Adaptar pipeline para processamento incremental (semanal ou quinzenal)
- Automatizar detecção de IN/OUT para novas obras
- Reduzir dependência de processamento histórico completo

---
