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
[1] Conversão XLSX → Parquet
         ↓
[1.1] Análise de Plataformas (opcional/auxiliar)
         ↓
[2] Validação e Filtragem
         ↓
[3] Preparação da Tabela Dimensão
         ↓
    OUTPUT FINAL
```

---

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
- **Parquets**: `dados_catalogo/staging/BB_Media_YYYY-MM-DD.parquet`
- **Manifesto**: `dados_catalogo/manifesto_xlsx.json`
  - Controle de arquivos já processados (evita duplicação)
- **Relatório**: `dados_catalogo/logs/relatorio_conversao_TIMESTAMP.csv`
  - Status de cada arquivo (sucesso/erro)
  - Colunas não mapeadas encontradas

### Características
- **Incremental**: Só processa arquivos novos (usa manifesto)
- **Performance**: Multiprocessing para processamento em lote
- **Validação**: Identifica colunas não mapeadas (para atualizar configuração)

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
- Valida presença das colunas obrigatórias
- Cria lista `PLATAFORMAS_PARA_MANTER`

#### Bloco 2: Relatório de Validação
Gera análise cronológica **semana a semana**:

**Para cada semana:**
1. **Novas Plataformas**: Aparecem pela primeira vez
2. **Plataformas que Retornaram**: Estavam ausentes e voltaram (gaps)
3. **Plataformas que Saíram**: Deixaram de reportar dados
4. **Variações de Catálogo**: Mudanças >5% no total de registros

**Outputs do Bloco 2:**
- Tabela pivot completa (todas as plataformas x todas as semanas)
- `dados_catalogo/processed/relatorio_timeline_plataformas_TIMESTAMP.csv`
- `dados_catalogo/processed/relatorio_timeline_plataformas_TIMESTAMP.xlsx`
- Relatório visual no notebook (HTML formatado)

#### Bloco 3: Consolidação e Filtragem Final
- **Otimização**: Usa DuckDB para operação direta em SQL
  - Evita carregar DataFrame gigante em memória
  - Aplica filtro de plataformas diretamente no banco
- Salva resultado consolidado em formato Parquet

### Output Principal
- **Arquivo**: `dados_catalogo/processed/dados_consolidados_filtrados.parquet`
  - Contém **apenas** plataformas marcadas com `USAR = 'S'`
  - União de todas as semanas (histórico completo)
  - Campo `data_ref` preservado para análise temporal

### Características
- **Performance**: DuckDB para queries rápidas em grandes volumes
- **Controle Manual**: Usuário decide quais plataformas manter
- **Rastreabilidade**: Relatórios datados preservam histórico de análises

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
- Para cada coluna, calcula % de valores não-nulos
- Visualização com barras de progresso
- **Objetivo**: Entender quais campos são mais confiáveis

#### Bloco 2: Análise de Estabilidade
- Para cada BB_UID, verifica quantos valores distintos possui em cada campo ao longo das semanas
- % de UIDs com >1 valor = **instabilidade do campo**
- **Exemplo**: Se `BB_Title` muda para 20% dos UIDs ao longo do tempo, o campo é instável
- Visualização com gradiente de cor (verde = estável, vermelho = instável)

#### Bloco 3: Crunch Temporal (Perfis Únicos)
Query SQL que:
1. Remove semanas da `SEMANAS_A_EXCLUIR`
2. Para cada `BB_UID`, mantém **apenas o registro mais recente** (`data_ref DESC`)
3. Seleciona apenas `COLUNAS_PARA_SPLINK`

**Resultado**: De ~4-5M registros → ~225K perfis únicos (obras distintas)

#### Bloco 4: Limpeza de Strings e Padronização

**Funções Aplicadas:**
- `normalizar_string()`:
  - Converte para lowercase
  - Remove acentos (unidecode)
  - Remove espaços extras
  
- `normalizar_lista_em_string()`:
  - Para campos como `BB_Cast`, `BB_Directors`
  - Split por vírgula
  - Normaliza cada nome
  - Ordena alfabeticamente
  - Rejunta com vírgula

**Campos Normalizados:**
- Títulos: `BB_Title`, `BB_Original_Title`
- Listas: `BB_Cast`, `BB_Directors`
- Metadados: `BB_Primary_Country`

**Ajuste de Tipo:**
- `BB_Year`: convertido para `Int64` (aceita nulos)

#### Bloco 5 (ADICIONAL): Agregação de Plataformas
- Cria versão enriquecida da tabela
- Para cada BB_UID, agrega **todas** as plataformas onde já esteve disponível
- Join com dados históricos completos

### Outputs

#### Output Principal
**Arquivo**: `dados_catalogo/processed/tabela_dimensao_limpa_para_splink.parquet`
- ~225K linhas (perfis únicos por BB_UID)
- 15 colunas (conforme `COLUNAS_PARA_SPLINK`)
- Dados normalizados e limpos
- **Status de uso:** ⚠️ Preparado para Splink (notebooks 4-5 abandonados), não usado na solução atual

#### Output Enriquecido
**Arquivo**: `dados_catalogo/processed/tabela_dimensao_com_plataformas.parquet`
- Mesmas 225K linhas
- 16 colunas (15 anteriores + `Plataformas`)
- Campo `Plataformas`: string com lista separada por vírgula
- **Status de uso:** ⚠️ Útil para análise, mas não integrado ao matching atual

### Características
- **Deduplicação Temporal**: Elimina snapshots históricos, mantém só perfil atual
- **Normalização Agressiva**: Padroniza texto para facilitar comparações
- **Preserva Histórico**: Versão com plataformas mantém rastreabilidade
- **Memory-Efficient**: Usa DuckDB para operações pesadas

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
```json
{
  "BB_UID": "str",
  "Platform_Name": "str",
  "BB_Title": "str",
  "BB_Year": "float",
  ...
}
```
- Define schema de importação
- Evita carregar colunas desnecessárias
- Inclui tipo de dado esperado

### plataformas_controle.xlsx
| Plataforma | USAR |
|------------|------|
| Netflix | S |
| Disney+ | S |
| Pluto TV | N |
| ... | ... |

- Controle manual de quais plataformas processar
- Coluna `USAR`: valores aceitos = `'S'` ou `'N'` (case insensitive)

---

## Troubleshooting

### "Colunas não mapeadas" no Notebook 1
- **Causa**: Novo campo apareceu nos arquivos Excel
- **Solução**: Adicionar campo ao `mapeamento_colunas.json` se relevante

### "Plataforma não encontrada" no Notebook 2
- **Causa**: Nova plataforma nos dados, não está em `plataformas_controle.xlsx`
- **Solução**: 
  1. Consultar relatório de validação (seção "Novas Plataformas")
  2. Adicionar à planilha com flag `'S'` ou `'N'`
  3. Reprocessar Notebook 2

### Memória insuficiente
- **Notebooks 1 e 2**: Usar DuckDB (já implementado)
- **Notebook 3**: 
  - Reduzir `COLUNAS_PARA_SPLINK` (menos campos)
  - Processar em lotes por plataforma (não implementado)

### Valores estranhos na análise de estabilidade
- **Valores altos** (>30%): Campo naturalmente variável (ex: `BB_Score`, `Package`)
- **Valores médios** (10-30%): Inconsistências nas fontes de dados
- **Valores baixos** (<10%): Campos confiáveis (ex: `BB_Year`, `IMDb_ID`)

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

**Nota para retorno futuro:** Ao voltar a este projeto em alguns meses, comece pelo `pipeline_cpb_uid.md` para entender o matching em produção. Os notebooks 1-3 são preparação de dados históricos que aguardam integração.

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
