# Pipeline de Matching CPB-UID 2025

## 1. INPUTS - Arquivos Carregados

| Arquivo | Descrição | Variável |
|---------|-----------|----------|
| `Matchs até 2024.xlsx` | Histórico de matches de 2024 | `matchs_2024` |
| `Obras Ancine.xlsx` | Base SAD da Ancine (CPB) | `Base_SAD` |
| `BB Media - 2025-08-28.xlsx` | Base completa BB 2025 | `Base_BB_import` |
| `Base BB 2024 inicial.xlsx` | UIDs já processados em 2024 | `Base_BB_2024` |

---

## 2. DATAFRAMES PRINCIPAIS

### 2.1 Bases Iniciais (após limpeza)

**Base_SAD** (Ancine)
- Obras brasileiras registradas na Ancine
- Identificador: `Nº CPB`

**Base_BB_inicial** (BB filtrada)
- Remove: duplicatas por `BB UID`
- Remove: UIDs já processados em 2024
- Remove: plataformas indesejadas (34 plataformas excluídas)

**Base_BB_trim**
- Apenas colunas de interesse (20 colunas mantidas)

### 2.2 Divisão por Tipo e Origem

**SAD (2 dataframes):**
- `filmesBR_SAD` → Organização Temporal = "NÃO SERIADA"
- `seriesBR_SAD` → Organização Temporal ≠ "NÃO SERIADA"

**BB (4 dataframes):**
- `filmesBR_BB` → Type = "Movie" + BB Countries contém "BR"
- `seriesBR_BB` → Type = "Series" + BB Countries contém "BR"
- `filmesEstr_BB` → Type = "Movie" + BB Countries NÃO contém "BR"
- `seriesEstr_BB` → Type = "Series" + BB Countries NÃO contém "BR"

### 2.3 Bases Limpas (versões *_limpo)

Após processamento de strings e criação de campos derivados:

**SAD:**
- `filmesBR_SAD_limpo`
- `seriesBR_SAD_limpo`

**BB:**
- `filmesBR_BB_limpo`
- `seriesBR_BB_limpo`
- `filmesEstr_BB_limpo`
- `seriesEstr_BB_limpo`

**Campos criados:**
- `*_processado` → versões normalizadas de títulos, diretores, produtoras
- `Ano Inicial` / `Ano Final` → extraídos de "Ano de Produção" (SAD)
- `dir_set_sad` / `dir_set_bb` → conjuntos de sobrenomes de diretores
- `prod_tokens_sad` / `prod_tokens_bb` → tokens de produtoras

---

## 3. PROCESSO DE MATCHING

### 3.1 Regra 1: Título + Ano + DIRETOR

**Função:** `match_pair_min()`

**Critérios:**
- Título processado igual (compara 3 variantes BB: Title, Original Title, Platform Title)
- Ano ±2 (BB Year entre Ano Inicial-2 e Ano Final+2)
- Diretor obrigatório (sobrenomes devem ter interseção)

**Resultado:** `matches_dir` (concatena 4 categorias)

### 3.2 Regra 2: Título + Ano + PRODUTORA

**Função:** `match_pair_produtora()`

**Critérios:**
- Título processado igual
- Ano ±2
- Produtora obrigatória (tokens devem ter interseção)
- **Importante:** Só processa UIDs que NÃO casaram na Regra 1

**Resultado:** `matches_prod` (concatena 4 categorias)

### 3.3 União Final

**DataFrame:** `matches_all`

**Lógica:**
```
matches_all = concat([matches_dir, matches_prod])
              .drop_duplicates(subset=['UID', 'Nº CPB'], keep='first')
```

**Prioridade:** DIRETOR > PRODUTORA (por ordem do concat)

### 3.4 Categorias de Matching

| Categoria | Descrição |
|-----------|-----------|
| `filmes_BR` | Filmes brasileiros BB × SAD |
| `series_BR` | Séries brasileiras BB × SAD |
| `filmes_EST` | Filmes estrangeiros BB × SAD |
| `series_EST` | Séries estrangeiras BB × SAD |

---

## 4. FUNÇÕES-CHAVE

### 4.1 Normalização de Texto

| Função | Finalidade |
|--------|-----------|
| `process_string3()` | Lowercase, remove acentos, pontuação, palavras comuns, romanos→arábicos |
| `_colapsar_sigla_pontilhada()` | "L.A.P.A." → "LAPA" |
| `remove_cpf_inicio()` | Remove CPFs no início de strings |
| `remove_cpf_fim()` | Remove CPFs no final de strings |
| `remove_last_baixada()` | Remove "(BAIXADA)", "- BAIXADO", etc. |
| `remove_palavras_específicas2()` | Remove LTDA, ME, EIRELI, etc. de produtoras |
| `nonempty_str()` | Valida strings não-vazias (exclui NaN, '', 'nan', 'None') |

### 4.2 Extração de Features

| Função | Finalidade |
|--------|-----------|
| `surnames_from_raw()` | Extrai sobrenomes de diretores (divide por vírgula, considera partículas) |
| `tokens_empresas()` | Gera tokens distintivos de produtoras (>2 letras) |
| `extract_ano_inicial()` | Extrai ano inicial de "AAAA A AAAA" |
| `extract_ano_final()` | Extrai ano final de "AAAA A AAAA" |

### 4.3 Matching

| Função | Finalidade |
|--------|-----------|
| `build_bb_title_keys()` | Explode 3 variantes de título BB (melt) |
| `match_pair_min()` | Match por título+ano+diretor |
| `match_pair_produtora()` | Match por título+ano+produtora |

---

## 5. OUTPUTS - Arquivos Exportados

### 5.1 Pre-processamento bases limpas.xlsx

**Finalidade:** Checkpoint intermediário (economiza tempo de reprocessamento)

**Abas:**
- `Filmes SAD` → filmesBR_SAD_limpo
- `Séries SAD` → seriesBR_SAD_limpo
- `Filmes BR BB` → filmesBR_BB_limpo
- `Séries BR BB` → seriesBR_BB_limpo
- `Filmes Estr BB` → filmesEstr_BB_limpo
- `Séries Estr BB` → seriesEstr_BB_limpo

**Quando usar:** Para retomar trabalho sem reprocessar tudo (pular seções 1-3)

---

### 5.2 Auditoria - matches consolidado.xlsx

**Finalidade:** Resultado FINAL dos matches encontrados (lado a lado SAD × BB)

**Abas:**

1. **consolidado**
   - Todos os pares (Nº CPB ↔ UID) encontrados
   - Colunas lado a lado: Título Original vs BB Title, Diretor vs Directors, etc.
   - Contém campos `categoria` e `match_rule`

2. **resumo_categoria**
   - Contagens por categoria (filmes_BR, series_BR, filmes_EST, series_EST)
   - Métricas: pares, UIDs únicos, CPBs únicos

3. **resumo_regra**
   - Contagens por regra de matching
   - Colunas: titulo+ano+diretor vs titulo+ano+produtora

**Quando usar:** Para conferir resultado final, validar matches, gerar relatórios

---

### 5.3 Auditoria - estágios por categoria.xlsx

**Finalidade:** Análise detalhada do funil de matching (debug/otimização)

**Abas (5 por categoria = 20 abas totais):**

Para cada categoria (filmes_BR, series_BR, filmes_EST, series_EST):

1. `[categoria] - título` → Apenas título coincide (sem filtros)
2. `[categoria] - título+diretor` → Título + diretor batem (sem ano)
3. `[categoria] - título+prod` → Título + produtora batem (sem ano)
4. `[categoria] - final dir` → Match FINAL com diretor + ano ±2
5. `[categoria] - final prod` → Match FINAL com produtora + ano ±2

**Quando usar:** Para entender onde há "perda" no funil, diagnosticar problemas de matching

---

### 5.4 Revisao - NaoMatch (Estrg por plataforma + BR) — FINAL.xlsx

**Finalidade:** Listas para revisão MANUAL de obras que NÃO casaram

**Abas:**

1. **filmes_BR**
   - Filmes brasileiros sem match
   - Qualquer plataforma
   - BB Countries contém "BR"

2. **series_BR**
   - Séries brasileiras sem match
   - Qualquer plataforma
   - BB Countries contém "BR"

3. **filmes_ESTR**
   - Filmes estrangeiros sem match
   - **APENAS** plataformas-alvo (lista abaixo)
   - BB Countries vazio
   - BB Directors **obrigatório** (não vazio)

4. **series_ESTR**
   - Séries estrangeiras sem match
   - **APENAS** plataformas-alvo
   - BB Countries vazio
   - BB Directors **obrigatório** (não vazio)

5. **lista_completa**
   - TODAS as listas anteriores empilhadas
   - Útil para distribuir trabalho de revisão

**Plataformas-alvo (aplicadas apenas a Estrg):**
- Filmicca
- AmazôniaFLIX
- TV Brasil Play
- UOL Play
- Cine Humberto Mauro Mais
- Reserva Imovision
- Curta!On
- Cinemateca Pernambucana

**Campos para preenchimento manual:**
- Responsável
- Nº CPB encontrado
- Título encontrado
- Observação

**Quando usar:** Para trabalho manual de busca de CPBs não encontrados automaticamente

---

## 6. REGRAS DE NEGÓCIO

### 6.1 Tolerância de Ano

```
ANO_TOL = 2
```

**Regra:** BB Year deve estar entre `(Ano Inicial - 2)` e `(Ano Final + 2)`

**Exemplo:**
- SAD: Ano Inicial = 2020, Ano Final = 2022
- BB Year aceito: 2018 a 2024

---

### 6.2 Prioridade de Match

1. **Primeiro:** Título + Ano + DIRETOR
2. **Depois:** Título + Ano + PRODUTORA (apenas UIDs não casados no 1º)

**Deduplicação:** Por `(UID, Nº CPB)` mantendo primeiro registro (keep='first')

---

### 6.3 Validação de Campos

**Diretor presente:**
```python
nonempty_str(df['BB Directors'])
# Exclui: NaN, '', 'nan', 'None', strings só com espaços
```

**País BR:**
```python
df['BB Countries'].str.contains('BR', case=False, na=False)
```

**Produtora presente:**
```python
tokens_empresas(df['Produtora_processada'])  # retorna set com >0 elementos
```

---

## 7. FLUXO RESUMIDO

```
1. LOAD INPUTS
   ↓
2. FILTRAR/LIMPAR
   - Remove duplicatas por UID
   - Remove UIDs já processados em 2024
   - Remove plataformas indesejadas
   ↓
3. DIVIDIR BASES
   - SAD: filmes vs series
   - BB: BR vs Estr × filmes vs series (4 combinações)
   ↓
4. PROCESSAR STRINGS
   - Normaliza textos (process_string3)
   - Remove CPFs, "baixada", siglas empresariais
   - Cria campos *_processado
   - Extrai Ano Inicial/Final
   - Materializa dir_set e prod_tokens
   ↓
5. SALVAR CHECKPOINT
   → Pre-processamento bases limpas.xlsx
   ↓
6. MATCH REGRA 1 (Diretor)
   - match_pair_min() em 4 categorias
   - Gera matches_dir
   ↓
7. MATCH REGRA 2 (Produtora)
   - match_pair_produtora() em 4 categorias
   - Apenas UIDs não casados na Regra 1
   - Gera matches_prod
   ↓
8. UNIÃO FINAL
   - matches_all = concat([matches_dir, matches_prod])
   - Deduplicação por (UID, Nº CPB)
   ↓
9. EXPORT MATCHES
   → Auditoria - matches consolidado.xlsx
   ↓
10. EXPORT ESTÁGIOS (Funil)
    → Auditoria - estágios por categoria.xlsx
    ↓
11. LISTAS DE REVISÃO
    - Filtra não-casados por categoria
    - BR: qualquer plataforma
    - Estrg: apenas plataformas-alvo + diretor presente
    → Revisao - NaoMatch... FINAL.xlsx
```

---

## 8. MÉTRICAS-CHAVE

### Contagem de Matches

```python
# Total de pares encontrados
len(matches_all)

# Pares por categoria e regra
matches_all.groupby(['categoria', 'match_rule']).size().unstack(fill_value=0)

# UIDs únicos casados (BB)
matches_all['UID'].nunique()

# CPBs únicos casados (SAD)
matches_all['Nº CPB'].nunique()
```

### Conversão por Etapa

Consultar saída da função `funil_uid_com_prod()` para cada categoria:
- `conv_total_titulo_%` → % de UIDs que batem no título
- `conv_titulo→diretor_%` → % dos títulos que também batem diretor
- `conv_titulo→produtora_%` → % dos títulos que também batem produtora
- `conv_total_%` → % de UIDs finalmente casados (após aplicar ano ±2)

---

## 9. DICAS DE RETOMADA

### Cenário 1: Reprocessar tudo do zero
1. Execute células da seção 1 (Carregamento)
2. Execute células da seção 2 (Limpeza inicial)
3. Execute células da seção 3 (Processamento de strings)
4. Checkpoint salvo em `Pre-processamento bases limpas.xlsx`

### Cenário 2: Já tenho o checkpoint
1. Carregue `Pre-processamento bases limpas.xlsx`:
```python
arquivo = 'Pre-processamento bases limpas.xlsx'
filmesBR_SAD_limpo = pd.read_excel(arquivo, sheet_name='Filmes SAD')
seriesBR_SAD_limpo = pd.read_excel(arquivo, sheet_name='Séries SAD')
filmesBR_BB_limpo = pd.read_excel(arquivo, sheet_name='Filmes BR BB')
seriesBR_BB_limpo = pd.read_excel(arquivo, sheet_name='Séries BR BB')
filmesEstr_BB_limpo = pd.read_excel(arquivo, sheet_name='Filmes Estr BB')
seriesEstr_BB_limpo = pd.read_excel(arquivo, sheet_name='Séries Estr BB')
```
2. Pule para seção 4 (Matching)

### Cenário 3: Revisar matches já gerados
- Abra `Auditoria - matches consolidado.xlsx`
- Aba `consolidado`: ver todos os pares
- Aba `resumo_categoria`: ver contagens

### Cenário 4: Trabalho manual de revisão
- Abra `Revisao - NaoMatch... FINAL.xlsx`
- Use aba `lista_completa` para ver tudo junto
- Preencha campos: Responsável, Nº CPB encontrado, Título encontrado, Observação

### Cenário 5: Debug de regras
- Abra `Auditoria - estágios por categoria.xlsx`
- Compare abas de cada etapa do funil
- Identifique onde há maior "perda" de registros

---

## 10. OBSERVAÇÕES IMPORTANTES

### Limitações Conhecidas
1. Não há busca fuzzy de títulos (apenas exato após normalização)
2. Tolerância de ano fixa (±2 anos)
3. Diretor: requer pelo menos 1 sobrenome em comum
4. Produtora: requer pelo menos 1 token em comum (>2 letras)

### Campos Críticos
- **Nº CPB** pode aparecer como "N° CPB" (variação Unicode)
- **BB Countries** pode conter múltiplos países separados por vírgula
- **BB Directors** pode conter múltiplos diretores separados por vírgula
- **BB Production Companies** pode diferir de **BB Primary Company**

### Plataformas Excluídas (34 total)
Lista completa em `PLATAFORMAS_EXCLUIR` na seção 1.1 do notebook

### Plataformas-Alvo para Estrg (8 total)
Lista completa em `PLATAFORMAS_ALVO` na última seção do notebook

---

## 11. TROUBLESHOOTING

### Problema: "Nº CPB não encontrado"
- Verifique se não é "N° CPB" (Unicode diferente)
- Use: `cpb_col = 'Nº CPB' if 'Nº CPB' in df.columns else 'N° CPB'`

### Problema: "Diretor vazio nas listas Estrg"
- Garantir uso de `nonempty_str()` em vez de `.str.strip() != ''`
- Função captura mais casos: NaN, 'nan', 'None', espaços invisíveis

### Problema: "Muitos falsos positivos"
- Revisar tolerância de ano (ANO_TOL)
- Revisar critério de sobrenome (surnames_from_raw)
- Considerar adicionar mais campos de validação

### Problema: "Poucos matches encontrados"
- Verificar normalização de strings (process_string3)
- Verificar se campos *_processado foram criados
- Consultar `Auditoria - estágios por categoria.xlsx` para ver funil
