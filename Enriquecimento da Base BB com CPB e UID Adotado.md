# 🎯 Notebook: Enriquecimento da Base BB com CPB e UID Adotado

## 📋 Visão Geral

Este notebook consolida dados da base BB (Broad Beam) com informações de CPB (Certificado de Produto Brasileiro) da ANCINE, adota UIDs consolidados e processa channels para criar novas plataformas.

---

## 🎯 Objetivos

### 1. **Enriquecimento com CPB e UID Adotado**
- Adiciona colunas `UID Adotado` e `CPB` na base BB
- Se UID existe no dicionário de mapeamento → usa valores do dicionário
- Se UID não existe no dicionário → `UID Adotado = UID` e `CPB = 'N/A'`

### 2. **Substituição de Campos com Dados ANCINE**
- Para registros que **possuem CPB** (CPB ≠ 'N/A')
- Substitui 7 campos da base BB pelos valores corretos da tabela ANCINE
- Adiciona nova coluna `Classificação` (obras sem CPB = 'Estrangeira')

### 3. **Processamento de Channels**
- Cria novas plataformas a partir de channels
- Exemplo: "Paramount+ Amazon Channel" (Paramount+ ofertado dentro do Prime Video)
- Duas ações possíveis:
  - `IGNORAR`: Mantém plataforma original, limpa channel
  - `CRIAR_PLATAFORMA`: Cria nova plataforma com nome composto

### 4. **Validações e Export**
- Valida integridade dos dados processados
- Exporta base BB enriquecida final
- Gera análise de mudanças nas plataformas (debug)

---

## 📂 Arquivos de Entrada

| Arquivo | Descrição | Localização |
|---------|-----------|-------------|
| **Base BB Original** | Base de dados de streaming (Broad Beam) | `Bases/BB Media - 2025-08-28 - Brazil - ALL PLATFORMS.xlsx` |
| **Dicionário CPB ↔ UID** | Mapeamento consolidado entre CPBs e UIDs | `Bases/FINAL - Consolidação de lista de match 2025.xlsx` |
| **Dados de Obras ANCINE** | Informações oficiais das obras com CPB | `Bases/Relat_rio_de_CPBs_Emitidos__agrupado_.xlsx` |
| **Mapeamento de Channels** | Definições de como processar channels | `Bases/mapeamento_channels_manual.xlsx` |

---

## 📤 Arquivos de Saída

| Arquivo | Descrição |
|---------|-----------|
| **Base_BB_Enriquecida_YYYY-MM-DD.xlsx** | Base BB final com todos os enriquecimentos |
| **DEBUG_Analise_Plataformas_*.xlsx** | Análise detalhada de plataformas (antes/depois) |

---

## 🔄 Fluxo de Processamento
```
┌─────────────────────────────────────────────────────────────┐
│  FASE 1: IMPORTAÇÃO                                         │
├─────────────────────────────────────────────────────────────┤
│  • Base BB Original (378,618 registros)                     │
│  • Dicionário CPB ↔ UID                                     │
│  • Dados de Obras ANCINE                                     │
│  • Mapeamento de Channels                                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  FASE 2: ENRIQUECIMENTO COM UID ADOTADO E CPB              │
├─────────────────────────────────────────────────────────────┤
│  • Cria dicionário {UID: {UID_Adotado, CPB}}                │
│  • Adiciona colunas na base BB:                             │
│    - UID Adotado                                            │
│    - CPB (ou 'N/A' se não mapeado)                         │
│  • Reduz UIDs únicos através da consolidação                │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  FASE 3: SUBSTITUIÇÃO DE CAMPOS COM DADOS ANCINE           │
├─────────────────────────────────────────────────────────────┤
│  • Faz LEFT JOIN: Base BB + Tabela ANCINE (chave = CPB)    │
│  • Substitui 7 campos quando CPB ≠ 'N/A':                  │
│    1. BB Title → Título Original                           │
│    2. BB Year → Ano de Produção                            │
│    3. BB Primary Genre → Gênero                             │
│    4. BB Production Companies → Produtora                   │
│    5. BB Duration → Duração Obra                            │
│    6. BB Directors → Diretor                                │
│    7. BB Countries → Países Coprodutores                    │
│  • Adiciona coluna Classificação:                           │
│    - Com CPB → valor ANCINE                                 │
│    - Sem CPB → 'Estrangeira'                               │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  FASE 4: PROCESSAMENTO DE CHANNELS                          │
├─────────────────────────────────────────────────────────────┤
│  • Carrega mapeamento manual de channels                    │
│  • Valida se todas as combinações (Platform, Channel)       │
│    estão mapeadas                                           │
│  • Aplica transformações:                                    │
│    - IGNORAR: Mantém platform, limpa channel               │
│    - CRIAR_PLATAFORMA: Substitui por nova platform         │
│  • Cria novas plataformas compostas                         │
│    Ex: "Paramount+ - Amazon Prime Video"                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  FASE 5: VALIDAÇÕES E EXPORT                                │
├─────────────────────────────────────────────────────────────┤
│  • Valida integridade de colunas críticas                   │
│  • Verifica formato de CPBs                                 │
│  • Gera estatísticas finais                                 │
│  • Exporta base enriquecida                                 │
│  • Gera análise de plataformas (debug)                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Estrutura de Células

### **Célula 1: Imports e Configuração**
- Importa bibliotecas: pandas, numpy, pathlib, datetime
- Configura display do pandas
- Timestamp de início

### **Célula 2: Importação de Arquivos**
- Define caminhos dos arquivos de entrada
- Verifica existência de todos os arquivos
- Importa 4 arquivos:
  - Base BB Original (Sheet1)
  - Dicionário CPB ↔ UID (primeira aba)
  - Dados de Obras ANCINE (cpbs_emitidos_agrupado)
  - Mapeamento de Channels (Mapeamento)
- Exibe estatísticas básicas de cada arquivo

### **Célula 3: Enriquecimento com UID Adotado e CPB**
- Cria dicionário de mapeamento `{UID: {UID_Adotado, CPB}}`
- Usa apenas registro mais recente por UID (Ano Match)
- Aplica mapeamento na base BB:
  - Se UID mapeado → usa valores do dicionário
  - Se UID não mapeado → `UID_Adotado = UID`, `CPB = 'N/A'`
- Exibe estatísticas de cobertura e redução de UIDs

### **Célula 4: Preparação para Substituição**
- Padroniza coluna `N° CPB` da tabela ANCINE
- Cria subset com 9 campos necessários
- Remove duplicatas de CPB (mantém primeira ocorrência)
- Analisa overlap entre CPBs da base BB e tabela ANCINE
- Identifica quantos registros terão substituição

### **Célula 5: Substituição de Campos**
- Faz LEFT JOIN: Base BB + Tabela ANCINE (chave = CPB)
- Substitui 7 campos quando há dados ANCINE (CPB ≠ 'N/A'):
  1. `BB Title` ← `Título Original`
  2. `BB Year` ← `Ano de Produção`
  3. `BB Primary Genre` ← `Gênero`
  4. `BB Production Companies` ← `Produtora`
  5. `BB Duration` ← `Duração Obra`
  6. `BB Directors` ← `Diretor`
  7. `BB Countries` ← `Países Coprodutores`
- Adiciona coluna `Classificação`:
  - Com CPB → valor da ANCINE
  - Sem CPB → `'Estrangeira'`
- Remove colunas temporárias do merge
- Exibe estatísticas de substituições

### **Célula 6: Validações Finais**
- Verifica integridade de colunas críticas (nulos)
- Valida formato de CPBs (padrão: B + 13 dígitos)
- Gera distribuição de classificação
- Exibe estatísticas finais da base enriquecida
- Lista colunas com valores nulos (top 10)

### **Célula 7: Carregar Mapeamento de Channels**
- Importa arquivo `mapeamento_channels_manual.xlsx` (aba: Mapeamento)
- Valida estrutura: colunas `Platform_Name`, `Channel`, `Acao`
- Exibe distribuição por ação (IGNORAR vs CRIAR_PLATAFORMA)
- Cria dicionário `{Platform|Channel: {acao, nova_platform}}`

### **Célula 8: Validar Mapeamento de Channels**
- Define função `normalizar_channel()` (trata vazios/nulos)
- Identifica todas as combinações (Platform, Channel) na base BB
- Separa combinações com/sem channel
- Valida se todas com channel estão mapeadas
- Se houver não-mapeadas: exporta Excel e interrompe execução

### **Célula 9: Aplicar Lógica de Channels**
- Define função `processar_channel(row)`:
  - `IGNORAR`: Mantém platform original, limpa channel
  - `CRIAR_PLATAFORMA`: Substitui por nova_platform, limpa channel
- Aplica processamento em toda a base BB
- Exibe estatísticas antes/depois:
  - Plataformas únicas
  - Channels preenchidos/vazios
- Identifica plataformas novas criadas vs removidas
- Lista top 10 novas plataformas com volume de registros

### **Célula 10: Export Final**
- Gera nome de arquivo com data: `Base_BB_Enriquecida_YYYY-MM-DD.xlsx`
- Exibe estatísticas finais completas:
  - Dimensões (registros × colunas)
  - Identificadores únicos (UIDs, CPBs)
  - Plataformas e channels
  - Cobertura de CPB
  - Distribuição de classificação
- Exporta Excel (aba: Base_Enriquecida)
- Resume transformações aplicadas

### **Célula 11: Depuração - Análise de Plataformas (OPCIONAL)**
- Coleta plataformas ANTES (df_bb_final) e DEPOIS (df_bb_com_channels)
- Faz merge completo (FULL OUTER JOIN)
- Classifica cada plataforma:
  - `MANTIDA`: Existe antes e depois
  - `CRIADA`: Só existe depois (nova)
  - `REMOVIDA`: Só existe antes (sumiu)
- Calcula diferenças de registros
- Exporta Excel com 5 abas:
  1. **Analise_Completa**: Todas as plataformas com status
  2. **Removidas**: Plataformas que sumiram
  3. **Criadas**: Plataformas novas
  4. **Mantidas_com_Mudanca**: Plataformas que ganharam/perderam registros
  5. **Resumo**: Estatísticas por status

---

## 🔑 Campos Críticos

### **Campos Adicionados:**
- `UID Adotado`: UID consolidado (pode ser igual ao UID original)
- `CPB`: Certificado de Produto Brasileiro (ou 'N/A')
- `Classificação`: Classificação ANCINE (ou 'Estrangeira')

### **Campos Modificados (quando CPB ≠ 'N/A'):**
- `BB Title`
- `BB Year`
- `BB Primary Genre`
- `BB Production Companies`
- `BB Duration`
- `BB Directors`
- `BB Countries`

### **Campos Processados (channels):**
- `Platform Name`: Pode ser substituído por nova plataforma composta
- `Channel`: Normalizado/limpo após processamento

---

## 📈 Métricas Esperadas

| Métrica | Descrição |
|---------|-----------|
| **Taxa de cobertura CPB** | % de registros com CPB ≠ 'N/A' |
| **Redução de UIDs** | Quantos UIDs foram consolidados |
| **Substituições por campo** | Quantos registros tiveram cada campo substituído |
| **Plataformas criadas** | Quantas novas plataformas via channels |
| **Obras estrangeiras** | % com Classificação = 'Estrangeira' |

---

## ⚠️ Notas Importantes

### **Sobre CPBs:**
- Formato esperado: `B` + 13 dígitos (ex: B1234567890123)
- Apenas obras brasileiras recebem CPB da ANCINE
- Obras sem CPB são classificadas como 'Estrangeira'

### **Sobre Substituição de Campos:**
- Substituição ocorre **APENAS** quando há match via CPB
- Se CPB não for encontrado na tabela ANCINE, mantém valores originais
- Formato de duração mantido como está (numérico, minutos)

### **Sobre Channels:**
- Channels representam plataformas ofertadas dentro de outras
- Exemplo: Paramount+ pode ser comprado via Amazon Prime Video
- Após processamento, channel fica vazio e Platform Name muda
- Necessário preencher `mapeamento_channels_manual.xlsx` manualmente

### **Sobre o Mapeamento de Channels:**
- Arquivo deve ter colunas: `Platform_Name`, `Channel`, `Acao`, `Nova_Platform_Name`
- Ações disponíveis:
  - `IGNORAR`: Plataforma mantém nome, channel é limpo
  - `CRIAR_PLATAFORMA`: Nova plataforma composta é criada
- Se houver combinações não mapeadas, notebook interrompe e exporta lista para revisão

---

## 🛠️ Dependências
```python
pandas
numpy
pathlib
datetime
openpyxl  # Para leitura/escrita de Excel
```

---

## 🚀 Como Executar

1. **Preparar arquivos de entrada** na pasta `Bases/`:
   - Base BB original
   - Dicionário CPB ↔ UID consolidado
   - Dados de obras ANCINE
   - Mapeamento de channels (preenchido manualmente)

2. **Executar células sequencialmente** (1 → 10)
   - Célula 11 é opcional (debug)

3. **Verificar outputs:**
   - Base enriquecida em `Base_BB_Enriquecida_YYYY-MM-DD.xlsx`
   - Análise de debug (se executada) em `DEBUG_Analise_Plataformas_*.xlsx`

4. **Validar resultados:**
   - Conferir estatísticas finais
   - Revisar plataformas criadas/removidas
   - Validar taxa de cobertura CPB

---

## 📝 Changelog

**Versão 1.0** (2025-10-21)
- Implementação inicial completa
- Enriquecimento com CPB e UID Adotado
- Substituição de 7 campos com dados ANCINE
- Processamento de channels
- Célula de depuração de plataformas