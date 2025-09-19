# Documentação do Pipeline de Ingestão e Histórico de Catálogo VOD

**Versão:** 1.1
**Data:** 18 de setembro de 2025

---

## 1. Objetivo do Projeto

O objetivo deste projeto é construir um pipeline de dados robusto para processar snapshots semanais do catálogo de plataformas de VOD no Brasil. O pipeline visa criar e manter uma base de dados histórica, deduplicada e enriquecida que rastreie a disponibilidade de cada conteúdo ao longo do tempo, registrando suas datas de entrada e saída. O fim último é permitir análises aprofundadas sobre a presença do conteúdo nacional no streaming, cruzando esta base com dados específicos de obras brasileiras.

## 2. Filosofia e Arquitetura do Pipeline

As escolhas de arquitetura foram feitas para priorizar a **robustez, manutenibilidade, performance e eficiência de memória**.

-   **Arquitetura em Camadas:** O pipeline separa os dados em camadas lógicas (Raw, Staging, Processed, Final), garantindo que a fonte da verdade seja preservada e que cada etapa de transformação seja clara e auditável.
-   **Formato Parquet:** Os dados brutos `.xlsx` são imediatamente convertidos para o formato **Apache Parquet**. Esta escolha se deve à sua alta performance de leitura, compressão eficiente (reduzindo custos de armazenamento) e suporte nativo a esquemas de dados complexos.
-   **Processamento "In-Process" com DuckDB:** Em vez de depender de um servidor de banco de dados externo, utilizamos o **DuckDB**. Ele opera diretamente nos arquivos Parquet, permitindo consultas complexas (SQL) em volumes de dados maiores que a memória RAM disponível, com altíssima velocidade.
-   **Configuração Externalizada:** As regras de negócio críticas (mapeamento de colunas, lista de plataformas) são mantidas em arquivos externos (`.json`, `.xlsx`). Isso permite que usuários alterem o comportamento do pipeline sem tocar no código Python, aumentando a flexibilidade e reduzindo o risco de erros.
-   **Processamento Paralelo:** A Etapa 1, que envolve a leitura dos lentos arquivos `.xlsx`, foi paralelizada para utilizar todos os núcleos da CPU, reduzindo drasticamente o tempo de ingestão.

## 3. Inventário de Artefatos (Entradas e Saídas)

### Entradas (Controladas pelo Usuário)

-   `dados_catalogo/raw_xlsx/`: Pasta contendo os arquivos `.xlsx` brutos, que são a fonte primária de dados.
-   `mapeamento_colunas.json`: Arquivo de configuração que mapeia nomes de colunas dos arquivos de origem para um esquema padronizado. Controla quais colunas são mantidas e como são nomeadas.
-   `plataformas_controle.xlsx`: Arquivo de controle de negócio que define quais plataformas devem ser incluídas (`USAR = 'S'`) ou ignoradas (`USAR = 'N'`) no processamento.

### Saídas (Geradas pelo Pipeline)

-   `dados_catalogo/staging/*.parquet`: Arquivos Parquet individuais, um para cada `.xlsx` de origem, já com os nomes de colunas padronizados. Representam a "fonte da verdade" otimizada.
-   `dados_catalogo/processed/relatorio_timeline_plataformas_[timestamp].xlsx`: Relatório de auditoria que mostra a contagem de registros por plataforma ao longo de todas as semanas processadas.
-   `dados_catalogo/processed/dados_consolidados_filtrados.parquet`: Arquivo Parquet único contendo todos os dados das plataformas relevantes.
-   `dados_catalogo/processed/tabela_dimensao_limpa_para_splink.parquet`: **(Novo)** A tabela dimensão final, com um perfil único e limpo por `BB_UID`, pronta para a deduplicação.
-   `dados_catalogo/logs/`: Pasta que contém relatórios de execução detalhados de cada etapa.
-   `manifesto_xlsx.json`: Arquivo de controle interno para evitar o reprocessamento de arquivos `.xlsx` já ingeridos.
-   **(Futuro) `tabela_vinculos.parquet`**: Saída da etapa de deduplicação, mapeando `BB_UID`s para um ID canônico.
-   **(Futuro) `base_historica.parquet`**: O artefato final do projeto, contendo o histórico completo com datas de entrada/saída.

## 4. Modelo Operacional (Execução Única vs. Contínua)

O pipeline foi projetado para operar em dois modos distintos:

### Momento 0: Carga Inicial (Execução Única)

-   **Objetivo:** Processar todo o histórico de arquivos `.xlsx` disponíveis pela primeira vez para construir a base inicial.
-   **Processo:** Envolve a execução de todas as etapas do pipeline (1, 2, 3, etc.) sobre o conjunto completo de arquivos. É uma operação intensiva em processamento, mas realizada apenas uma vez. **É nesta fase que nos encontramos atualmente.**

### Momento 2: Carga Incremental (Execução Contínua)

-   **Objetivo:** Atualizar a base histórica com os novos arquivos `.xlsx` que chegam semanalmente ou quinzenalmente.
-   **Processo:** Requer **versões otimizadas** dos scripts. Em vez de reprocessar todo o histórico, os scripts irão ler o estado anterior (ex: o último relatório de timeline, a base histórica existente) e apenas processar e incorporar os dados novos. Esta otimização será crucial para manter a execução rápida e eficiente no futuro.

## 5. Detalhamento das Etapas do Pipeline

### Etapa 1: Ingestão e Padronização
-   **Notebook:** `1_conversao_paralela.ipynb`
-   **Objetivo:** Converter os dados brutos para um formato performático e padronizar o esquema de colunas.
-   **Lógica Principal:** Um script Python lê cada `.xlsx`, aplica as regras do `mapeamento_colunas.json` para renomear/descartar colunas, e salva o resultado em Parquet na pasta `/staging`.
-   **Escolhas de Design:** O uso de `multiprocessing` foi escolhido para acelerar a leitura dos arquivos `.xlsx`, que é o principal gargalo de performance nesta etapa.

### Etapa 2: Validação e Filtragem
-   **Notebook:** `2_validacao_e_filtragem.ipynb`
-   **Objetivo:** Realizar uma análise de consistência nos dados e filtrar apenas as plataformas relevantes para o negócio.
-   **Lógica Principal:** Um notebook usa o DuckDB para ler todos os arquivos da camada Staging e gerar um relatório cronológico visual. Ele compara as plataformas encontradas com as regras do `plataformas_controle.xlsx` e produz um único arquivo Parquet consolidado e filtrado.
-   **Escolhas de Design:** A geração de um relatório visual com `pandas.Styler` foi implementada para facilitar a detecção de anomalias (entradas, saídas, variações bruscas) pelo usuário a cada execução.

### Etapa 3: Preparação da Tabela Dimensão e Limpeza Final
-   **Notebook:** `3_preparacao_e_limpeza_final.ipynb`
-   **Objetivo:** Transformar os ~15 milhões de registros temporais em uma tabela dimensão limpa e otimizada (~225 mil perfis únicos), pronta para o Splink.
-   **Sub-etapas:**
    1.  **Diagnóstico:** Análise de completude e estabilidade das colunas para informar a seleção de atributos. Detecção de semanas com anomalias de idioma para exclusão.
    2.  **Crunch Temporal:** Utiliza DuckDB para agrupar os dados por `BB_UID` e selecionar o perfil mais recente de cada obra, com base na premissa de negócio de que "o dado mais recente é o mais confiável".
    3.  **Limpeza Final:** Aplica normalizações de string (minúsculas, remoção de acentos), padroniza listas (elenco, diretores) e garante a consistência dos tipos de dados.
-   **Saída Principal:** O arquivo `tabela_dimensao_limpa_para_splink.parquet`.

### Etapa 4: Deduplicação Interna (Próxima Etapa)
-   **Notebook:** `4_deduplicacao_com_splink.ipynb`
-   **Objetivo:** Identificar e agrupar múltiplos `BB_UID`s que representam a mesma obra.
-   **Lógica Principal:** Utilizar a biblioteca `Splink` para realizar um linkage probabilístico de registros. O processo envolve:
    1.  **Segmentação:** Separar os dados em "universos" (ex: obras com metadados ricos vs. obras com dados esparsos) para aplicar modelos distintos e mais precisos.
    2.  **Definição do Modelo:** Escolher regras de bloqueio e de comparação com base na análise exploratória.
    3.  **Treinamento e Avaliação:** Treinar o modelo em uma amostra de dados e avaliar sua performance com as ferramentas visuais do Splink.
    4.  **Execução Final:** Aplicar o modelo treinado ao conjunto de dados completo para gerar a Tabela de Vínculos.
-   **Escolhas de Design:** O Splink foi escolhido por ser uma ferramenta poderosa para lidar com a "sujeira" de dados do mundo real (pequenos erros de digitação, nomes abreviados), onde regras exatas de "match" falhariam.

## 6. Limitações, Riscos e Desafios Futuros

-   **Dependência de Ambiente:** A dificuldade que encontramos com a instalação do Splink destaca a sensibilidade do pipeline ao ambiente Python. A execução em máquinas diferentes pode exigir a criação de ambientes virtuais bem definidos para garantir a consistência das versões das bibliotecas.
-   **Qualidade dos Dados de Entrada:** A eficácia do Splink depende de uma qualidade mínima dos dados. Se um mesmo `BB_UID` for erroneamente associado a obras completamente distintas nos dados de origem (como o caso "Peaky Blinders"), o modelo de deduplicação precisará de regras de comparação robustas para separá-los corretamente.
-   **Escalabilidade a Longo Prazo:** O DuckDB é extremamente performático, mas para um horizonte de muitos anos e um crescimento para centenas de milhões de registros, a migração para uma solução de Data Warehouse em nuvem (como Google BigQuery, Snowflake) ou um Data Lakehouse pode ser necessária.
-   **Manutenção dos Arquivos de Controle:** A precisão do pipeline depende diretamente da manutenção cuidadosa dos arquivos `mapeamento_colunas.json` e `plataformas_controle.xlsx`. Um erro humano nestes arquivos pode levar a dados incorretos ou faltantes no resultado final. O pipeline é semi-automatizado e requer supervisão.
-   **Complexidade da Lógica Incremental:** A implementação da lógica de atualização incremental para a Etapa 5 (cálculo de histórico com a regra de 22 dias) será complexa e precisará ser cuidadosamente testada para lidar com todos os casos de borda (ex: uma obra que entra, sai, e retorna após o gap).

## 7. Guia de Execução e Dependências

O projeto requer um ambiente Python (preferencialmente virtual) com as seguintes bibliotecas principais instaladas:

```bash
pip install pandas openpyxl duckdb jupyterlab
pip install unidecode
pip install splink
```
