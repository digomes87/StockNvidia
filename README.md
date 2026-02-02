# 📊 NVIDIA Stock Analysis Pipeline

Pipeline completa de análise de dados das ações da NVIDIA utilizando **PySpark** para processamento distribuído e análise técnica avançada.

## 🎯 Funcionalidades

### 📥 Carregamento de Dados
- Download automático do dataset do Kaggle
- Validação de qualidade dos dados
- Schema tipado para melhor performance

### 🔄 Processamento de Dados
- Limpeza e tratamento de valores ausentes
- Conversão de tipos de dados
- Features temporais (ano, mês, trimestre, dia da semana)
- Features de preço (retornos, ranges, ratios)
- Features de volume

### 🔧 Engenharia de Features
- **Médias Móveis Simples (SMA)**: 7, 14, 30, 50, 200 dias
- **Médias Móveis Exponenciais (EMA)**: 12, 26 dias
- **Bandas de Bollinger**: Upper, Middle, Lower bands
- **RSI (Relative Strength Index)**: Indicador de momentum
- **MACD**: Moving Average Convergence Divergence
- **Métricas de Volatilidade**: 7, 14, 30, 60 dias
- **ATR (Average True Range)**
- **Indicadores de Momentum**
- **Níveis de Suporte e Resistência**

### 📊 Análises
- Estatísticas gerais do dataset
- Análise temporal (anual, mensal, trimestral)
- Padrões por dia da semana
- Identificação de melhores/piores dias
- Métricas de risco (Volatility, VaR, Sharpe Ratio)
- Análise de tendências de volume
- Sinais técnicos atuais

### 📈 Visualizações
- Histórico de preços e volume
- Médias móveis
- Bandas de Bollinger
- RSI
- MACD
- Análise de volatilidade
- Performance anual
- Padrões de dia da semana

## 📁 Estrutura do Projeto

```
nvidia-stock-analysis/
├── main.py                      # Script principal da pipeline
├── config/
│   ├── analysis_config.yaml     # Configurações de análise
│   └── spark_config.yaml        # Configurações do Spark
├── notebooks/
│   └── exploratory_analysis.ipynb  # Análise exploratória
├── src/
│   ├── __init__.py
│   ├── data_loader.py          # Carregamento de dados
│   ├── data_processor.py       # Processamento de dados
│   ├── feature_engineer.py     # Engenharia de features
│   ├── analyzer.py             # Análises estatísticas
│   └── visualizer.py           # Visualizações
├── requirements.txt            # Dependências Python
├── .gitignore
└── README.md                   # Este arquivo
```

## 🚀 Como Usar

### 1. Instalação

```bash
# Clone o repositório
cd nvidia-stock-analysis

# Crie um ambiente virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instale as dependências
pip install -r requirements.txt
```

### 2. Configuração do Kaggle

Para baixar o dataset, você precisa configurar suas credenciais do Kaggle:

```bash
# Faça login no Kaggle e vá para Account Settings
# Crie um novo API token (kaggle.json)
# Coloque o arquivo em:
mkdir ~/.kaggle
mv kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

### 3. Executar a Pipeline

```bash
# Execute o script principal
python main.py
```

A pipeline irá:
1. ✅ Criar sessão Spark
2. ✅ Baixar dados do Kaggle
3. ✅ Processar e limpar dados
4. ✅ Criar features técnicas
5. ✅ Executar análises
6. ✅ Gerar visualizações
7. ✅ Salvar resultados

### 4. Resultados

Os resultados serão salvos em `output/`:

```
output/
├── processed_data.parquet          # Dataset processado
├── processed_data_sample.csv       # Amostra em CSV
├── yearly_analysis.csv             # Análise anual
├── monthly_analysis.csv            # Análise mensal
├── quarterly_analysis.csv          # Análise trimestral
├── weekday_analysis.csv            # Análise por dia da semana
└── visualizations/
    ├── price_history.png
    ├── moving_averages.png
    ├── bollinger_bands.png
    ├── rsi_indicator.png
    ├── macd_indicator.png
    ├── volatility_analysis.png
    ├── yearly_performance.png
    └── weekday_patterns.png
```

## ⚙️ Configuração

### Spark Config (`config/spark_config.yaml`)

```yaml
spark:
  app_name: "NVIDIA Stock Analysis"
  master: "local[*]"
  driver_memory: "4g"
  executor_memory: "4g"
```

### Analysis Config (`config/analysis_config.yaml`)

```yaml
analysis:
  moving_averages: [7, 14, 30, 50, 200]
  bollinger_bands:
    window: 20
    num_std: 2
  rsi:
    window: 14
  macd:
    fast_period: 12
    slow_period: 26
    signal_period: 9
```

## 📊 Indicadores Técnicos

### Médias Móveis (SMA)
Identificam tendências de longo prazo e níveis de suporte/resistência dinâmicos.

### Bandas de Bollinger
Medem a volatilidade e identificam condições de sobrecompra/sobrevenda.

### RSI (Relative Strength Index)
- RSI < 30: Sobrevenda (possível sinal de compra)
- RSI > 70: Sobrecompra (possível sinal de venda)

### MACD
- MACD > Signal: Tendência de alta (bullish)
- MACD < Signal: Tendência de baixa (bearish)

### Sharpe Ratio
Mede retorno ajustado ao risco. Quanto maior, melhor a relação risco-retorno.

## 🔧 Customização

### Adicionar Novos Indicadores

Edite `src/feature_engineer.py`:

```python
def add_custom_indicator(self, df: DataFrame) -> DataFrame:
    # Seu código aqui
    return df
```

### Modificar Análises

Edite `src/analyzer.py`:

```python
def custom_analysis(self, df: DataFrame) -> dict:
    # Seu código aqui
    return results
```

## 📚 Dependências Principais

- **PySpark 3.5.0**: Processamento distribuído
- **Pandas**: Manipulação de dados
- **Matplotlib/Seaborn**: Visualizações
- **Plotly**: Gráficos interativos
- **KaggleHub**: Download de datasets

## 🐛 Troubleshooting

### Erro de memória do Spark
Aumente a memória em `config/spark_config.yaml`:
```yaml
driver_memory: "8g"
executor_memory: "8g"
```

### Erro no download do Kaggle
Verifique suas credenciais em `~/.kaggle/kaggle.json`

## 📝 Notas

- A pipeline usa **caching** do Spark para melhorar performance
- Os dados são processados de forma **lazy** (só quando necessário)
- Visualizações são salvas em **alta resolução** (100 DPI)
- Análises são otimizadas para **grandes volumes** de dados

## 🤝 Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para:
- Reportar bugs
- Sugerir novas features
- Melhorar documentação
- Adicionar novos indicadores

## 📄 Licença

Este projeto é de código aberto e está disponível sob a licença MIT.

## 👨‍💻 Autor

Pipeline desenvolvida para análise profissional de ações usando PySpark.

---

**⚠️ Disclaimer**: Esta ferramenta é apenas para fins educacionais e de pesquisa. Não constitui aconselhamento financeiro. Sempre faça sua própria pesquisa antes de tomar decisões de investimento.