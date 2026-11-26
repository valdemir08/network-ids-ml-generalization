# Artefatos experimentais — pcc-v1.0

Este documento descreve os artefatos associados aos experimentos de avaliação da generalização de modelos supervisionados aplicados à detecção de intrusão. A versão `pcc-v1.0` disponibiliza os arquivos `datasets.zip` e `modelos.zip`. O código-fonte, os notebooks e os resultados intermediários e finais permanecem no repositório do projeto.

## `datasets.zip`

Contém as versões finais dos três datasets utilizadas nos experimentos, armazenadas no formato Parquet:

- `cicids2017_sampled.parquet`
- `cicids2017_train.parquet`
- `cicids2017_test.parquet`
- `unsw_nb15_sampled.parquet`
- `unsw_nb15_train.parquet`
- `unsw_nb15_test.parquet`
- `iot23_sampled.parquet`
- `iot23_train.parquet`
- `iot23_test.parquet`

Os arquivos com o sufixo `_sampled` contêm as amostras formadas antes da divisão dos dados. Cada amostra possui 336.415 fluxos, sendo 269.132 benignos e 67.283 de ataque. Os arquivos com os sufixos `_train` e `_test` correspondem, respectivamente, às partições estratificadas de treinamento (70%) e teste (30%).

O campo `label_binary` registra os rótulos binários `BENIGN` e `ATTACK`. Essas denominações são mantidas nos arquivos para preservar a compatibilidade com o código do projeto.

Os arquivos são versões processadas dos datasets públicos CICIDS2017, UNSW-NB15 e IoT-23. A utilização e a redistribuição dos dados devem observar as condições estabelecidas pelas fontes originais.

## `modelos.zip`

Contém 30 pipelines finais armazenados no formato Joblib. Para cada um dos cinco algoritmos, são disponibilizados seis modelos, correspondentes às três bases individuais e às três combinações de duas bases utilizadas como origens de treinamento.

As pastas são organizadas por algoritmo:

- `decision_tree`
- `random_forest`
- `xgboost`
- `linear_svm`
- `mlp`

Em cada pasta, o nome do arquivo identifica a origem de treinamento:

- `cicids2017.joblib`
- `unsw_nb15.joblib`
- `iot23.joblib`
- `cicids2017_unsw_nb15.joblib`
- `cicids2017_iot23.joblib`
- `unsw_nb15_iot23.joblib`

Cada arquivo contém o pipeline vencedor da respectiva busca de hiperparâmetros, reajustado com toda a partição de treinamento da origem correspondente. O pipeline inclui as transformações de pré-processamento e o modelo treinado.

## Ambiente de execução

Os experimentos foram executados com Python 3.13.5. As versões das demais dependências estão registradas em `requirements.txt`. Para reduzir problemas de compatibilidade na leitura dos arquivos Joblib, recomenda-se utilizar as mesmas versões, especialmente de Joblib, scikit-learn e XGBoost.

## Organização dos demais resultados

- `notebooks/`: processamento, treinamento, avaliação e análises;
- `results/model_training/`: resultados da busca de hiperparâmetros;
- `results/model_evaluation/`: métricas das avaliações finais;
- `results/friedman_test/`: resultados dos testes estatísticos;
