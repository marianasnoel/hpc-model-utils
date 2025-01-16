# hpc-model-utils

Aplicação CLI para auxiliar no processamento de tarefas associadas aos modelos NEWAVE / DECOMP / DESSEM em ambiente HPC.

A versão atual do `hpc-model-utils` imagina que exista uma entidade externa (ModelOps) que divida o fluxo de execução dos modelos em etapas, com integração direta com o S3 da AWS.

Para cada etapa do fluxo de execução é disponibilizado um comando da CLI:

1 - Validação e aquisição do S3 dos executáveis dos modelos nas respectivas versões
2 - Extração e validação dos dados entrada do S3 e tratamento de `encoding`
3 - Hashing das entradas e geração de um identificador único
4 - Tarefas de pré-processamento específicas de cada modelo
5 - Execução do modelo através da submissão para um scheduler (SLURM)
6 - Diagnóstico do status da execução (sucesso, erro, etc.) com base nas saídas
6 - Tarefas de pós-processamento específicas de cada modelo
7 - Compressão e limpeza das saídas produzidas
8 - Upload das saídas para o S3

## Instalação

Apesar de ser um módulo `python`, o hpc-model-utils não está disponível nos repositórios oficiais. Para realizar a instalação, é necessário fazer o download do código a partir do repositório e fazer a instalação manualmente:

```
$ git clone https://github.com/marianasnoel/hpc-model-utils
$ cd hpc-model-utils
$ pip install .
```

## Funcionalidades Gerais

### Compressão Paralela

Uma das funcionalidades fornecidas pelo `hpc-model-utils` é a compressão de arquivos em paralelo para o formato `.zip`. Não existe de maneira fácil um programa de linux que realize paralelismo na compressão de arquivos da maneira que é desejada para acelerar a compressão das saídas dos modelos de planejamento energético, onde existem muitos arquivos de tamanho reduzido e poucos arquivos grandes.

Algumas alternativas de compressão como o [pigz](https://zlib.net/pigz/) e [pbzip2](https://linux.die.net/man/1/pbzip2) não são as mais adequadas para o processo, visto que elas realizam paralelismo na compressão de um mesmo arquivo, o que é ineficiente na maioria das vezes e ainda seria lento no caso em questão. Mais sobre a infeciência de paralelizar a compressão de arquivos é encontrado [aqui](https://stackoverflow.com/questions/66989293/parallel-zipping-of-a-single-large-file).

Por isso, foi extraída [deste](https://github.com/urishab/ZipFileParallel) repositório aberto uma classe Python para paralelizar a compressão de diversos arquivos. Esta classe foi adaptada para que fosse fornecida uma lista de arquivos e, a partir de um `pool` de processadores que funcionam de maneira assíncrona, fosse escrito em um mesmo arquivo `.zip` o resultado da compressão de cada arquivo da lista, feita de maneira independente por cada processador. Isto se mostrou essencial para lidar com o número de arquivos de saída do modelo NEWAVE individualizado.

## Funcionalidades Disponíveis por Modelo

### Pré-processamento específico

TODO

### Diagnóstico do `STATUS` da execução

Para cada modelo, o `hpc-model-utils` realiza um processamento dos arquivos de saída para avaliar se a execução realizada terminou em sucesso ou erro e, neste caso, qual a provável fonte do erro. Os possíveis valores são:

- SUCCESS
- INFEASIBLE
- DATA_ERROR
- RUNTIME_ERROR
- COMMUNICATION_ERROR
- UNKNOWN

Nem todos os modelos devem possuir informações suficientes para diagnosticar cada um desses possíveis `status`. Geralmente o diagnóstico resultará em `SUCCESS`, `DATA_ERROR`, `RUNTIME_ERROR` ou `INFEASIBLE`.

#### NEWAVE

Para realizar o diagnóstico, o NEWAVE faz uso dos arquivos `caso.dat`, `arquivos.dat`, `dger.dat` e `pmo.dat`.

#### DECOMP

Para realizar o diagnóstico, o DECOMP faz uso dos arquivos `caso.dat`, `rvX`, `dadger.rvX`, `relato.rvX` e `inviab_unic.rvX`.

#### DESSEM

TODO

### Pós-processamento específico

TODO

### Produção de arquivos para ambientes analíticos (sínteses)

#### NEWAVE

A execução do modelo também realiza a chamada ao [sintetizador-newave](https://github.com/rjmalves/sintetizador-newave).

#### DECOMP

A execução do modelo também realiza a chamada ao [sintetizador-decomp](https://github.com/rjmalves/sintetizador-decomp).

#### DESSEM

A execução do modelo também realiza a chamada ao [sintetizador-dessem](https://github.com/rjmalves/sintetizador-dessem).
