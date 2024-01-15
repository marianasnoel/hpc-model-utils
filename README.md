# tuber
Conjunto de scripts para auxiliar no pré-processamento, execução e pós-processamento de NEWAVE / DECOMP / DESSEM em ambiente HPC, também executando programas auxiliares quando necessário.

A versão atual do `tuber` contém scripts para executar os programas NEWAVE e DECOMP em ambiente gerenciado com Sun Grid Engine (SGE) e o modelo DESSEM em ambiente livre, sem gerenciador de filas.

## Instalação

Apesar de ser um módulo `python`, o tuber não está disponível nos repositórios oficiais. Para realizar a instalação, é necessário fazer o download do código a partir do repositório e fazer a instalação manualmente:

```
$ git clone https://github.com/marianasnoel/tuber
$ cd tuber
$ pip install -r requirements.txt
```

## Funcionalidades Gerais

### Compressão Paralela

Uma das funcionalidades fornecidas pelo `tuber` é a compressão de arquivos em paralelo para o formato `.zip`. Não existe de maneira fácil um programa de linux que realize paralelismo na compressão de arquivos da maneira que é desejada para acelerar a compressão das saídas dos modelos de planejamento energético, onde existem muitos arquivos de tamanho reduzido e poucos arquivos grandes.

Algumas alternativas de compressão como o [pigz]() e [pbzip2]() não são as mais adequadas para o processo, visto que elas realizam paralelismo na compressão de um mesmo arquivo, o que é ineficiente na maioria das vezes e ainda seria lento no caso em questão. Mais sobre a infeciência de paralelizar a compressão de arquivos é encontrado [aqui](https://stackoverflow.com/questions/66989293/parallel-zipping-of-a-single-large-file).

Por isso, foi extraída [deste](https://github.com/urishab/ZipFileParallel) repositório aberto uma classe Python para paralelizar a compressão de diversos arquivos. Esta classe foi adaptada para que fosse fornecida uma lista de arquivos e, a partir de um `pool` de processadores que funcionam de maneira assíncrona, fosse escrito em um mesmo arquivo `.zip` o resultado da compressão de cada arquivo da lista, feita de maneira independente por cada processador. Isto se mostrou essencial para lidar com o número de arquivos de saída do modelo NEWAVE individualizado.

## Funcionalidades Disponíveis por Modelo

### NEWAVE

O modelo NEWAVE é executado pelo `jobs/mpi_newave.job`, que permite declarar tanto o número de cores alocados para a sua execução quanto a versão do modelo a ser utilizada. Uma chamada simples é:

`qsub -cwd -V -N $CASO -pe orte $NUM_PROC mpi_newave.job $VERSAO $NUM_PROC`

São suportados argumentos opcionais que podem ser fornecidos através das palavras-chave `sintetizador` e `posproc`, que são encaminhados para as respectivas etapas durante a execução do job.

Todos os argumentos passados após a palavra `sintetizador` são redirecionados para a chamada do [sintetizador-newave](), que é feita após a execução dos programas auxiliares NWLISTCF e NWLISTOP. Já os argumentos passados após a palavra `posproc` são redirecionados para o script `pos_processa_newave.py`, que é responsável pela divisão e compactação dos arquivos.

### DECOMP

O modelo DECOMP é executado pelo `jobs/mpi_decomp.job`, que permite declarar tanto o número de cores alocados para a sua execução quanto a versão do modelo a ser utilizada. Uma chamada simples é:

`qsub -cwd -V -N $CASO -pe orte $NUM_PROC mpi_decomp.job $VERSAO $NUM_PROC`

### DESSEM

O modelo DESSEM é executado pelo `jobs/dessem.sh`, que permite declarar apenas a versão do modelo a ser utilizada. Este script não suporta ambientes de gerenciamento de filas, pois não é o modo atual de uso internamente. Uma chamada simples é:

`./dessem.sh $VERSAO`

