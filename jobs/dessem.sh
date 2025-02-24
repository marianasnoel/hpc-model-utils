#
# shell script para executar o modelo DESSEM on-premises
#

# Variaveis importantes

CASO=`basename $PWD`
WORKDIR=$PWD

# Processa parametros de entrada
VERSAO=$1
SLOTS=$2
echo Versao do DESSEM: $VERSAO
echo Numero de slots: $SLOTS

INSTALLDIR=/home/ESTUDO/PEM/git/hpc-model-utils
INTERPRETADOR=$INSTALLDIR/venv/bin/python3

DIR_DESSEM=/home/SW/dessem
DESSEM=$DIR_DESSEM/dessem_${VERSAO}
SINTETIZADOR=sintetizador-dessem
OPCAO=completa

echo Mudando o diretorio para $WORKDIR
cd $WORKDIR

echo Configurando o numero de processadores
$INTERPRETADOR $INSTALLDIR/main.py pre_processa_dessem $SLOTS

echo Executando o DESSEM
$DESSEM

if [ $? -eq 0 ]
then
    echo Sintetizando
    $SINTETIZADOR $OPCAO

    # Pos processamento dessem
    $INTERPRETADOR $INSTALLDIR/main.py pos_processa_dessem
else
    echo Erro na execução do DESSEM: $?
fi

