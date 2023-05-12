#!/bin/bash

#Em caso de necessidade, o comando abaixo lista as layers presentes no arquivo
#ogrinfo ../../temp/DBANM.gdb

#lista das layers de interesse contidas dentro da .gdb
layers=("TB_Processo" "TB_ProcessoPessoa" "TB_ProcessoEvento" "TB_ProcessoMunicipio"\
                    "TB_Pessoa" "TB_ProcessoSubstancia" "FC_ProcessoAtivo")

#Comando bash do ogr em loop que salva no bd cada uma das layers de interesse da lista acima
#Indicação do serviço de banco utilizado e credenciais, o usuário necessita de Grant Usage no BD/schema alvo
#-lco launder=no flag para não forçar snakecase e manter nomenclatura padrão da base
#-progres apresenta um progresso de gravação de cada layer
#--config PG_USE_COPY YES método de inserção dos dados no banco garante maior desempenho para grandes tabelas
#--config OGR_TRUNCATE YES realiza o truncate nas tabelas presentes no banco caso já existam e atualiza com novos dados
for str in ${!layers[@]};
do
    ogr2ogr -f "PostgreSQL" PG:"host=localhost port=5433 dbname=airflow active_schema=etl user=airflow password=airflow" \
            /home/gabriel/airflow/temp/DBANM.gdb  ${layers[$str]} -lco launder=no -progress --config PG_USE_COPY YES --config OGR_TRUNCATE YES
    status=$?
    echo "O comando tem os seguintes status: ${status}"

    if [ $status -eq 0 ]
    then
        echo "Layer "${layers[$str]}" gravada no banco."
    else
        echo "Layer "${layers[$str]}" não foi gravada no banco."
        exit -1
    fi
done

