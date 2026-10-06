--Query para remover processos inativos
--Para identificar os processos inativos FC_ProcessoTotal é feito um left join com a tabela TB_processos com o filtro de processo ativo
--então é utilizado a clausula buscando apenas os valores nulos baseado em uma coluna da TB_processo ...
-- uma vez que deveria ser preenchido em caso de correspondecia com filtro de ativos
--posteriormente são deletados aquele com valor nulo

delete from anm.fc_processototal ft2 
using (select ft.dsprocesso
		from anm.fc_processototal ft
		left join anm.tb_processo tp on ft.dsprocesso= tp.dsprocesso and tp.btativo ='S'
		where tp.idtiporequerimento is null) as subquery
where subquery.dsprocesso = ft2.dsprocesso;
