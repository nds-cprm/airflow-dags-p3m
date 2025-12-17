--Query para remover processos inativos
--Para identificar os processos inativos FC_ProcessoTotal é feito um left join com a tabela TB_processos com o filtro de processo ativo
--então é utilizado a clausula buscando apenas os valores nulos baseado em uma coluna da TB_processo ...
-- uma vez que deveria ser preenchido em caso de correspondecia com filtro de ativos
--posteriormente são deletados aquele com valor nulo

delete from anm."FC_ProcessoTotal" ft2 
using (select ft."DSProcesso"
		from anm."FC_ProcessoTotal" ft
		left join anm."TB_Processo" tp on ft."DSProcesso"= tp."DSProcesso" and tp."BTAtivo" ='S'
		where tp."IDTipoRequerimento" is null) as subquery
where subquery."DSProcesso" = ft2."DSProcesso";