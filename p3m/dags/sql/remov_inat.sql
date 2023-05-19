--Query para remover processos inativos
--Para identificar os processos inativos FC_ProcessoAtivo é feito um left join com a tabela TB_processos com o filtro de processo ativo
--então é utilizado a clausula buscando apenas os valores nulos baseado em uma coluna da TB_processo ...
-- uma vez que deveria ser preenchido em caso de correspondecia com filtro de ativos
--posteriormente são deletados aquele com valor nulo

delete from anm."FC_ProcessoAtivo" fp2 
using (select fp."DSProcesso"
		from anm."FC_ProcessoAtivo" fp
		left join anm."TB_Processo" tp on fp."DSProcesso"= tp."DSProcesso" and tp."BTAtivo" ='S'
		where tp."IDTipoRequerimento" is null) as subquery
where subquery."DSProcesso" = fp2."DSProcesso";