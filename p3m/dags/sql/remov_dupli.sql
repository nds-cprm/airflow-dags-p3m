--Query para remover processos duplicados
--Faz a procura de registros duplicados com base no DSProcesso, QTAreaHa e Geometria(shape)
--seleciona o menor objectid entre os registros como sendo o primeiro registro daquele processo como min_obj
--após isso apaga da tabela original os registros que repetem os campos de verificação e tem o objectid menor que o identificado na sbq
delete from etl."FC_ProcessoAtivo" fpa
using (
	select fp."DSProcesso", fp."QTAreaHA", fp."SHAPE", min(fp."OBJECTID") as min_obj
	from etl."FC_ProcessoAtivo"fp
	group by fp."DSProcesso", fp."QTAreaHA", fp."SHAPE"  
	having count(*) > 1
) as sbq
where fpa."DSProcesso" = sbq."DSProcesso" 
	and fpa."QTAreaHA" = sbq."QTAreaHA" 
	and fpa."SHAPE" = sbq."SHAPE"
	and fpa."OBJECTID" > sbq.min_obj;