--Query para remover processos duplicados
--Faz a procura de registros duplicados com base no DSProcesso, QTAreaHa e Geometria(shape)
--seleciona o menor objectid entre os registros como sendo o primeiro registro daquele processo como min_obj
--após isso apaga da tabela original os registros que repetem os campos de verificação e tem o objectid menor que o identificado na sbq
delete from anm."FC_ProcessoTotal" ft
using (
	select ftt."DSProcesso", ftt."QTAreaHA", ftt."SHAPE", min(ftt."OBJECTID") as min_obj
	from anm."FC_ProcessoTotal"ftt
	group by ftt."DSProcesso", ftt."QTAreaHA", ftt."SHAPE"  
	having count(*) > 1
) as sbq
where ft."DSProcesso" = sbq."DSProcesso" 
	and ft."QTAreaHA" = sbq."QTAreaHA" 
	and ft."SHAPE" = sbq."SHAPE"
	and ft."OBJECTID" > sbq.min_obj;