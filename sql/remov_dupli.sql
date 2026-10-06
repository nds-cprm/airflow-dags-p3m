--Query para remover processos duplicados
--Faz a procura de registros duplicados com base no DSProcesso, QTAreaHa e Geometria(shape)
--seleciona o menor objectid entre os registros como sendo o primeiro registro daquele processo como min_obj
--após isso apaga da tabela original os registros que repetem os campos de verificação e tem o objectid menor que o identificado na sbq
delete from anm.fc_processototal ft
using (
	select ftt.dsprocesso, ftt.qtareaha, ftt.shape, min(ftt.objectid) as min_obj
	from anm.fc_processototal ftt
	group by ftt.dsprocesso, ftt.qtareaha, ftt.shape  
	having count(*) > 1
) as sbq
where ft.dsprocesso = sbq.dsprocesso 
	and ft.qtareaha = sbq.qtareaha 
	and ft.shape = sbq.shape
	and ft.objectid > sbq.min_obj;