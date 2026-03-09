--Conjunto de query que constroem a tabela de apoio para todas as geometrias compostas pela FC_ProcessoAtivo, FC_Disponibilidade e FC_Arrentamento

truncate table anm."FC_ProcessoTotal";

insert into anm."FC_ProcessoTotal" ("QTAreaHA","DSProcesso","SHAPE") select fpa."QTAreaHA",fpa."DSProcesso",fpa."SHAPE"  from anm."FC_ProcessoAtivo" fpa;

insert into anm."FC_ProcessoTotal" ("QTAreaHA","DSProcesso","SHAPE") select fd."QTAreaHA",fd."DSProcesso",fd."SHAPE" from anm."FC_Disponibilidade" fd;

insert into anm."FC_ProcessoTotal" ("QTAreaHA","DSProcesso","SHAPE") select fa."QTAreaHA",fa."DSProcesso",fa."SHAPE"from anm."FC_Arrendamento" fa;