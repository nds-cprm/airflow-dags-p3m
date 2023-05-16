--Query em bloco para atualização dos índices das tabelas do eschema
--executada após todos os tratamentos
--sch_name é o nome do eschema no qual estão armazenadas as tabelas a serem reindexadas

do $$
declare
	r record;
	sch_name varchar := 'etl';--substituir nome do schema
begin
	for r in (select t.table_name from information_schema."tables" t where t.table_schema =sch_name)
	loop
		execute format('reindex (verbose) table %I.%I',sch_name, r.table_name);
	end loop;
end$$;