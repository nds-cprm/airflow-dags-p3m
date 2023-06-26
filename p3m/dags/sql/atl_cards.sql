--Query que atualiza o campo da tabela de metadados para compor a data dos cards do dashboard;

update public.p3m_metadados set datareferencia = current_date where id = 1 OR id = 7;