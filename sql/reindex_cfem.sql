--Query em bloco para atualização dda tabela cfem
--executada após atualização
-- TODO: Trocar o schema geoserver para anm, depois de resolver no Django
reindex (verbose) table anm.cfem_arrecadacao_ativa;
