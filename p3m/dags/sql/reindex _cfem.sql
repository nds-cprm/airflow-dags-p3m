--Query em bloco para atualização dda tabela cfem
--executada após atualização

reindex (verbose) table geoserver.cfem_arrecadacao_ativa;
