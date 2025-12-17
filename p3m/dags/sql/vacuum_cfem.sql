--Conjunto de querys para realizar o vacuum na tabela cfem
--executada após atualização
--comando vacuum não pode ser utilizado em bloco

vacuum (verbose,analyze) geoserver.cfem_arrecadacao_ativa;

    