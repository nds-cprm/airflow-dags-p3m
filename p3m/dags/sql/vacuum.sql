--Conjunto de querys para realizar o vacuum em todas as tabelas do eschema
--executada após todos os tratamentos
--comando vacuum não pode ser utilizado em bloco

--vacuum (verbose,analyze) etl."TB_Processo";

--vacuum (verbose,analyze) etl."TB_ProcessoPessoa";

--vacuum (verbose,analyze) etl."TB_ProcessoEvento";

--vacuum (verbose,analyze) etl."TB_ProcessoMunicipio";

--vacuum (verbose,analyze) etl."TB_Pessoa";

--vacuum (verbose,analyze) etl."TB_ProcessoSubstancia";
vacuum (verbose,analyze) etl."FC_ProcessoAtivo";

    