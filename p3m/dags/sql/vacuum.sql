--Conjunto de querys para realizar o vacuum em todas as tabelas do eschema
--executada após todos os tratamentos
--comando vacuum não pode ser utilizado em bloco

--vacuum (verbose,analyze) anm."TB_Processo";

--vacuum (verbose,analyze) anm."TB_ProcessoPessoa";

--vacuum (verbose,analyze) anm."TB_ProcessoEvento";

--vacuum (verbose,analyze) anm."TB_ProcessoMunicipio";

--vacuum (verbose,analyze) anm."TB_Pessoa";

--vacuum (verbose,analyze) anm."TB_ProcessoSubstancia";
vacuum (verbose,analyze) anm."FC_ProcessoAtivo";

    