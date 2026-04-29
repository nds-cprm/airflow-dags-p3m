update geoserver.cfem_arrecadacao_ativa caa
set processo_ano = '871.787/2024'
where substancia ilike '%ur%nio%' and extract(uear from caa.data_recolhimento_cfem) >= 2020;

update geoserver.cfem_arrecadacao_ativa caa
set processo_ano = '871.786/2024'
where substancia ilike '%ur%nio%' and extract(uear from caa.data_recolhimento_cfem) < 2020;

