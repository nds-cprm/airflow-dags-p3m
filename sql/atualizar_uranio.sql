update geoserver.cfem_arrecadacao_ativa caa
set processo_ano = '871787/2024'
where substancia ilike '%ur%nio%' and extract(year from caa.data_recolhimento_cfem) >= 2020;

update geoserver.cfem_arrecadacao_ativa caa
set processo_ano = '871786/2024'
where substancia ilike '%ur%nio%' and extract(year from caa.data_recolhimento_cfem) < 2020;

