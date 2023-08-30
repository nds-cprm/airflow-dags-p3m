drop table if exists public.arealavra;

select ST_Union(geom) as geom
into  table arealavra
from geoserver.mvw_processos_minerarios_ativos mpma 
where ds_fase_agrupada ='Lavra';

update anm.area_intersect
set geom = ST_Multi(ST_intersection(al.geom,ilm.geom))
from public.p3m_municipio_geom ilm 
join arealavra al on ilm.geom && al.geom
where cd_mun = ilm.cod_mun;

update public.p3m_direitominerario_lavramun 
set area_crs = (ST_Area(ai.geom::geography))/1000000
from anm.area_intersect ai
where cod_mun = ai.cd_mun ;