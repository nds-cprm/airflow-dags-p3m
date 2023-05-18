--Querys para correção de geometrias
--Faz a filtragem de geometrias inválidas e transforma as geometrias do polígonos em uma valida com base no metodo similar ao buffer 0 
update etl."FC_ProcessoAtivo"
-- set "SHAPE" = ST_MakeValid("SHAPE",'method=structure')  --> PostGIS > 3.2.0 & GEOS 3.10.0 (https://postgis.net/docs/ST_MakeValid.html)
set "SHAPE" = ST_MakeValid("SHAPE")
where st_isvalid("SHAPE") is false; 

--Faz a filtragem de quais polígonos possuem mais de 2 dimensões, ou seja coordenadas M/Z e força apenas representação (x,y)
update etl."FC_ProcessoAtivo"
set "SHAPE"=st_force2d("SHAPE")
where st_ndims("SHAPE") != 2;