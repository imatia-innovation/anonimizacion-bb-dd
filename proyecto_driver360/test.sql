select * 
from (
	select a.table_name, CONCAT('~/^', a.table_name, '$/,') AS nombre_transformado,
	ROUND(((b.data_length + b.index_length) / 1024 / 1024), 2) AS size_mb,
	max(if(tipo = 'Independiente', 'Independiente', null)) as independiente, max(if(tipo = 'Padre', 'Padre', null)) as padre, max(if(tipo = 'Hija', 'Hija', null)) as hija, count(*)
	from (
		-- Tablas independientes
		SELECT TABLE_NAME, 'Independiente' AS tipo
		FROM information_schema.TABLES
		where TABLE_NAME NOT IN (
			SELECT TABLE_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			where REFERENCED_TABLE_NAME IS NOT null and table_schema = 'demo_driver360_copia'
			union all 
			SELECT REFERENCED_TABLE_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE REFERENCED_TABLE_NAME IS NOT null and table_schema = 'demo_driver360_copia'
		)
		and table_schema = 'demo_driver360_copia'
		-- Tablas padres
		union all 
		SELECT DISTINCT REFERENCED_TABLE_NAME AS tabla_padre, 'Padre' AS tipo
		FROM information_schema.KEY_COLUMN_USAGE
		where REFERENCED_TABLE_NAME IS NOT null and table_schema = 'demo_driver360_copia'
		union all
		-- Tablas hijas
		SELECT DISTINCT TABLE_NAME AS tabla_hija, 'Hija' AS tipo
		FROM information_schema.KEY_COLUMN_USAGE
		where REFERENCED_TABLE_NAME IS NOT null and table_schema = 'demo_driver360_copia'
	) a
	left join information_schema.tables b
	on a.table_name = b.table_name
	group by 1,2,3
	order by size_mb desc
) a
WHERE table_name NOT LIKE '%sin_anonimizar';