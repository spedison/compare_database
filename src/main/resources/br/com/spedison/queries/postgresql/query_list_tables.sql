select tb.schemaname       as SCHEMA_NAME,
       tb.tablename        as TABLE_NAME,
       kcu.column_name     as KEY_FIELD_NAME
from pg_catalog.pg_tables tb
         left outer join information_schema.table_constraints tco
                         on (tco.constraint_schema = tb.schemaname and tco.table_name = tb.tablename)
         left outer join information_schema.key_column_usage kcu
                         on (kcu.constraint_name = tco.constraint_name and kcu.table_name = tb.tablename and
                             kcu.table_schema = tb.schemaname)
where tb.schemaname not in ('information_schema', 'pg_catalog')
  and (tco.constraint_type = 'PRIMARY KEY')
UNION
select tb.schemaname       as SCHEMA_NAME,
       tb.tablename        as TABLE_NAME,
       NULL                as KEY_FIELD_NAME
from pg_catalog.pg_tables tb
where (tb.schemaname,tb.tablename) not in (
    select tco.table_schema,tco.table_name from information_schema.table_constraints tco
    where constraint_type = 'PRIMARY KEY'
) and  tb.schemaname not in ('information_schema', 'pg_catalog');