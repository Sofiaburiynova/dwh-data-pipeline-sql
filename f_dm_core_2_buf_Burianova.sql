DROP FUNCTION IF EXISTS dm_wrk.f_dm_core_2_buf;

CREATE OR REPLACE FUNCTION dm_wrk.f_dm_core_2_buf
(
    in_table_name character varying(128)
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name            varchar(64);
    v_src_view_owner        varchar(64);
    v_src_view_name         varchar(64);

    v_buf_create_table_cols text;
    v_buf_table_cols        text;

    v_drop_cmd              text;
    v_create_cmd            text;
    v_insert_cmd            text;

    out_sql                 text := '';
BEGIN
    v_table_name := substring(in_table_name, strpos(in_table_name,'.') + 1);
    v_src_view_owner := substring(in_table_name, 1, strpos(in_table_name,'.') - 1) || '_wrk';
    v_src_view_name := 'v_' || v_table_name;

    EXECUTE '
        SELECT string_agg(column_name || '' '' || data_type, '','' ORDER BY column_pos),
               string_agg(column_name, '','' ORDER BY column_pos)
        FROM _meta_' || v_table_name || '
        WHERE lower(column_name) NOT IN (''_t_datetime'', ''_t_datetime_insert'')
    '
    INTO v_buf_create_table_cols, v_buf_table_cols;

    v_drop_cmd := format('DROP TABLE IF EXISTS %I.%I', 'dm_wrk', 'buf_' || v_table_name);
    v_create_cmd := format('CREATE TABLE %I.%I (%s)', 'dm_wrk', 'buf_' || v_table_name, v_buf_create_table_cols);
    v_insert_cmd := format('INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
                           'dm_wrk', 'buf_' || v_table_name, v_buf_table_cols, v_buf_table_cols,
                           v_src_view_owner, v_src_view_name);

    out_sql := format($F$
DO $inner$
DECLARE
    v_cnt bigint;
BEGIN
    PERFORM dm_wrk.f_log(%1$L, 'START f_dm_core_2_buf');

    EXECUTE %2$L;

    EXECUTE %3$L;

    PERFORM dm_wrk.f_log(%1$L, 'TABLE created');

    EXECUTE %4$L;

    GET DIAGNOSTICS v_cnt = ROW_COUNT;

    PERFORM dm_wrk.f_log(%1$L, 'Inserted rows: ' || v_cnt);

    PERFORM dm_wrk.f_log(%1$L, 'END f_dm_core_2_buf');
END $inner$;
$F$,
    v_table_name,
    v_drop_cmd,
    v_create_cmd,
    v_insert_cmd
);

    RETURN out_sql;
END;
$$;