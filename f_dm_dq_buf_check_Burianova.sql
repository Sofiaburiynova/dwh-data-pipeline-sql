DROP FUNCTION IF EXISTS dm_wrk.f_dm_dq_buf_check;

CREATE OR REPLACE FUNCTION dm_wrk.f_dm_dq_buf_check
(
    in_table_name varchar
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name varchar;
    v_pk_cols text;
    out_sql text;
BEGIN
    v_table_name := substring(in_table_name, strpos(in_table_name,'.') + 1);

    SELECT string_agg(column_name, ',')
    INTO v_pk_cols
    FROM _meta_pk
    WHERE lower(table_name) = lower(v_table_name);

    out_sql := format($F$
DO $inner$
DECLARE 
    v_cnt bigint;
BEGIN
    PERFORM dm_wrk.f_log(%1$L, 'START dq_check');

    EXECUTE format(
        'SELECT count(*) FROM (SELECT %2$s FROM dm_wrk.buf_%1$s GROUP BY %2$s HAVING count(*) > 1) t'
    ) INTO v_cnt;

    PERFORM dm_wrk.f_log(%1$L, 'Duplicates: ' || v_cnt);

    EXECUTE format(
        'DELETE FROM dm_wrk.buf_%1$s a USING (
            SELECT ctid, row_number() OVER (PARTITION BY %2$s ORDER BY ctid) rn
            FROM dm_wrk.buf_%1$s
        ) b
        WHERE a.ctid = b.ctid AND b.rn > 1'
    );

    PERFORM dm_wrk.f_log(%1$L, 'END dq_check');
END $inner$;
$F$, 
    v_table_name,
    v_pk_cols
);

    RETURN out_sql;
END;
$$;