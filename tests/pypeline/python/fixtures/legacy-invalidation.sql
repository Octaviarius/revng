
INSERT INTO invalidated_objects_{uuid}
    SELECT DISTINCT objects.rowid
    FROM objects
    JOIN (
            SELECT savepoint_id_start, savepoint_id_end, configuration_hash, object_id
            FROM dependencies
            WHERE dependencies.model_path IN (SELECT path FROM model_paths_{uuid})
        UNION
            SELECT savepoint_id_start, savepoint_id_end, configuration_hash, object_id
            FROM additional_objects_{uuid}
    ) AS dependencies
    WHERE (
            (
              objects.object_id >= dependencies.object_id
              AND objects.object_id <= CAST((dependencies.object_id || {objectid_mask}) AS BLOB)
            ) OR (
              dependencies.object_id != x'' AND objects.object_id = x''
            )
          )
          AND objects.configuration_hash = dependencies.configuration_hash
          AND objects.savepoint_id >= dependencies.savepoint_id_start
          AND objects.savepoint_id <= dependencies.savepoint_id_end
;

DELETE FROM dependencies
WHERE rowid in (
    SELECT DISTINCT dependencies.rowid
    FROM dependencies
    JOIN (
        SELECT savepoint_id, container_id, configuration_hash, object_id
        FROM objects
        WHERE rowid IN (SELECT object_row_id FROM invalidated_objects_{uuid})
    ) AS invalidated_objects
    WHERE
        dependencies.savepoint_id_start = invalidated_objects.savepoint_id
        AND dependencies.container_id = invalidated_objects.container_id
        AND dependencies.configuration_hash = invalidated_objects.configuration_hash
        AND dependencies.object_id = invalidated_objects.object_id
);
