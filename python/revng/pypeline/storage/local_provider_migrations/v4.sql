--
-- This file is distributed under the MIT License. See LICENSE.md for details.
--

ALTER TABLE custom_dependencies RENAME TO custom_dependencies_v3;
CREATE TABLE custom_dependencies(
    pipe_id INT NOT NULL,
    configuration_hash TEXT NOT NULL,
    argument_index INT NOT NULL,
    object_id BLOB NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY(pipe_id, configuration_hash, argument_index, object_id)
) STRICT;
INSERT INTO custom_dependencies SELECT * FROM custom_dependencies_v3;
DROP TABLE custom_dependencies_v3;
CREATE INDEX custom_dependencies_index ON custom_dependencies(pipe_id, configuration_hash);
