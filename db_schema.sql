drop table IF EXISTS edge; drop table IF EXISTS node; drop table if exists cluster; drop type if exists task_state;

CREATE TYPE task_state AS ENUM ('PENDING', 'EXECUTING', 'COMPLETE', 'FAILED');

CREATE TABLE cluster (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    name TEXT
);

CREATE TABLE node (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    cluster UUID NOT NULL REFERENCES cluster(id),
    state task_state NOT NULL DEFAULT 'PENDING',
    type TEXT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE edge (
    id SERIAL PRIMARY KEY,
    cluster UUID NOT NULL REFERENCES cluster(id),
    from_node UUID REFERENCES node(id) NOT NULL,
    to_node UUID REFERENCES node(id) NOT NULL
);



