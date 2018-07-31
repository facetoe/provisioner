drop table IF EXISTS edge;
drop table IF EXISTS node;
drop table if exists cluster cascade;
drop type if exists task_state;
drop table if exists data_centre cascade;
drop table if exists server cascade;
drop table if exists network cascade;



CREATE TYPE task_state AS ENUM (
    'PENDING_PROVISION',
    'PROVISIONING', 'PROVISIONED',
    'PENDING_DELETION',
    'DELETING',
    'DELETED',
    'FAILED'
);

CREATE TABLE cluster (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    name TEXT,
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE data_centre (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    name TEXT,
    cluster UUID NOT NULL REFERENCES cluster(id),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE server (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    instance_id TEXT NOT NULL,
    cluster UUID NOT NULL REFERENCES cluster(id),
    data_centre UUID NOT NULL REFERENCES data_centre(id),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE network (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    instance_id TEXT NOT NULL,
    cluster UUID NOT NULL REFERENCES cluster(id),
    data_centre UUID NOT NULL REFERENCES data_centre(id),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE node (
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    cluster UUID NOT NULL REFERENCES cluster(id),
    data_centre UUID NULL REFERENCES data_centre(id),
    state task_state NOT NULL DEFAULT 'PENDING_PROVISION',
    type TEXT NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE edge (
    id SERIAL PRIMARY KEY,
    cluster UUID NOT NULL REFERENCES cluster(id),
    data_centre UUID NOT NULL REFERENCES data_centre(id),
    from_node UUID REFERENCES node(id) NOT NULL,
    to_node UUID REFERENCES node(id) NOT NULL,
    UNIQUE(cluster, data_centre, from_node, to_node)
);
