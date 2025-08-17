CREATE KEYSPACE IF NOT EXISTS etl 
WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1 };

CREATE TABLE etl.persons (
    name text,
    age int,
    city text,
    age_in_months int,
    PRIMARY KEY (name, age, city)
);