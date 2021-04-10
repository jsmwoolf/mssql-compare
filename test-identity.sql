CREATE TABLE test1 (
    test1a BIGINT IDENTITY
);

CREATE TABLE test2 (
    test2a BIGINT IDENTITY(4,3)
);

CREATE TABLE test3 (
    test3a BIGINT IDENTITY,
    test3b INT
);

CREATE TABLE test4 (
    test4a BIGINT IDENTITY(4,3),
    test4b INT
);