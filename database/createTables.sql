CREATE TABLE IF NOT EXISTS Tests (
        id varchar(24) NOT NULL,
        CONSTRAINT Tests_PK PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Object (
        id varchar(24) NOT NULL,
        -- id varchar (24) NOT NULL,
        -- id varchar (24) NOT NULL,

        string VARCHAR(100),
        number INTEGER,
        CONSTRAINT Object_PK PRIMARY KEY (id) -- CONSTRAINT Object_PK PRIMARY KEY (id)

);

CREATE TABLE IF NOT EXISTS Objects (
        id varchar(24) NOT NULL,
        test VARCHAR(100),
        testNumber INTEGER,
        CONSTRAINT Objects_PK PRIMARY KEY (id)
);
