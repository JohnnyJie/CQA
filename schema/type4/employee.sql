
CREATE TABLE Employee( 
--df: mult=1
    eid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL, 
    -- df: sub=power prefix=fn size=1000 rate=0.03
    lastname TEXT NOT NULL,
    -- df: sub=power prefix=ln size=10000 rate=0.01 
    hiredate DATE NOT NULL, 
    -- df: start=2000-01-01 end=2019-01-01
    salary INTEGER NOT NULL, 
    -- df: sub=uniform offset=10000 size=30000 
    phone TEXT NOT NULL,
    -- df: chars='0-9' length=10 lenvar=0
    email TEXT NOT NULL CHECK(email LIKE '%@%'),
    -- df: pattern='[a-z]{3,8}\.[a-z]{3,8}@(gmail|yahoo)\.com'
    degree TEXT NOT NULL
    -- df: pattern='(bsc|mphil|msc|phd){1}'
);