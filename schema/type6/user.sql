-- df.size = 10000
CREATE TABLE Users( 
--df: mult=1 
    rid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL, 
    -- df: sub=serial prefix=fn
    lastname TEXT NOT NULL,
    -- df: sub=uniform prefix=ln offset=10000 
    born DATE NOT NULL, 
    -- df: start=1923-01-01 end=2010-01-01
    gender BOOLEAN NOT NULL, 
    -- df: rate=0.5
    email TEXT NOT NULL CHECK(email LIKE '%@%')
    -- df: pattern='[a-z]{3,8}\.[a-z]{3,8}@(gmail|yahoo)\.com'
);