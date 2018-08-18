-- df.size = 10000
CREATE TABLE Users( 
--df: mult=1.01 
    uid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL, 
    -- df: sub=serial prefix=fn
    lastname TEXT NOT NULL,
    -- df: sub=uniform prefix=ln offset=10000 
    age INTEGER NOT NULL, 
    -- df: sub=uniform offset=18 size=82
    gender BOOLEAN NOT NULL, 
    -- df: rate=0.5
    email TEXT NOT NULL CHECK(email LIKE '%@%')
    -- df: pattern='[a-z]{3,8}\.[a-z]{3,8}@(gmail|yahoo)\.com'
);

CREATE TABLE Orders(
    --df: mult=1 
    orderid SERIAL PRIMARY KEY,
    uid INTEGER NOT NULL REFERENCES Users,
    -- df: mangle
    productname TEXT NOT NULL, 
    -- df: sub=serial prefix=pn
    saledate DATE NOT NULL
    -- df: start=2000-01-01 end=2018-01-01
); 


