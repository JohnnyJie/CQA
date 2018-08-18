-- df.size = 10000
CREATE TABLE Book( 
--df: mult=100
  bid SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  -- df: extent=2
  isbn ISBN13 NOT NULL 
  -- df: size=1000000000  
);


CREATE TABLE Reader( 
--df: mult=1
  rid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL, 
    -- df: sub=power prefix=fn size=1000 rate=0.03
    lastname TEXT NOT NULL,
    -- df: sub=power prefix=ln size=10000 rate=0.01 
    born DATE NOT NULL, 
    -- df: start=1923-01-01 end=2010-01-01
    gender BOOLEAN NOT NULL, 
    -- df: rate=0.25
    phone TEXT ,
    -- df: chars='0-9' length=10 lenvar=0
    -- df: null=0.01 size=1000000
    email TEXT NOT NULL CHECK(email LIKE '%@%')
    -- df: pattern='[a-z]{3,8}\.[a-z]{3,8}@(gmail|yahoo)\.com'
);

CREATE TABLE Borrow(
  --df: mult=10
  borrowed TIMESTAMP NOT NULL, 
  -- df: size=720000 prec=60
    rid INTEGER NOT NULL REFERENCES Reader,
    -- df: mangle
    bid INTEGER NOT NULL REFERENCES Book, 
    PRIMARY KEY(bid) -- a book is borrowed once at a time!
);


