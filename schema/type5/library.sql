CREATE TABLE Book(
--df: mult=1 
  bid INTEGER NOT NULL,
  name TEXT NOT NULL,
  title TEXT NOT NULL
);

CREATE TABLE Reader(
  --df: mult=1 
  firstname TEXT NOT NULL,
  lastname TEXT NOT NULL,
  rid INTEGER NOT NULL,
  -- df: sub=uniform size=100000
  born DATE NOT NULL,
  gender BOOLEAN NOT NULL,
  phone INTEGER NOT NULL 
  -- df: chars='0-9' length=9 lenvar=0
);

CREATE TABLE Borrow(
  --df: mult=1 
  borrowed TIMESTAMP NOT NULL PRIMARY KEY,
  rid INTEGER NOT NULL,
  bid INTEGER NOT NULL
);


