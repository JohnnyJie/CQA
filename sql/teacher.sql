CREATE TABLE Teacher( --df: mult=10.0
  tid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL,
    -- df: sub=power  size=1000 rate=0.03 word=/Users/qq/Documents/GitHub/DPMT/sql/firstname.txt
    lastname TEXT NOT NULL,
    -- df: sub=power size=10000 rate=0.01 word=/Users/qq/Documents/GitHub/DPMT/sql/lastname.txt
    title text NOT NULL,
    -- df: word=/Users/qq/Documents/GitHub/DPMT/sql/title.txt
    age int NOT NULL
    -- df: sub=power rate=0.25 offset=25 size=50
);