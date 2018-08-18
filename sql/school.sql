

CREATE TABLE Teacher( --df: mult=10.0
  tid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL,
    -- df: sub=power  size=1000 rate=0.03 word=/Users/qq/Documents/GitHub/DPMT/sql/firstname.txt
    lastname TEXT NOT NULL,
    -- df: sub=power size=10000 rate=0.01 word=/Users/qq/Documents/GitHub/DPMT/sql/lastname.txt
    title text NOT NULL,
    -- df: word=/Users/qq/Documents/GitHub/DPMT/sql/title.txt
    age int NOT NULL
    -- df: sub=power rate=0.1 offset=25 size=50
);

CREATE TABLE Student(  --df: mult=50.0
  sid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL,
    -- df: sub=power  size=1000 rate=0.03 word=/Users/qq/Documents/GitHub/DPMT/sql/firstname.txt
    lastname TEXT NOT NULL,
    -- df: sub=power size=10000 rate=0.01 word=/Users/qq/Documents/GitHub/DPMT/sql/lastname.txt
    gender text NOT NULL,
    -- df: word=/Users/qq/Documents/GitHub/DPMT/sql/gender.txt
    phone TEXT
    -- df: chars='0-9' length=10 lenvar=0
    --  df: null=0.01 size=1000000
);

CREATE TABLE Class(
  cid SERIAL PRIMARY KEY,
    cname TEXT NOT NULL,
    --df: word=/Users/qq/Documents/GitHub/DPMT/sql/CourseName.txt
    location varchar(12) NOT NULL
    --df: pattern='(room [0-7]{1}\.[0-9]{2}|Theather [0-9]{2})'
);

CREATE TABLE STU_CLASS( --df: mult=500.0
  cid INTEGER NOT NULL REFERENCES Class,
  sid INTEGER NOT NULL REFERENCES Student,
  PRIMARY KEY(cid, sid),
  grade int NOT NULL
  -- df: sub=uniform offset=35 size=60
);

CREATE TABLE Teacher_CLASS( --df: mult=1
  cid INTEGER NOT NULL REFERENCES Class,
  tid INTEGER NOT NULL REFERENCES Teacher,
  PRIMARY KEY(cid, tid)
);