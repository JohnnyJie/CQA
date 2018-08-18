CREATE TABLE STU_CLASS( --df: mult=500.0
  cid INTEGER NOT NULL REFERENCES Class,
  sid INTEGER NOT NULL REFERENCES Student,
  PRIMARY KEY(cid, sid)
  grade int NOT NULL,
  -- df: sub=uniform offset=35 size=60
);