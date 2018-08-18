-- df.size = 10000
CREATE TABLE Map2pk( 
-- df distinct: int size=1000000
 mac MACADDR NOT NULL, -- df share=distinct
 ip INET NOT NULL -- df share=distinct inet='10.0.0.0/8'
 );