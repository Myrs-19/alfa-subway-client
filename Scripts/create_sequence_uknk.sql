-- последовательность для pk и uk 
DROP SEQUENCE mike.key_dwh;

CREATE SEQUENCE mike.key_dwh
  MINVALUE 1
  MAXVALUE 999999999999999999
  START WITH 1
  INCREMENT BY 1;