-- вставляем типы унификации для каждого атрибута клиента

SELECT * FROM mike.uni_attrs;

--DELETE FROM mike.uni_attrs;

INSERT INTO mike.uni_attrs(
	attr, uni_type_attr
)
SELECT 'ID', 'A' FROM dual
UNION ALL
SELECT 'NAME', 'A' FROM dual
UNION ALL
SELECT 'BIRTHDAY', 'A' FROM dual
UNION ALL
SELECT 'AGE', 'A' FROM dual
UNION ALL
SELECT 'PHONE', 'P' FROM dual
UNION ALL
SELECT 'INN', 'A' FROM dual
UNION ALL
SELECT 'PASPORT', 'A' FROM dual
UNION ALL
SELECT 'WEIGHT', 'A' FROM dual
UNION ALL
SELECT 'HEIGHT', 'A' FROM dual;