import sqlglot
from sqlglot import exp  # <-- this gives access to AST node classes

ddl = """CREATE OR REPLACE VIEW PROD_DB.SALES.VW_ORDERS AS 
         SELECT * FROM PROD_DB.SALES.ORDERS;"""

parsed = sqlglot.parse_one(ddl, dialect="snowflake")

for table in parsed.find_all(exp.Table):
    print("Table name:", table.name)
    print("Schema (db):", table.args.get("db"))
    print("Database (catalog):", table.args.get("catalog"))
