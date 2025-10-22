# Importaciones de librerías
from sqlalchemy import create_engine, text
import urllib.parse
import pandas as pd

# CONEXIÓN A BASES DE DATOS
username = "postgres"
password = urllib.parse.quote_plus("VELASQUEZ74")
host = "localhost"
port = "5432"

db_src = "chinook"
db_dw = "dwchinook"

url_src = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_src}"
url_dw = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_dw}"

engine_src = create_engine(url_src)
engine_dw = create_engine(url_dw)

try:
    with engine_src.connect() as conn:
        print("Conexión exitosa ORIGEN:", conn.execute(text("SELECT version();")).fetchone()[0])
    with engine_dw.connect() as conn:
        print("Conexión exitosa DESTINO:", conn.execute(text("SELECT version();")).fetchone()[0])
except Exception as e:
    print("Error de conexión:", e)
    exit(1)

# EXTRACCIÓN Y TRANSFORMACIÓN

# Dim_Date
query_date = """
SELECT DISTINCT invoice_date::date AS fulldate,
       EXTRACT(YEAR FROM invoice_date) AS yeard,
       EXTRACT(MONTH FROM invoice_date) AS monthd,
       EXTRACT(DAY FROM invoice_date) AS dayd
FROM invoice;
"""
dim_date = pd.read_sql(query_date, engine_src)
dim_date.columns = [c.lower().replace("_","") for c in dim_date.columns]

# Dim_Customer
query_customer = """
SELECT customer_id AS customerid,
       first_name || ' ' || last_name AS fullname,
       company, address, city, state, country,
       postal_code AS postalcode, phone, email
FROM customer;
"""
dim_customer = pd.read_sql(query_customer, engine_src)
dim_customer.columns = [c.lower().replace("_","") for c in dim_customer.columns]

# Dim_Employee
query_employee = """
SELECT employee_id AS employeeid,
       first_name || ' ' || last_name AS fullname,
       title, city, state, country, email
FROM employee;
"""
dim_employee = pd.read_sql(query_employee, engine_src)
dim_employee.columns = [c.lower().replace("_","") for c in dim_employee.columns]

# Dim_Track
query_track = """
SELECT t.track_id AS trackid,
       t.name AS trackname,
       a.title AS album,
       ar.name AS artist,
       g.name AS genre,
       m.name AS mediatype,
       t.composer, t.milliseconds, t.bytes
FROM track t
LEFT JOIN album a ON t.album_id = a.album_id
LEFT JOIN artist ar ON a.artist_id = ar.artist_id
LEFT JOIN genre g ON t.genre_id = g.genre_id
LEFT JOIN media_type m ON t.media_type_id = m.media_type_id;
"""
dim_track = pd.read_sql(query_track, engine_src)
dim_track.columns = [c.lower().replace("_","") for c in dim_track.columns]

# Dim_Invoice
query_invoice = """
SELECT invoice_id AS invoiceid,
       billing_address AS billingaddress,
       billing_city AS billingcity,
       billing_state AS billingstate,
       billing_country AS billingcountry,
       billing_postal_code AS billingpostalcode
FROM invoice;
"""
dim_invoice = pd.read_sql(query_invoice, engine_src)
dim_invoice.columns = [c.lower().replace("_","") for c in dim_invoice.columns]

# Fact_Sales
query_fact = """
SELECT il.invoice_line_id AS invoicelineid,
       il.invoice_id AS invoiceid,
       il.track_id AS trackid,
       i.customer_id AS customerid,
       c.support_rep_id AS employeeid,
       i.invoice_date::date AS invoicedate,
       il.quantity,
       il.unit_price AS unitprice,
       (il.quantity * il.unit_price) AS total
FROM invoice_line il
JOIN invoice i ON il.invoice_id = i.invoice_id
JOIN customer c ON i.customer_id = c.customer_id;
"""
fact_sales = pd.read_sql(query_fact, engine_src)
fact_sales.columns = [c.lower().replace("_","") for c in fact_sales.columns]

# CARGA AL DATA WAREHOUSE
dim_date.to_sql("dim_date", engine_dw, schema="dw", if_exists="append", index=False)
dim_customer.to_sql("dim_customer", engine_dw, schema="dw", if_exists="append", index=False)
dim_employee.to_sql("dim_employee", engine_dw, schema="dw", if_exists="append", index=False)
dim_track.to_sql("dim_track", engine_dw, schema="dw", if_exists="append", index=False)
dim_invoice.to_sql("dim_invoice", engine_dw, schema="dw", if_exists="append", index=False)

# Traer dim_date para mapear fechas
date_dim = pd.read_sql("SELECT dateid, fulldate FROM dw.dim_date;", engine_dw)
date_dim.columns = [c.lower().replace("_","") for c in date_dim.columns]

# Merge con fact_sales
fact_sales = fact_sales.merge(date_dim, left_on="invoicedate", right_on="fulldate", how="left")

# Seleccionar columnas finales
fact_sales_final = fact_sales[[
    "invoiceid", "trackid", "customerid", "employeeid",
    "dateid", "quantity", "unitprice", "total"
]]

# Insertar Fact_Sales
fact_sales_final.to_sql("fact_sales", engine_dw, schema="dw", if_exists="append", index=False)

print("\n ETL completado correctamente")






