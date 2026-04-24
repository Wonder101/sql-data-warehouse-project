# Data Warehouse and Analytics Project 
This is a spin of a previous SQL Server Project. I made this in PostgreSQL as a means of learning the language. 
Overview: Building a modern data warehouse with PostgreSQL. Includes ETL processes, data modeling, and analytics. 

# Note on CSV Loading (DBeaver Users)
I ran into this issue when trying to populate the bronze tables in DBeaver. Thought I might as well leave a note :)
The COPY command in PostgreSQL is server-side, meaning it looks for files on the PostgreSQL server's filesystem rather than your local machine. If you are running this project locally through DBeaver, the COPY commands in the bronze layer procedure will not work as written. Instead, right click each table in DBeaver -> Import Data -> select the corresponding CSV file manually. Alternatively, you can run the scripts through psql in your terminal, which reads files from your local machine directly.

