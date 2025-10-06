import duckdb

def integrate_customer_data():
    # Connect to an in-memory DuckDB instance
    con = duckdb.connect(database=':memory:')
    
    # Load the standardised CSV files into DuckDB tables
    con.execute("""
        CREATE TABLE sys_a AS 
        SELECT * FROM read_csv_auto('/data/sys_a_standardised.csv')
    """)
    
    con.execute("""
        CREATE TABLE sys_b AS 
        SELECT * FROM read_csv_auto('/data/sys_b_standardised.csv')
    """)
    
    # Perform the integration logic (example: simple UNION)
    con.execute("""
        CREATE TABLE integrated_customers AS
        SELECT * FROM sys_a
        UNION ALL
        SELECT * FROM sys_b
    """)
    
    # Export the integrated data to a CSV file
    con.execute("""
        COPY (SELECT * FROM integrated_customers) 
        TO '/data/integrated_customers.csv' 
        WITH (HEADER, DELIMITER ',')
    """)
    
    # Close the connection
    con.close()
    
if __name__ == "__main__":
    integrate_customer_data()