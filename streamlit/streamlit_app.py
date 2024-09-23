import streamlit as st

st.title('OHLC Data Viewer')

try:
    # Initialize connection
    conn = st.connection("postgresql", type="sql")
    
    # Perform query
    df = conn.query('SELECT * FROM ohlc', ttl="10m")
    
    # Display results
    st.dataframe(df)
except Exception as e:
    st.error(f"An error occurred: {str(e)}")