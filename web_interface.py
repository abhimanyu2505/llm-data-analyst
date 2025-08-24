import streamlit as st
import pandas as pd
from data_analyst_optimized import DataAnalystAssistant
from enhanced_visualizer import EnhancedVisualizer
import os

st.set_page_config(page_title="Data Analyst Assistant", layout="wide")

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #2E86AB 0%, #A23B72 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .chart-container {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
    
    .info-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
    }
    
    .success-box {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
    }
    
    .stExpander {
        background: #f8f9fa;
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    
    .stExpander > div > div {
        background: white;
        border-radius: 8px;
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'assistant' not in st.session_state:
    st.session_state.assistant = None
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'visualizer' not in st.session_state:
    st.session_state.visualizer = EnhancedVisualizer()

st.markdown('<div class="main-header"><h1>🤖 LLM-Based Data Analyst Assistant</h1></div>', unsafe_allow_html=True)

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")
    
    # Ollama URL
    ollama_url = st.text_input("Ollama URL", value="http://localhost:11434")
    
    if st.button("Initialize Assistant") and not st.session_state.assistant:
        try:
            st.session_state.assistant = DataAnalystAssistant(ollama_url)
            st.success("Assistant initialized!")
        except Exception as e:
            st.error(f"Failed to connect to Ollama: {str(e)}")
    
    st.header("Upload Data")
    
    # File upload
    uploaded_files = st.file_uploader(
        "Upload Excel/CSV files", 
        type=['xlsx', 'xls', 'csv'], 
        accept_multiple_files=True
    )
    
    if uploaded_files and st.session_state.assistant:
        for file in uploaded_files:
            # Save uploaded file temporarily
            temp_path = f"temp_{file.name}"
            with open(temp_path, "wb") as f:
                f.write(file.getbuffer())
            
            # Load into assistant
            table_name = st.text_input(f"Table name for {file.name}", 
                                     value=os.path.splitext(file.name)[0])
            
            if st.button(f"Load {file.name}"):
                try:
                    result = st.session_state.assistant.load_file(temp_path, table_name)
                    st.success(result)
                    os.remove(temp_path)  # Clean up
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")

# Main interface
if st.session_state.assistant:
    # Show available tables
    if st.session_state.assistant.tables:
        st.subheader("Available Tables")
        for table_name, info in st.session_state.assistant.tables.items():
            with st.expander(f"Table: {table_name}"):
                st.write(f"Columns: {', '.join(info['columns'])}")
                st.write("Sample data:")
                st.json(info['sample_data'][:2])
    
    # Chat interface
    st.subheader("Ask Questions About Your Data")
    
    # Display chat history
    for chat in st.session_state.chat_history:
        with st.chat_message("user"):
            st.write(chat['question'])
        
        with st.chat_message("assistant"):
            if chat['success']:
                st.write(chat['insights'])
                
                # Add visualization if possible
                if 'results' in chat and chat['results']:
                    results_df = pd.DataFrame(chat['results'])
                    
                    # Clean data summary
                    data_summary = st.session_state.visualizer.get_chart_summary(results_df)
                    st.info(data_summary)
                    
                    # Create and display chart
                    chart = st.session_state.visualizer.create_visualization(
                        chat['question'], chat['sql_query'], results_df
                    )
                    if chart:
                        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                        st.plotly_chart(chart, use_container_width=True)
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Optional: Show advanced details in collapsible section
                    with st.expander("🔧 Advanced Details"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Shape:** {results_df.shape}")
                            st.write(f"**Columns:** {list(results_df.columns)}")
                        with col2:
                            st.write(f"**Data Types:** {dict(results_df.dtypes)}")
                            st.write("**Sample Data:**")
                            st.dataframe(results_df.head(3))
                        
                        # Generated Plotly code
                        st.write("**Generated Plotly Code:**")
                        plotly_code = st.session_state.visualizer.generate_plotly_code(
                            chat['question'], chat['sql_query'], results_df
                        )
                        st.code(plotly_code, language='python')
                    
                    viz_explanation = st.session_state.visualizer.get_visualization_explanation(
                        chat['question'], chat['sql_query'], results_df
                    )
                    st.success(viz_explanation)
                
                with st.expander("View SQL Query & Results"):
                    st.code(chat['sql_query'], language='sql')
                    if chat['results']:
                        st.dataframe(pd.DataFrame(chat['results']))
            else:
                st.error(chat['error'])
    
    # Input for new question
    question = st.chat_input("Ask a question about your data...")
    
    if question:
        with st.chat_message("user"):
            st.write(question)
        
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                result = st.session_state.assistant.analyze(question)
                st.session_state.chat_history.append(result)
                
                if result['success']:
                    st.write(result['insights'])
                    
                    # Add visualization if possible
                    if 'results' in result and result['results']:
                        results_df = pd.DataFrame(result['results'])
                        
                        # Clean data summary
                        data_summary = st.session_state.visualizer.get_chart_summary(results_df)
                        st.info(data_summary)
                        
                        # Create and display chart
                        chart = st.session_state.visualizer.create_visualization(
                            question, result['sql_query'], results_df
                        )
                        if chart:
                            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                            st.plotly_chart(chart, use_container_width=True)
                            st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Optional: Show advanced details in collapsible section
                        with st.expander("🔧 Advanced Details"):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Shape:** {results_df.shape}")
                                st.write(f"**Columns:** {list(results_df.columns)}")
                            with col2:
                                st.write(f"**Data Types:** {dict(results_df.dtypes)}")
                                st.write("**Sample Data:**")
                                st.dataframe(results_df.head(3))
                            
                            # Generated Plotly code
                            st.write("**Generated Plotly Code:**")
                            plotly_code = st.session_state.visualizer.generate_plotly_code(
                                question, result['sql_query'], results_df
                            )
                            st.code(plotly_code, language='python')
                        
                        viz_explanation = st.session_state.visualizer.get_visualization_explanation(
                            question, result['sql_query'], results_df
                        )
                        st.success(viz_explanation)
                    
                    with st.expander("View SQL Query & Results"):
                        st.code(result['sql_query'], language='sql')
                        if result['results']:
                            st.dataframe(pd.DataFrame(result['results']))
                else:
                    st.error(result['error'])

else:
    st.warning("Please initialize the assistant in the sidebar. Make sure Ollama is running with Llama 3 model.")