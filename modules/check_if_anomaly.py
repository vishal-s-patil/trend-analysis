from langchain.chains import LLMChain
from langchain_huggingface import HuggingFaceEndpoint
from langchain_core.prompts import PromptTemplate

question = ("""Anomaly Detection Prompt for Vertica Database
**Objective:**
Analyze the given data points and system information to detect any anomalies in sessions,
queues, query counts, and performance metrics in the Vertica database.""")

template = """
**Inputs:**

+ **Minute wise session count for the entire day**  
+ **Minute wise queue length for the entire day**
+ **Day wise query count for last 7 days**
+ **Day wise query performance for last 7 days**

**Expected Output:**  
+ Identify any anomalies in the data such as:  
  - Unusual spikes or drops in sessions, query counts, or performance metrics.  
  - Queues with an unexpectedly high number of waiting queries.  
  - Resource usage exceeding expected thresholds (e.g., CPU, memory, or disk).  

+ Provide a summary of detected anomalies with timestamps and affected metrics.  

**Additional Notes:**  
+ Use historical trends or averages where applicable to set dynamic thresholds for anomaly detection.  
+ Include any relevant charts or visualizations to support your findings.  
"""

prompt = PromptTemplate.from_template(template)

repo_id = "mistralai/Mistral-7B-Instruct-v0.2"

llm = HuggingFaceEndpoint(
    repo_id=repo_id,
    max_length=128,
    temperature=0.5,
    huggingfacehub_api_token="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",
)

llm_chain = prompt | llm

print(llm_chain.invoke({"question": question}))