# Run ollama model inference example

Following instruction are only used in Mac OS.
## Installation
### Pyflink
```bash
cd flink-python; python setup.py sdist bdist_wheel; cd apache-flink-libraries; python setup.py sdist; cd ..;
python -m pip install apache-flink-libraries/dist/*.tar.gz
python -m pip install dist/*.whl



```
### Ollama
1. Download and install ollama from https://github.com/ollama/ollama?tab=readme-ov-file
2. Install ollama python sdk 
```
pip install ollama
```

3. Download qwen3:4b model. Please refer to https://ollama.com/search. You can also choose the model in GUI and trigger the downloading.

## Run the example

```python
python ollama_model_inference.py
```
Output looks like this:
```text
Using Any for unsupported type: typing.Sequence[~T]
No module named google.cloud.bigquery_storage_v1. As a result, the ReadFromBigQuery transform *CANNOT* be used with `method=DIRECT_READ`.
Printing result to stdout. Use --output to specify output path.
11> Question ID 1: response: Here's a quick, clean joke for you:

**Why don't scientists trust atoms?**
*Because they make up everything!* 😄

*(P.S. It’s a classic pun play on "make up" vs. "constitute everything" — hope it gives you a chuckle!)*
10> Question ID 0: response: I am Qwen, the large language model developed by Tongyi Lab. I can help with answering questions, writing stories, emails, scripts, performing logical reasoning, coding, and more. How can I assist you today? 😊
12> Question ID 2: response: To compare **0.8** and **0.11**, follow these steps:

1. **Align the decimals** (add a trailing zero to 0.8 for clarity):
   - 0.8 → **0.80**
   - 0.11 → **0.11**

2. **Compare digit by digit from left to right**:
   - **Tenths place**: 8 (in 0.80) vs. 1 (in 0.11) → **8 > 1**
   *(Since 8 is greater than 1, the value of 0.80 is already larger than 0.11.)*

3. **Conclusion**:
   **0.8 > 0.11** (0.8 is greater than 0.11).

**Why this works**:
- 0.8 = 80/100
- 0.11 = 11/100
- Since **80 > 11**, **0.8 > 0.11**.

**Final result**:
✅ **0.8 is greater than 0.11**.
Running time: 59.000 seconds
```
