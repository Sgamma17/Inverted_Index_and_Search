import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Carica il CSV
df = pd.read_csv("hadoop_vs_mr.csv")

# Verifica le colonne
required_columns = {'input_size_mb', 'execution_time_sec', 'job_type'}
if not required_columns.issubset(df.columns):
    raise ValueError(f"Il CSV deve contenere le colonne: {required_columns}")

# Crea il grafico
plt.figure(figsize=(10, 6))
job_types = df['job_type'].unique()
#colors = plt.cm.get_cmap('tab10', len(job_types))
color_cycle = ['#1f77b4', '#d62728', '#2ca02c', "#dac90d"]  # Solo rosso, blu, verde e giallo

for i, job in enumerate(job_types):
    job_df = df[df['job_type'] == job]
    # Ordina per input size per linee più lisce
    job_df = job_df.sort_values(by='input_size_mb')
    plt.plot(job_df['input_size_mb'], job_df['execution_time_sec'],
             label=job, marker='o', color=color_cycle[i % len(color_cycle)])

# Asse X logaritmico
plt.xscale('log')
plt.xlabel('Input size (MB)')
plt.ylabel('Execution time (seconds)')
plt.title('Time comparison Classic MapReduce vs more Reducers')
plt.legend()
plt.grid(True, which="both", linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.show()
