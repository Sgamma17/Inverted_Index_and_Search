import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Carica il CSV
df = pd.read_csv("job_stats.csv")

# Verifica le colonne
required_columns = {'input_size_mb', 'execution_time_sec', 'job_type'}
if not required_columns.issubset(df.columns):
    raise ValueError(f"Il CSV deve contenere le colonne: {required_columns}")

# Crea il grafico
plt.figure(figsize=(10, 6))
job_types = df['job_type'].unique()
colors = plt.cm.get_cmap('tab10', len(job_types))

for i, job in enumerate(job_types):
    job_df = df[df['job_type'] == job]
    # Ordina per input size per linee più lisce
    job_df = job_df.sort_values(by='input_size_mb')
    plt.plot(job_df['input_size_mb'], job_df['execution_time_sec'],
             label=job, marker='o', color=colors(i))

# Asse X logaritmico
plt.xscale('log')
plt.xlabel('Dimensione input (MB)')
plt.ylabel('Tempo di esecuzione (secondi)')
plt.title('Tempo di esecuzione dei job Hadoop in funzione della dimensione input')
plt.legend(title="Tipo di job")
plt.grid(True, which="both", linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.show()
