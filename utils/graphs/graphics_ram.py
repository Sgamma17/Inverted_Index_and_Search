import pandas as pd
import matplotlib.pyplot as plt

# Carica il CSV
df = pd.read_csv("hadoop_vs_spark.csv")

# Verifica le colonne
required_columns = {'input_size_mb', 'execution_time_sec', 'job_type', 'avg_ram_mb'}
if not required_columns.issubset(df.columns):
    raise ValueError(f"Il CSV deve contenere le colonne: {required_columns}")

# Crea il grafico con asse secondario
fig, ax1 = plt.subplots(figsize=(10, 6))
ax2 = ax1.twinx()  # Asse secondario a destra

job_types = df['job_type'].unique()
color_cycle = ['#1f77b4', '#d62728', '#2ca02c']  # Solo rosso, blu e verde

for i, job in enumerate(job_types):
    job_df = df[df['job_type'] == job].sort_values(by='input_size_mb')
    color = color_cycle[i % 3]  # Alterna tra rosso, blu e verde

    # Linea principale: execution time
    ax1.plot(job_df['input_size_mb'], job_df['execution_time_sec'],
             label=f'{job} (Time)', marker='o', color=color)

    # Linea RAM tratteggiata, stesso colore
    ax2.plot(job_df['input_size_mb'], job_df['avg_ram_mb'],
             label=f'{job} (RAM)', linestyle='--', color=color)

# Asse X logaritmico
ax1.set_xscale('log')
ax1.set_xlabel('Input size (MB)')
ax1.set_ylabel('Execution time (seconds)')
ax2.set_ylabel('Average RAM usage per seconds (MB/sec)')
ax2.set_ylim(bottom=0)

ax1.set_title('Hadoop Job execution time and RAM usage vs Input size')

# Unifica legende
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2,)

ax1.grid(True, which="both", linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.show()
