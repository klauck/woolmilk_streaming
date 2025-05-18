import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO
import numpy as np

with open("nexmark_results.csv", "r") as f:
    csv_data = f.read()

df = pd.read_csv(StringIO(csv_data))

df = df[df["query"] == 1]

agg = df.groupby(["query", "read_type", "bit_rate"]).agg(
    transfer_rate=("transfer_rate", "mean"),
    total_time=("total_time", "mean")
).reset_index()

def bar_plot(metric, ylabel, title_suffix):
    queries = sorted(agg["query"].unique())
    for q in queries:
        data_q = agg[agg["query"] == q]
        bit_rates = sorted(data_q["bit_rate"].unique())
        indices = np.arange(len(bit_rates))
        bar_width = 0.35

        fig, ax = plt.subplots()
        for i, rt in enumerate(["disk", "memory"]):
            vals = data_q[data_q["read_type"] == rt].sort_values("bit_rate")[metric].values
            ax.bar(indices + i * bar_width, vals, bar_width, label=rt, align='center')

        ax.set_xlabel("Chunk size (bit_rate)")
        ax.set_xticks(indices + bar_width / 2)
        ax.set_xticklabels([f"{br:,}" for br in bit_rates])
        ax.set_ylabel(ylabel)
        ax.set_title(f"Query {q}: {title_suffix}")
        ax.legend()
        plt.tight_layout()
        return fig

plot1 = bar_plot("transfer_rate", "Transfer Rate (MB/s)", "Transfer Rate by Chunk Size")
plot1.savefig("transfer_rate_1.png")

plot2 = bar_plot("total_time", "Total Time (s)", "Total Time by Chunk Size")
plot2.savefig("total_time_1.png")

