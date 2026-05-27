import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from sklearn.metrics import mean_squared_error, r2_score


def score(latency_to_fast_quorum, other_nodes_proposals_per_sec):
    return 1 / (
        1 + np.power(2 * latency_to_fast_quorum * other_nodes_proposals_per_sec, 4 / 3)
    )


output_path = Path("./results/well-defined_score_function/")
output_path.mkdir(parents=True, exist_ok=True)
df = pd.read_csv("./data/follower_metrics3.csv")
df["other_nodes_proposals_per_sec"] = (
    df["leader_proposals_per_sec"] + df["other_followers_proposals_per_sec"]
)

# Eval
df["pred"] = score(df["latency_to_fast_fq"], df["other_nodes_proposals_per_sec"])
rmse = np.sqrt(mean_squared_error(df["successful_rate"], df["pred"]))
r2 = r2_score(df["successful_rate"], df["pred"])
print("\n--- Performance ---")
print(f"RMSE: {rmse:.5f}")
print(f"R^2 Score: {r2:.5f}")

fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(22, 7))
# Plot 1: Actual vs Predicted
ax1.scatter(df["successful_rate"], df["pred"], alpha=0.3, color="teal")
ax1.plot([0, 1], [0, 1], color="red", linestyle="--", label="Perfect Fit")
ax1.set_title("Actual vs. Predicted Success Rate")
ax1.set_xlabel("Actual")
ax1.set_ylabel("Predicted")
ax1.legend()

# Plot 2: Residuals vs Raw Contention (Log Scale)
residuals = df["successful_rate"] - df["pred"]
ax2.scatter(
    df["latency_to_fast_fq"] * df["other_nodes_proposals_per_sec"],
    residuals,
    alpha=0.4,
    color="crimson",
)
ax2.axhline(0, color="black", linestyle="--")
ax2.set_xscale("log")
ax2.set_title("Residuals vs. Raw Contention")
ax2.set_xlabel("Fast Quorum Latency (in sec) * Incoming Accept Messages/sec")
ax2.set_ylabel("Error (Actual - Pred)")

# Plot 3: Model vs Actual
df["inflight_proposals"] = (
    df["latency_to_fast_fq"] * df["other_nodes_proposals_per_sec"]
)

ax3.scatter(
    df["inflight_proposals"],
    df["successful_rate"],
    alpha=0.4,
    color="teal",
    label="Actual",
)

x_range = np.logspace(
    np.log10(df["inflight_proposals"].min()),
    np.log10(df["inflight_proposals"].max()),
    500,
)

# Model curve: score = 1 / (1 + (2c)^(4/3))
y_range = 1 / (1 + np.power(2 * x_range, 4 / 3))
ax3.plot(x_range, y_range, color="red", linewidth=2, label="Model")
ax3.set_title("Model Fit: Contention vs. Success Rate")
ax3.set_xscale("log")
ax3.set_xlabel("Fast Quorum Latency (in sec) * Incoming Accept Messages/sec")
ax3.set_ylabel("Success Rate")
ax3.legend()
ax3.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig(output_path / "linear_log_analysis.png")

# Worst Points Analysis
df["abs_error"] = abs(residuals)
worst = df.sort_values(by="abs_error", ascending=False).head(100)
cols_to_save = [
    "latency_to_fast_fq",
    "other_nodes_proposals_per_sec",
    "successful_rate",
    "pred",
    "abs_error",
]
worst[cols_to_save].to_csv(output_path / "worst_linear_points.csv", index=False)

print(f"\nSaved to: {output_path.absolute()}")
