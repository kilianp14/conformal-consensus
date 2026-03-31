import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error, r2_score

output_path = Path("./results/sigmoid_fit_contention/")
output_path.mkdir(parents=True, exist_ok=True)
df = pd.read_csv("./data/follower_metrics2.csv")

# Feature Engineering
df["other_nodes_proposals_per_sec"] = (
    df["leader_proposals_per_sec"] + df["other_followers_proposals_per_sec"]
)
df["in-flight_proposals"] = (
    df["latency_to_fast_fq"] * df["other_nodes_proposals_per_sec"]
)
# Log Space for numerical stability
df["log_in-flight_proposals"] = np.log1p(df["in-flight_proposals"])
df["actual"] = df["successful_rate"] / 100.0

df_train, df_temp = train_test_split(df, test_size=0.3, random_state=14)
df_val, df_test = train_test_split(df_temp, test_size=0.5, random_state=14)


# Model fit
def stable_sigmoid(x, k):
    z = k * (x - np.log1p(500))
    return np.where(
        z >= 0,
        1 / (1 + np.exp(-z)),
        np.exp(z) / (1 + np.exp(z)),
    )


popt, _ = curve_fit(
    stable_sigmoid,
    df_train["log_in-flight_proposals"],
    df_train["actual"],
    maxfev=5000,
)

print("--- Sigmoid Model Parameters ---")
print(f"(k): {popt[0]:.5f}")
# print(f"(x0): {popt[1]:.5f}")

# Eval
df_test = df_test.copy()
df_test["pred"] = stable_sigmoid(df_test["log_in-flight_proposals"], *popt)
rmse = np.sqrt(mean_squared_error(df_test["actual"], df_test["pred"]))
r2 = r2_score(df_test["actual"], df_test["pred"])
print("\n--- Performance ---")
print(f"RMSE: {rmse:.5f}")
print(f"R^2 Score: {r2:.5f}")

fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(22, 7))
# Plot 1: Actual vs Predicted
ax1.scatter(df_test["actual"], df_test["pred"], alpha=0.3, color="teal")
ax1.plot([0, 1], [0, 1], color="red", linestyle="--", label="Perfect Fit")
ax1.set_title("Actual vs. Predicted Success Rate")
ax1.set_xlabel("Actual")
ax1.set_ylabel("Predicted")
ax1.legend()

# Plot 2: Residuals vs Raw Contention (Log Scale)
residuals = df_test["actual"] - df_test["pred"]
ax2.scatter(df_test["in-flight_proposals"], residuals, alpha=0.4, color="crimson")
ax2.axhline(0, color="black", linestyle="--")
ax2.set_xscale("log")
ax2.set_title("Residuals vs. Raw Contention")
ax2.set_xlabel("Raw Contention (Log Scale)")
ax2.set_ylabel("Error (Actual - Pred)")

# Plot 3: Sigmoid Curve Fit (Log In-flight vs Success Rate)
ax3.scatter(
    df_test["in-flight_proposals"],
    df_test["actual"],
    alpha=0.4,
    color="teal",
)
x_range = np.linspace(
    df["log_in-flight_proposals"].min(), df["log_in-flight_proposals"].max(), 500
)
y_range = stable_sigmoid(x_range, *popt)
ax3.plot(np.expm1(x_range), y_range, color="red", linewidth=3, label="Model")
ax3.set_title("Model Fit: Log Contention vs. Success Rate")
ax3.set_xscale("log")
ax3.set_xlabel("In-Flight Proposals")
ax3.set_ylabel("Success Rate")
ax3.legend()
ax3.grid(True, linestyle="--", alpha=0.5)

plt.tight_layout()
plt.savefig(output_path / "linear_log_analysis.png")

# Worst Points Analysis
df_test["abs_error"] = abs(residuals)
worst = df_test.sort_values(by="abs_error", ascending=False).head(100)
cols_to_save = [
    "latency_to_fast_fq",
    "other_nodes_proposals_per_sec",
    "in-flight_proposals",
    "log_in-flight_proposals",
    "actual",
    "pred",
    "abs_error",
]
worst[cols_to_save].to_csv(output_path / "worst_linear_points.csv", index=False)

print(f"\nSaved to: {output_path.absolute()}")
