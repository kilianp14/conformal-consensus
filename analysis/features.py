import pandas as pd
import matplotlib.pyplot as plt
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

df = pd.read_csv("follower_metrics.csv")

features = [
    "latency_to_leader",
    "latency_to_majority_cq",
    "latency_to_fast_fq",
    "latency_to_all_max",
    "own_proposals_per_sec",
    "leader_proposals_per_sec",
    "other_followers_proposals_per_sec",
    "max_follower_proposals_per_sec",
]

targets = ["successful_rate_pct", "collision_rate_pct"]
X = df[features]

for target in targets:
    y = df[target] / 100.0

    # Split: 70% Train, 30% Temporary
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    # Split Temporary: 50% Validation (15% total), 50% Test (15% total)
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.5, random_state=42
    )

    model = xgb.XGBRegressor(
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=6,
        tree_method="hist",
        early_stopping_rounds=50,
        n_jobs=-1,
        random_state=42,
    )

    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

    # Final Evaluation on Test Set
    preds = model.predict(X_test)
    rmse = mean_squared_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    print(f"\n--- Final Test Results for {target} ---")
    print(f"Best Iteration: {model.best_iteration}")
    print(f"RMSE: {rmse:.5f}")
    print(f"R^2 Score: {r2:.5f}")

    # Plot & Save
    fig, ax = plt.subplots(figsize=(10, 8))
    xgb.plot_importance(
        model, importance_type="gain", ax=ax, title=f"Importance: {target}"
    )
    plt.savefig(f"importance_{target}.png", bbox_inches="tight")
    plt.close(fig)
