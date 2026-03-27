import pandas as pd
import matplotlib.pyplot as plt
import xgboost as xgb
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score


def analyze_follower_metrics(df, features, targets, output_folder):
    # Create the output directory if it doesn't exist
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    X = df[features]

    for target in targets:
        print(f"\n{'=' * 20} Training for: {target} {'=' * 20}")

        # Standardize target (assuming input is percentage 0-100)
        y = df[target] / 100.0

        # Split: 70% Train, 15% Val, 15% Test
        X_train, X_temp, y_train, y_temp = train_test_split(
            X, y, test_size=0.3, random_state=14
        )
        X_val, X_test, y_val, y_test = train_test_split(
            X_temp, y_temp, test_size=0.5, random_state=14
        )

        model = xgb.XGBRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=6,
            tree_method="hist",
            early_stopping_rounds=50,
            n_jobs=-1,
            random_state=14,
        )

        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

        # --- Evaluation ---
        train_preds = model.predict(X_train)
        train_rmse = mean_squared_error(y_train, train_preds)
        train_r2 = r2_score(y_train, train_preds)
        val_preds = model.predict(X_val)
        val_rmse = mean_squared_error(y_val, val_preds)
        val_r2 = r2_score(y_val, val_preds)
        test_preds = model.predict(X_test)
        test_rmse = mean_squared_error(y_test, test_preds)
        test_r2 = r2_score(y_test, test_preds)

        print(f"Best Iteration: {model.best_iteration}")
        print(f"Train RMSE: {train_rmse:.5f}")
        print(f"Train R^2 Score: {train_r2:.5f}")
        print(f"Validation RMSE: {val_rmse:.5f}")
        print(f"Validation R^2 Score: {val_r2:.5f}")
        print(f"Test RMSE: {test_rmse:.5f}")
        print(f"Test R^2 Score: {test_r2:.5f}")

        # --- Feature Importance Plot ---
        importance_dict = model.get_booster().get_score(importance_type="gain")
        gain = pd.DataFrame(list(importance_dict.items()), columns=["Feature", "Gain"])
        gain = gain.sort_values(by="Gain", ascending=True)

        plt.figure(figsize=(12, 6))
        plt.barh(gain["Feature"], gain["Gain"], color="skyblue", edgecolor="navy")
        plt.title(f"Feature Importance: {target} (Gain)")
        plt.xlabel("Total Gain")
        plt.grid(axis="x", linestyle="--", alpha=0.7)
        plt.tight_layout()
        plt.savefig(output_path / f"importance_{target}.png")
        plt.close()

        # --- Residual Analysis Plots ---
        residuals = y_test - test_preds
        num_features = len(features)
        cols = 3
        rows = (num_features + cols - 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(15, rows * 4))
        fig.suptitle(f"Residuals vs. Features for {target}", fontsize=16)
        axes = axes.flatten()

        for i, col in enumerate(features):
            axes[i].scatter(X_test[col], residuals, alpha=0.5, color="teal")
            axes[i].axhline(0, color="red", linestyle="--")
            axes[i].set_xlabel(col)
            axes[i].set_ylabel("Residual (Actual - Pred)")
            axes[i].set_xscale("log")
            axes[i].set_title(f"Error Distribution: {col}")

        # Hide empty subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(output_path / f"residuals_{target}.png")
        plt.close(fig)

        # --- Worst Test Points Export ---
        test_analysis = X_test.copy()
        test_analysis["actual"] = y_test
        test_analysis["pred"] = test_preds
        test_analysis["abs_error"] = abs(residuals)
        worst = test_analysis.sort_values(by="abs_error", ascending=False).head(100)
        worst.to_csv(output_path / f"worst_predictions_{target}.csv", index=False)

    print(f"\nAll results saved to: {output_path.absolute()}")


if __name__ == "__main__":
    df = pd.read_csv("./data/follower_metrics2.csv")
    df["other_nodes_proposals_per_sec"] = (
        df["leader_proposals_per_sec"] + df["other_followers_proposals_per_sec"]
    )
    features = [
        # "latency_to_leader",
        # "latency_to_majority_cq",
        "latency_to_fast_fq",
        # "latency_to_all_max",
        # "own_proposals_per_sec",
        # "leader_proposals_per_sec",
        # "other_followers_proposals_per_sec",
        # "max_follower_proposals_per_sec",
        "other_nodes_proposals_per_sec",
        # "number_of_nodes",
    ]
    targets = [
        "successful_rate",
        "collision_rate",
        "leader_overwrite_rate",
        "follower_overwrite_rate",
    ]
    result_dir = "./results/features_two/"
    analyze_follower_metrics(df, features, targets, result_dir)
