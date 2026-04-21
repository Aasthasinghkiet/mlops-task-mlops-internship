import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys
import os

def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def write_metrics(output_path, data):
    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    args = parser.parse_args()

    start_time = time.time()

    setup_logging(args.log_file)
    logging.info("Job started")

    try:
        # ================= CONFIG =================
        if not os.path.exists(args.config):
            raise Exception("Config file not found")

        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        for key in ["seed", "window", "version"]:
            if key not in config:
                raise Exception(f"Missing config key: {key}")

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)
        logging.info(f"Config loaded: {config}")

        # ================= DATA LOAD =================
        if not os.path.exists(args.input):
            raise Exception("Input file not found")

        try:
            df = pd.read_csv(args.input)

            # Handle broken CSV (single column case)
            if len(df.columns) == 1:
                df = df[df.columns[0]].str.split(",", expand=True)

        except Exception as e:
            raise Exception(f"Invalid CSV format: {str(e)}")

        if df.empty:
            raise Exception("CSV is empty")

        # Assign column names safely
        expected_cols = ["timestamp", "open", "high", "low", "close", "volume_btc", "volume_usd"]
        if len(df.columns) == len(expected_cols):
            df.columns = expected_cols

        # Convert close to numeric
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        # Drop invalid rows
        df = df.dropna(subset=["close"])

        # Clean column names
        df.columns = df.columns.str.strip().str.lower()

        logging.info(f"Columns found: {df.columns.tolist()}")

        if "close" not in df.columns:
            raise Exception(f"Missing 'close' column. Found: {df.columns.tolist()}")

        logging.info(f"Rows loaded: {len(df)}")

        # ================= PROCESS =================
        df["rolling_mean"] = df["close"].rolling(window=window).mean()
        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        logging.info("Processing completed")

        # ================= METRICS =================
        rows_processed = len(df)
        signal_rate = df["signal"].mean()
        latency_ms = int((time.time() - start_time) * 1000)

        # 🔥 Added logging here
        logging.info(f"Signal rate computed: {signal_rate}")

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(float(signal_rate), 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        write_metrics(args.output, metrics)

        logging.info("Job completed successfully")

        print(json.dumps(metrics, indent=2))

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)

        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        write_metrics(args.output, error_metrics)

        logging.error(str(e))
        logging.info("Job failed")

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()