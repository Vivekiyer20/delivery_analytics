#!/usr/bin/env python3

import os
import shutil
import logging
import pandas as pd
from github import Github
from github.GithubException import GithubException
from dotenv import load_dotenv

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

GITHUB_REPO = "Vivekiyer20/delivery_analytics"   # ✅ your exact repo
DATA_FILES = [
    ("data/processed_data.csv", "processed_data.csv"),
    ("data/duplicate_data.csv", "duplicate_data.csv"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
def main():
    logging.info("🚀 Processing delivery data")

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("❌ GITHUB_TOKEN not found in .env")

    # --------------------------------------------------
    # READ DATA
    # --------------------------------------------------
    df = pd.read_csv(os.path.join(BASE_DIR, "delivery_data.csv"))
    logging.info(f"📥 Loaded {len(df)} records")

    df["Earnings_Rs"] = (df["Completed"] * 100) - (df["Rejected"] * 50)

    summary = df.groupby("Driver", as_index=False).agg({
        "Target": "sum",
        "Completed": "sum",
        "Rejected": "sum",
        "Earnings_Rs": "sum"
    })

    total = pd.DataFrame([{
        "Driver": "TOTAL",
        "Target": summary["Target"].sum(),
        "Completed": summary["Completed"].sum(),
        "Rejected": summary["Rejected"].sum(),
        "Earnings_Rs": summary["Earnings_Rs"].sum()
    }])

    summary = pd.concat([summary, total], ignore_index=True)

    # --------------------------------------------------
    # SAVE FILES
    # --------------------------------------------------
    summary.to_csv("processed_data.csv", index=False)
    shutil.copyfile("processed_data.csv", "duplicate_data.csv")

    logging.info("💾 Local CSV files written")

    # --------------------------------------------------
    # GITHUB UPLOAD (CREATE OR UPDATE)
    # --------------------------------------------------
    gh = Github(token)
    repo = gh.get_repo(GITHUB_REPO)

    for repo_path, local_file in DATA_FILES:
        with open(local_file, "r") as f:
            content = f.read()

        try:
            existing = repo.get_contents(repo_path, ref="main")
            repo.update_file(
                path=existing.path,
                message=f"Update {local_file}",
                content=content,
                sha=existing.sha,
                branch="main"
            )
            logging.info(f"🔁 Updated {repo_path}")

        except GithubException as e:
            if e.status == 404:
                repo.create_file(
                    path=repo_path,
                    message=f"Create {local_file}",
                    content=content,
                    branch="main"
                )
                logging.info(f"🆕 Created {repo_path}")
            else:
                raise

    logging.info("🎉 DONE — pipeline successful")

# --------------------------------------------------
if __name__ == "__main__":
    main()