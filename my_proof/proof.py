import logging
import os
import pandas as pd
import datetime as dt
from pathlib import Path

from my_proof.models.proof_response import ProofResponse
from my_proof.utils.blockchain import BlockchainClient
from my_proof.config import settings
from my_proof.schemas.netflix_columns import (
    VIEWING_REQUIRED, BILLING_REQUIRED, REQUIRED_THRESHOLD
)
from my_proof.utils.bloom_filter import BloomFilter, hash_csv_row, detect_new_rows


# Minimum proof threshold
PROOF_THRESHOLD = 0.5

class Proof:
    def __init__(self):
        self.proof_response = ProofResponse(dlp_id=settings.DLP_ID)
        self.errors: list[str] = []
        try:
            self.blockchain_client = BlockchainClient()
            self.blockchain_available = True
        except Exception as e:
            logging.warning(f"Blockchain client init failed: {e}")
            self.blockchain_available = False

    # ------------------------------------------------------------------ #
    def _append_error(self, msg: str):
        self.errors.append(msg)
        logging.error(msg)

    # ------------------------------------------------------------------ #
    def generate(self) -> ProofResponse:
        logging.info("⇢ starting Netflix-CSV proof")
        file_stats: dict[str, dict] = {}

        # Duplicate-contribution guard
        if self.blockchain_available and settings.OWNER_ADDRESS:
            if self.blockchain_client.get_contributor_file_count() > 0:
                self._append_error("DUPLICATE_CONTRIBUTION")

        # -------------------------------------------------------------- #
        for name in os.listdir(settings.INPUT_DIR):
            fpath = Path(settings.INPUT_DIR) / name
            if fpath.suffix.lower() != ".csv":
                continue

            try:
                df = pd.read_csv(fpath)
            except Exception as e:
                self._append_error(f"CSV_READ_ERROR: {e}")
                continue

            if df.empty or df.shape[1] < 3 or len(df) < 1:
                self._append_error(f"CSV_TOO_SMALL: {name} (rows={len(df)}, cols={df.shape[1]})")
                continue

            header = {c.strip().lower() for c in df.columns}
            # Note: For extra resiliency, could use: .str.lower().str.replace(" ", "_")
            is_view = len(header & VIEWING_REQUIRED)  >= len(VIEWING_REQUIRED)  * REQUIRED_THRESHOLD
            is_bill = len(header & BILLING_REQUIRED) >= len(BILLING_REQUIRED) * REQUIRED_THRESHOLD
            if not (is_view or is_bill):
                self._append_error("UNRECOGNISED_CSV_STRUCTURE")
                continue

            # ---------- duplicate detection -------------------------------------- #
            # Generate server-side hashes for verification
            server_hashes = []
            for _, row in df.iterrows():
                row_str = ",".join(str(val) for val in row.values)
                server_hashes.append(hash_csv_row(row_str))
            
            # Initialize Bloom filter (in practice, this would be loaded from persistent storage)
            bloom_filter = BloomFilter(expected_elements=1000000, false_positive_rate=0.01)
            
            # Detect new vs duplicate rows
            duplicate_stats = detect_new_rows(server_hashes, bloom_filter)

            # ---------- metrics -------------------------------------- #
            total_rows = len(df)
            new_rows = duplicate_stats["new_rows"]
            duplicate_rows = duplicate_stats["duplicate_rows"]
            uniqueness_ratio = duplicate_stats["uniqueness_ratio"]
            
            non_null = 1.0 - df.isna().mean().mean()
            # Guard against NaN non_null (when entire row is null)
            if pd.isna(non_null):
                non_null = 0.0
            
            # Adjust volume bonus based on new rows only
            volume_bonus = min(new_rows / 10_000, 0.50)
            recency_bonus = 0.0
            if is_view:
                col = next((c for c in df.columns if c.lower().startswith("start time")), None)
                if col and pd.api.types.is_datetime64_any_dtype(pd.to_datetime(df[col], errors="coerce")):
                    recent_cut = dt.datetime(2020, 1, 1)
                    if any(pd.to_datetime(df[col], errors="coerce") >= recent_cut):
                        recency_bonus = 0.10

            # Quality score now considers uniqueness
            base_quality = non_null * 0.6
            uniqueness_bonus = uniqueness_ratio * 0.3  # Reward for unique data
            quality = round(min(base_quality + volume_bonus + recency_bonus + uniqueness_bonus, 1.0), 3)
            
            file_type = "netflix-viewing-activity" if is_view else "netflix-billing-history"

            file_stats[name] = dict(
                rows=total_rows,
                new_rows=new_rows,
                duplicate_rows=duplicate_rows,
                uniqueness_ratio=round(uniqueness_ratio, 3),
                columns=list(df.columns),
                bytes=fpath.stat().st_size,
                file_type=file_type,
                quality=quality,
            )

        # -------------------------------------------------------------- #
        if not file_stats:
            self._append_error("NO_VALID_CSV_FILES")

        # Final decision ------------------------------------------------ #
        self.proof_response.valid   = len(self.errors) == 0
        self.proof_response.quality = max((s["quality"] for s in file_stats.values()), default=0.0)
        self.proof_response.uniqueness   = 1.0
        self.proof_response.ownership    = 1.0 if settings.OWNER_ADDRESS else 0.0
        self.proof_response.score  = (
              self.proof_response.quality   * 0.6
            + self.proof_response.uniqueness* 0.3
            + self.proof_response.ownership * 0.1
        )

        # Threshold gate
        if self.proof_response.score < PROOF_THRESHOLD:
            self.proof_response.valid = False
            self._append_error("SCORE_BELOW_THRESHOLD")

        # attributes / metadata
        self.proof_response.attributes = {
            "files": file_stats,                # full per-file info
            "errors": self.errors,
        }
        self.proof_response.metadata = {"schema_type": "netflix-csv"}

        logging.info(f"⇢ proof complete: valid={self.proof_response.valid} "
                     f"score={self.proof_response.score}")
        return self.proof_response

