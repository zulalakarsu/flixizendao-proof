import logging
import os
import pandas as pd
import datetime as dt
import requests
import tempfile
from pathlib import Path
from io import BytesIO

from my_proof.models.proof_response import ProofResponse
from my_proof.utils.blockchain import BlockchainClient
from my_proof.config import settings
from my_proof.schemas.netflix_columns import (
    VIEWING_REQUIRED, BILLING_REQUIRED, REQUIRED_THRESHOLD
)
from my_proof.utils.bloom_filter import BloomFilter, hash_csv_row, detect_new_rows


# Define required and optional Netflix files
REQUIRED_FILES = {
    "ViewingActivity.csv": "netflix-viewing-activity"
}

OPTIONAL_FILES = {
    "BillingHistory.csv": "netflix-billing-history"
}

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
    def _download_from_drive(self, file_id: str, access_token: str) -> bytes:
        """Download encrypted file from Google Drive."""
        try:
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
            headers = {"Authorization": f"Bearer {access_token}"}
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            return response.content
        except Exception as e:
            raise Exception(f"Failed to download from Drive: {e}")

    # ------------------------------------------------------------------ #
    def _decrypt_file(self, encrypted_data: bytes, signature: str) -> bytes:
        """Decrypt file using wallet signature."""
        try:
            # Import crypto utilities
            from my_proof.utils.crypto import client_side_decrypt
            
            # Decrypt the data
            decrypted_data = client_side_decrypt(encrypted_data, signature)
            return decrypted_data
        except Exception as e:
            raise Exception(f"Failed to decrypt file: {e}")

    # ------------------------------------------------------------------ #
    def generate(self) -> ProofResponse:
        logging.info("⇢ starting Netflix-CSV proof")
        file_stats: dict[str, dict] = {}

        # Get Drive credentials from environment
        drive_file_id = os.getenv("DRIVE_FILE_ID")
        drive_access_token = os.getenv("DRIVE_OAUTH_TOKEN")
        wallet_signature = os.getenv("WALLET_SIGNATURE")
        row_hashes_json = os.getenv("ROW_HASHES_JSON", "[]")  # Client-provided row hashes

        if not drive_file_id or not drive_access_token or not wallet_signature:
            self._append_error("MISSING_DRIVE_CREDENTIALS")
            self.proof_response.valid = False
            return self.proof_response

        # Duplicate-contribution guard
        if self.blockchain_available and settings.OWNER_ADDRESS:
            if self.blockchain_client.get_contributor_file_count() > 0:
                self._append_error("DUPLICATE_CONTRIBUTION")

        # -------------------------------------------------------------- #
        try:
            # Download encrypted file from Drive
            logging.info(f"⇢ downloading encrypted file from Drive: {drive_file_id}")
            encrypted_data = self._download_from_drive(drive_file_id, drive_access_token)
            
            # Decrypt the file
            logging.info("⇢ decrypting file with wallet signature")
            decrypted_data = self._decrypt_file(encrypted_data, wallet_signature)
            
            # Create temporary file for pandas processing
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                temp_file.write(decrypted_data)
                temp_file_path = temp_file.name

            try:
                # Read CSV with pandas
                df = pd.read_csv(temp_file_path)
                
                if df.empty or df.shape[1] < 3 or len(df) < 1:
                    self._append_error(f"CSV_TOO_SMALL: Drive file (rows={len(df)}, cols={df.shape[1]})")
                    return self.proof_response

                header = {c.strip().lower() for c in df.columns}
                # Note: For extra resiliency, could use: .str.lower().str.replace(" ", "_")
                is_view = len(header & VIEWING_REQUIRED)  >= len(VIEWING_REQUIRED)  * REQUIRED_THRESHOLD
                is_bill = len(header & BILLING_REQUIRED) >= len(BILLING_REQUIRED) * REQUIRED_THRESHOLD
                if not (is_view or is_bill):
                    self._append_error("UNRECOGNISED_CSV_STRUCTURE")
                    return self.proof_response

                # ---------- duplicate detection -------------------------------------- #
                import json
                try:
                    client_hashes = json.loads(row_hashes_json)
                    if not isinstance(client_hashes, list):
                        client_hashes = []
                except:
                    client_hashes = []
                
                # Generate server-side hashes for verification
                server_hashes = []
                for _, row in df.iterrows():
                    row_str = ",".join(str(val) for val in row.values)
                    server_hashes.append(hash_csv_row(row_str))
                
                # Initialize Bloom filter (in practice, this would be loaded from persistent storage)
                bloom_filter = BloomFilter(expected_elements=1000000, false_positive_rate=0.01)
                
                # Detect new vs duplicate rows
                duplicate_stats = detect_new_rows(server_hashes, bloom_filter)
                
                # Verify client hashes match server hashes (basic integrity check)
                hash_mismatch = len(client_hashes) != len(server_hashes)
                if hash_mismatch:
                    logging.warning(f"Hash count mismatch: client={len(client_hashes)}, server={len(server_hashes)}")

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

                file_stats["drive_file"] = dict(
                    rows=total_rows,
                    new_rows=new_rows,
                    duplicate_rows=duplicate_rows,
                    uniqueness_ratio=round(uniqueness_ratio, 3),
                    columns=list(df.columns),
                    bytes=len(encrypted_data),
                    file_type=file_type,
                    quality=quality,
                    drive_file_id=drive_file_id,
                    hash_mismatch=hash_mismatch,
                )

            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except:
                    pass

        except Exception as e:
            self._append_error(f"DRIVE_PROCESSING_ERROR: {e}")
            return self.proof_response

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

