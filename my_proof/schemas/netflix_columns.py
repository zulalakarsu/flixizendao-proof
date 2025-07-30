# my_proof/schemas/netflix_columns.py

VIEWING_REQUIRED = {
    "duration", "start time", "profile name", "title"
}

BILLING_REQUIRED = {
    "transaction date", "gross sale amt", "currency"
}

# We will consider the file valid if ≥ 50 % of required columns are present
REQUIRED_THRESHOLD = 0.5 