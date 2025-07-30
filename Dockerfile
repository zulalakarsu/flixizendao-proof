FROM python:3.12-slim

# Install system dependencies for cryptography
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install any Python dependencies your application needs, e.g.:
RUN pip install --no-cache-dir requests

RUN mkdir /sealed && chmod 777 /sealed

WORKDIR /app

COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "my_proof"]
