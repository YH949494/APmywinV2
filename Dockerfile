# Use slim python base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Avoid Python writing pyc files, force stdout flush
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Upgrade pip first
RUN pip install --upgrade pip

# Copy requirements first (to leverage caching if only code changes)
COPY requirements.txt .

# Always force reinstall to avoid old cache
RUN pip install --no-cache-dir --force-reinstall -r requirements.txt

# Copy the rest of the bot
COPY . .

# Run your bot
CMD ["python", "main.py"]
