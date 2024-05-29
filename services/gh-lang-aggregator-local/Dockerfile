# Use the official Python image
FROM python:3.9-slim

# Set working directory in the container
WORKDIR /app

COPY requirements.txt .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script into the container
COPY main.py .

# Run the Python script
CMD ["python", "main.py"]
