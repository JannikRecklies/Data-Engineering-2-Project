# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir pulsar-client

# Run consumer.py when the container launches
CMD ["python", "-u", "consumer.py"]

# TODO: Remove the following lines
# Because I am lazy:
# sudo docker build -t jannikrecklies/pulsar-consumer-1:latest .
# sudo docker push jannikrecklies/pulsar-consumer-1:latest