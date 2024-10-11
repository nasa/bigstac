# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the Python script into the container
COPY single.py .
COPY util ./util
COPY target_duckdb ./target_duckdb
COPY requirements.txt .
COPY suite.json .

# Install any necessary dependencies
RUN pip3 install -r requirements.txt

# Run the Python script when the container launches
CMD ["python", "single.py"]