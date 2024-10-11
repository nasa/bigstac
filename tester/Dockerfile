FROM python:3.10-slim
WORKDIR /app

# Copy the Python scripts into the container.
COPY single.py .
COPY util ./util
COPY target_duckdb ./target_duckdb
COPY requirements.txt .
COPY suite.json .

RUN pip3 install -r requirements.txt

# Where the result will be at the end.
VOLUME /app/logs

# # example run
# docker run \
#     --env S3_BUCKET="s3://overturemaps-us-west-2/release/2024-09-18.0/theme=places/type=place" \
#     --volume $(pwd)/logs:/app/logs \
#     single-runner

# Run the Python script when the container launches.
CMD python single.py --verbose --data $S3_BUCKET suite.json ; cp *.log /app/logs/.