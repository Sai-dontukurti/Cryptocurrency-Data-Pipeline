# Use a lightweight Python 3.8 image
FROM python:3.8-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the local files into the container
COPY . /app

# Install necessary Python dependencies
RUN pip install --no-cache-dir requests pandas

# Specify the command to run your Python script
CMD ["python", "./API_calling.py"]
