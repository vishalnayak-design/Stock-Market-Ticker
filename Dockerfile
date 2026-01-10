# Use an official Python runtime as a parent image (Stable Bullseye)
FROM python:3.10-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY stock_ticker/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Create the data directory
RUN mkdir -p stock_ticker/data

# Expose the port Streamlit runs on
EXPOSE 8501

# Make the entrypoint executable
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

# Run the entrypoint script
CMD ["./docker-entrypoint.sh"]
