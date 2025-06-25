FROM openjdk:17-slim

# Install Python and pip due different base image
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the cleaner script
COPY . .

# Run the cleaner script
CMD ["python3", "cleaner.py"]
