FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the SQL conversion script
COPY convert_sql.py .

# Copy the SQL files
COPY inst/ ./inst/

# Copy the merge script
COPY merge.sql .

# Create directory for database mount
RUN mkdir -p /app/data

# Make the script executable
RUN chmod +x convert_sql.py

# Set the default command
CMD ["python", "convert_sql.py"]
