FROM python:3.9-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Add debugging tools
RUN pip install --no-cache-dir ipdb

# Command to run the application with debugging enabled
CMD ["python", "-u", "-X", "dev", "controller.py"]