FROM python:3.10-slim

# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files to disk
# PYTHONUNBUFFERED: Ensures Python output is logged straight to the terminal
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Create a non-root system user and group for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements first to cache the pip install step
COPY requirements.txt /app/

# Install dependencies without storing the cache
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual application code into the container
COPY . /app/

# Transfer ownership of the files to our secure non-root user
RUN chown -R appuser:appgroup /app

# Switch to the secure user
USER appuser

# Command that runs when the container starts
CMD ["python", "main.py"]