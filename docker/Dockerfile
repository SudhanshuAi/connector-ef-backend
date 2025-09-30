# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory which will contain our app
WORKDIR /app

ENV PYTHONPATH "${PYTHONPATH}:/app"

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Copy the requirements file first to leverage Docker's build cache
COPY docker/requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the necessary application source code
COPY app.py .
COPY extractors ./extractors

# Expose the port the app runs on
EXPOSE 5001

# Run the app.
CMD ["python", "app.py"]