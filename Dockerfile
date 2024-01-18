# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory to /app
WORKDIR /bireports

# Copy the current directory contents into the container at /app
COPY . /bireports

# Print the contents of /pipeline (for debugging)
RUN ls -l /bireports

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variable for Python to run in unbuffered mode
ENV PYTHONUNBUFFERED 1
 
