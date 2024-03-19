# Use an official Python runtime as a parent image
FROM python:3.10.13-alpine3.19

# Set the working directory in the container to /app
WORKDIR /app

RUN apk add --no-cache gcc musl-dev linux-headers

COPY setup.py /app
COPY scraper_place /app/scraper_place

# Install any needed packages specified in requirements.txt
RUN pip install --editable .

# Copy the current directory contents into the container at /app
COPY config.ini /app/config.ini

# Make port 80 available to the world outside this container
EXPOSE 80

# tail dev null
CMD ["tail", "-f", "/dev/null"]

