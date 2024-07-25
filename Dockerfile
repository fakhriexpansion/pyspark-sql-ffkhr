# using base image. Use ubuntu just for simplicity as the time ticking
FROM ubuntu:latest

# Update apt-get and install openjdk
RUN apt-get update -qq && \
    apt-get install -y openjdk-17-jdk wget unzip && \
    apt-get install -y python3 python3-pip python3-dev python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working environment & copy files
WORKDIR /app
COPY . .

# Build virtual environment for python named myenv
RUN python3 -m venv myenv


# Set the PATH environment variable to include the virtual environment's bin directory
ENV PATH="/app/myenv/bin:$PATH"

# Install python dependencies
RUN pip3 install -r requirements.txt

# Run python script
CMD ["python3", "processing.py"]
