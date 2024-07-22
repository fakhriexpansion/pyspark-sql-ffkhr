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

#RUN python3 -m pip install — upgrade pip
RUN python3 -m venv myenv


# Set the PATH environment variable to include the virtual environment's bin directory
ENV PATH="/app/myenv/bin:$PATH"
RUN pip3 install -r requirements.txt
CMD ["python3", "processing.py"]
