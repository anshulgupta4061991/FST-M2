# Specify a base image
FROM docker.io/node:18-alpine

# Switch Workdir to /usr/app
WORKDIR /usr/app

# Copy our current directory into
# the container
COPY ./ ./

# Install the dependencies
RUN npm install

# Default Command
CMD ["npm", "start"]