# Use the official Node.js 20 image as a base
FROM node:20

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json files to the working directory
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application code to the working directory
COPY src/ ./src

# Install typescript and ts-node
RUN npm install typescript ts-node -g

# Expose the port on which your app runs
EXPOSE 3000

# Command to run your application
CMD ["npm", "start"]
