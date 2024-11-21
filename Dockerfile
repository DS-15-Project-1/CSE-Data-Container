# Use the official Nginx image as a base
FROM nginx:latest

# Set the working directory
WORKDIR /usr/share/nginx/html

# Copy the custom nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose port 8080
EXPOSE 8080

# Set the default command to run when starting the container
CMD ["nginx", "-g", "daemon off;"]
