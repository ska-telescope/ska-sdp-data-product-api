FROM postgres:16.3-alpine

# Set environment variable for the password (replace 'password' with your desired password)
ENV POSTGRES_PASSWORD=password

# Create the postgres data directory
RUN mkdir -p /var/lib/postgresql/data/postgres

# Expose the default postgres port (5432)
EXPOSE 5432

CMD ["postgres", "-d"]