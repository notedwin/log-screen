FROM python:3.7-slim

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker \
    pandas \
    sqlalchemy==1.4.48 \
    fabric

# fabric sucks so abd

# sqlite3 is include in std lib

ENV DAGSTER_HOME=/opt/dagster/app

WORKDIR $DAGSTER_HOME

ADD data_cow /opt/dagster/app/data_cow

# COPY repo.py /opt/dagster/app

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "data_cow"]