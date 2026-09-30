# Lakehouse foundation

Phase 2 introduces an isolated local lakehouse control/data plane without removing HDFS.

```text
Spark 3.5.9
   |
   +--> Iceberg 1.11.0 tables
            |
            +--> Polaris REST Catalog
            |       |
            |       +--> PostgreSQL metadata
            |
            +--> MinIO S3-compatible storage (local only)
                    |
                    +--> Trino 483 SQL
```

## Local startup

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.lakehouse.yml --profile lakehouse up -d --build
```

Endpoints: Trino `http://localhost:8080`, Polaris `http://localhost:8181`, RustFS S3 API `http://localhost:9000`, RustFS console `http://localhost:9001`.

## Migration rule

HDFS remains the legacy serving path while Spark writes are introduced into Iceberg in parallel. No HDFS table is deleted until equivalence checks exist for Bronze/Silver/Gold results.

## Production storage

RustFS is the local S3-compatible adapter. Cloud environments will map the same Iceberg/Polaris interfaces to AWS S3, Azure ADLS Gen2 or Google Cloud Storage through Terraform and environment-specific credentials.
