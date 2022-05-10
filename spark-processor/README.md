Create casandra schema

```
docker exec -it cassandra-iot bash
cqlsh --username cassandra --password cassandra  -f /schema.cql
```