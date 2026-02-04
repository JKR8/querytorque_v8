# PostgreSQL (tpch10) Connection Details

## Status
- Last known: stopped (2026-02-02)

## Connection Info
- Host (TCP): 127.0.0.1
- Port: 5433
- Unix socket dir: /mnt/d/pgdata
- Data directory: /mnt/d/pgdata
- Database: tpch10
- User: jakc9
- Password: jakc9

## Start / Stop

Start:
```
/usr/lib/postgresql/16/bin/pg_ctl -D /mnt/d/pgdata -l /mnt/d/pgdata/logfile -o "-p 5433 -k /mnt/d/pgdata" start
```

Stop:
```
/usr/lib/postgresql/16/bin/pg_ctl -D /mnt/d/pgdata stop
```

## Quick Test
```
/usr/lib/postgresql/16/bin/psql -p 5433 -h /mnt/d/pgdata -d tpch10 -c "SELECT COUNT(*) FROM lineitem;"
```
