# pgMemento

![Tests](https://github.com/pgMemento/pgMemento/workflows/pgmemento-tests/badge.svg)

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/pgmemento_logo.png "pgMemento Logo")

pgMemento provides an audit trail for your data inside a PostgreSQL
database using triggers and server-side functions written in PL/pgSQL.
It also tracks DDL changes to enable schema versioning and offers
powerful algorithms to restore or repair past revisions.

Only deltas of changes are logged and they are stored in one data log
table as JSONB. Transaction and table event metadata is recorded in
separate log tables making it easier to browse through past write
operations.

## Quickstart

To use pgMemento as an extension download the pgmemento-<release-version>.zip
archive either from the release on [GitHub](https://github.com/pgMemento/pgMemento/releases)
or [PGXN](https://pgxn.org/dist/pgmemento/), change to extracted folder
and run the following commands from within a shell environment:

```bash
make
sudo make install
```

Then, connect to your database where you want to use auditing and run:

```sql
CREATE EXTENSION pgmemento;
```

Alternatively, you can also add pgMemento to your database the classic
way by running the INSTALL_PGMEMENTO.sql script, which simply executes
all SQL files in the right order and updates the search path.

```sql
psql -h localhost -p 5432 -U my_user -d my_database -f INSTALL_PGMEMENTO.sql
```

All of pgMemento's log tables and functions are created in a separate
database schema called `pgmemento`. Auditing can be started per schema
with the `init` command, e.g. for the `public` schema it could be:

```sql
SELECT pgmemento.init('public');
```

A new column `pgmemento_audit_id` is added to every table in the given
schema to trace different row versions in the central data log - the
`pgmemento.row_log` table. Each write transaction is also logged in the
`pgmemento.transaction_log` which can consist of multiple table events
stored in the `pgmemento.table_event_log` table. The `event_key` column
in the latter links to the entries in the data log.

Schema versioning takes place in the `pgmemento.audit_table_log` and
`pgmemento.audit_column_log`. Transaction ranges show the life time of
an audited table and its columns. Changing the table schema, e.g.
altering data types or dropping entire columns will produce data logs as
well. Auditing can also be started automatically for newly created
tables in schemas where pgMemento has been initialized. This is tracked
(and configurable) in the `pgmemento.audit_schema_log`.

## System requirements

* PostgreSQL 9.5+
* PL/pgSQL language

## Documentation

Documentation can be found in the [wiki](https://github.com/pgMemento/pgMemento/wiki/Home) section of this repository.

## License

The scripts for pgMemento are open source under GNU Lesser General
Public License Version 3.0. See the file LICENSE for more details.

## Disclaimer

pgMemento IS PROVIDED "AS IS" AND "WITH ALL FAULTS."
I MAKE NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL I BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL,
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF
I HAVE BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
STILL, I WOULD FEEL SORRY.
