# pgMemento

[![Build Status](https://travis-ci.org/pgMemento/pgMemento.svg?branch=master)](https://travis-ci.org/pgMemento/pgMemento)

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/pgmemento_logo.png "pgMemento Logo")

pgMemento provides an audit trail for your data inside a PostgreSQL
database using triggers and server-side functions written in PL/pgSQL.
It also tracks DDL changes to enable schema versioning and offers
powerful algorithms to restore or repair past revisions.


## Index

1. License
2. About
3. System requirements
4. Background & References
5. Installation
6. Logging behaviour
7. Reverting transactions
8. Restoring previous versions
9. Branching
10. Future Plans
11. Media
12. Developers
13. Contact
14. Special thanks
15. Disclaimer


## 1. License

The scripts for pgMemento are open source under GNU Lesser General 
Public License Version 3.0. See the file LICENSE for more details. 


## 2. About

pgMemento logs DML and DDL changes inside a PostgreSQL database. 
These logs are bound to events and transactions and not to timestamp
fields that specify the validity interval as seen in many other auditing
approaches. This allows for rolling back events selectively by keeping 
the database consistent.

pgMemento uses triggers to log the changes. The OLD and the NEW version
of a tuple are accessable inside the corresponding trigger procedures.
pgMemento only logs the OLD version as the recent state can be queried
from the table. It breaks this priciple down on a columnar level meaning
that only deltas between OLD and NEW are stored when UPDATEs occur. Of
course, this is an overhead but it pays off in saving disk space and in
making rollbacks easier.

Logging only fragments can produce sparsely filled history/audit tables. 
Using a semistructured data type like JSONB can make the data logs more 
compact. In general, using JSONB for auditing has another big advantage:
The audit mechanism (triggers and audit tables) does not need to adapt
to schema changes. Actually, you do not even need history tables for each 
audited table (sometimes also called 'shadow tables'). All logs can be
written to one central table with a JSONB field. 

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/generic_logging.png "Generic logging")

To trace different versions of a tuple in the log table a synthetical key
is created in each audited table called `audit_id`. This is easier than
relying on a table's primary key which can be defined on multiple tables
and for different data types. Audit_ids are unique in a (single node)
database.

pgMemento provides functions to recreate a former table or database state
in a separate database schema incl. constraints and indexes. As event 
triggers are capturing any schema changes, the restored table or database
will have the layout of the past state. Historic versions of tuples and
tables can also be queried on-the-fly through provided funtcions.

An audit trail like pgMemento is probably not ideal for write-instensive
databases. However, as only OLD data is logged it will certainly take
less time to run out of disk space than other solutions. Nevertheless,
obsolete content can simply be removed from the logs at any time without
affecting the versioning mechanism.

pgMemento is written in plain PL/pgSQL. Thus, it can be set up on every
machine with PostgreSQL 9.5 or higher. I tagged a first version of 
pgMemento (v0.1) that works with the JSON data type and can be used along
with PostgreSQL 9.3. But, it is slower and can not handle very big JSON
strings. Releases v0.2 and v0.3 require at least PostgreSQL 9.4. The 
master uses JSONB functions introduced in PostgreSQL 9.5. I recommend to
always use the newest version of pgMemento.


## 3. System requirements

* PostgreSQL 9.5


## 4. Background & References

The auditing approach of pgMemento is nothing new. Define triggers to log
changes in your database is a well known practice. There are other tools 
which can also be used. When I started the development for pgMemento I
wasn't aware of that there are so many solutions out there (and new ones
popping up every once in while).

If you want a clearer table structure for logged data, say a history
table for each audited table, have a look at [tablelog](http://pgfoundry.org/projects/tablelog/) 
by Andreas Scherbaum. It's easy to query different versions of a row. 
Restoring former states is also possible. It writes all the data twice,
though. Runs only on Linux.

If you prefer to work with validity intervals for each row try out the
[temporal_tables](http://pgxn.org/dist/temporal_tables/) extension by Vlad Arkhipov or the
[table_version](http://pgxn.org/dist/table_version) extension by Jeremy Palmer.
[This talk](http://pgday.ru/files/papers/9/pgday.2015.magnus.hagander.tardis_orm.pdf) by Magnus Hagander goes in a similar direction.

If you like the idea of generic logging, but you prefer hstore over 
JSONB check out [audit trigger 91plus](http://wiki.postgresql.org/wiki/audit_trigger_91plus) by Craig Ringer.
It does not provide functions to restore previous database state or to 
rollback certain transactions.

If you want to use a tool, that's proven to run in production for several
years take a closer look at [Cyan Audit](http://pgxn.org/dist/cyanaudit/) by Moshe Jacobsen.
Logs are structured on columnar level, so auditing can also be switched
off for certain columns. DDL changes on tables are caught by an event 
trigger. Rollbacks of transactions are possible for single tables. 

If you think the days for using triggers for auditing are numbered because
of the new logical decoding feature of PostgreSQL you are probably right.
But this technology is still young and there are not many tools out there 
that provide the same functionality like pgMemento. A notable 
implementation is [Logicaldecoding](https://github.com/sebastian-r-schmidt/logicaldecoding) 
by Sebastian R. Schmidt. [pgaudit](https://github.com/2ndQuadrant/pgaudit) by 2ndQuadrant 
and its [fork](https://github.com/pgaudit/pgaudit) by David Steele are 
only logging transaction metadata at the moment and not the data itself.


## 5. Installation

### 5.1. Add pgMemento to a database

A brief introduction about the different SQL files:
* `DDL_LOG.sql` enables logging of schema changes (DDL statements)
* `LOG_UTIL.sql` provides some helper functions for handling the audited information
* `REVERT.sql` contains procedures to rollback changes of a certain transaction and
* `SCHEMA_MANAGEMENT.sql` includes functions to define constraints in the schema where tables have been restored
* `SETUP.sql` contains DDL scripts for tables and basic setup functions
* `VERSIONING.sql` is necessary to restore past tuple/table/database states

Run the `INSTALL_PGMEMENTO.sql` script with the psql client of 
PostgreSQL. Now a new schema will appear in your database called 
`pgmemento`. As of version 0.4 the `pgmemento` schema consist of 
5 log tables and 2 view:

* `TABLE audit_column_log`: Stores information about columns of audited tables (DDL log target)
* `TABLE audit_table_log`: Stores information about audited tables (DDL log target)
* `TABLE row_log`: Table for data log (DML log target)
* `TABLE table_event_log`: Stores metadata about table events related to transactions (DML log target)
* `TABLE transaction_log`: Stores metadata about transactions (DML log target)
* `VIEW audit_tables`: Displays tables currently audited by pgMemento incl. information about the transaction range
* `VIEW audit_tables_dependency`: Lists audited tables in order of their dependencies with each other

The following figure shows how the log tables are referenced with each
other:

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/log_tables.png "Log tables of pgMemento")


### 5.2. Start pgMemento

To enable auditing for an entire database schema simply run the `INIT.sql`
script. First, you are requested to specify the target schema. For the 
second parameter you can define a set of tables you want to exclude from
auditing (comma-separated list). As for the third parameter you can decide
to log already existing data as inserted (by passing the number 1). This
is highly recommended in order to have a proper baseline the versioning
can reflect on. Finally, you can choose if newly created tables shall be
enabled for auditing automatically (again using the number 1). `INIT.sql`
creates event triggers for the database to track schema changes of
audited tables.

Auditing can also be enabled manually for single tables using the following
function, which adds an additional `audit_id` column to the table and
creates triggers that are fired during DML changes.

<pre>
SELECT pgmemento.create_table_audit('table_A', 'public', 1);
</pre>

With the last argument you define, if existing data is logged or not. 
For each row in the audited tables another row will be written to the 
`row_log` table telling the system that it has been 'inserted' at the 
timestamp the procedure has been executed. Depending on the number of 
tables to alter and on the amount of data that has to be defined as 
INSERTed this process can take a while. By passing the number 0 nothing
is logged in the first place. If you change your mind later, you can
still call `pgmemento.log_table_state` (or `pgmemento.log_schema_state`). 

**HINT:** When setting up a new database I would recommend to start 
pgMemento after bulk imports. Otherwise the import will be slower and 
several different timestamps might appear in the `transaction_log` table.

If `INIT.sql` has not been used event triggers can be created by calling
the following procedure:

<pre>
SELECT pgmemento.create_schema_event_trigger(1);
</pre>

By passing a 1 to the procedure an additional event trigger for 
`CREATE TABLE` events is created (not for `CREATE TABLE AS` events).

Logging can be stopped and restarted by running the `STOP_AUDITING.sql`
and `START_AUDITING.sql` scripts. Note that theses scripts do not 
remove the `audit_id` column in the logged tables.


### 5.3. Uninstall pgMemento

In order to remove pgMemento simply run the `UNINSTALL_PGMEMENTO.sql`
script.


## 6. Logging behaviour

### 6.1. What is logged

The following table provides an overview what DML and DDL events are
logged and which command is applied when reverting the event (see
chapter 7).

| OP_ID | EVENT                     | REVERSE EVENT                   | LOG CONTENT                    |
|:-----:|:--------------------------|:--------------------------------|:-------------------------------|
| 1     | CREATE TABLE'             | DROP TABLE                      | -                              |
| 2     | ALTER TABLE ADD COLUMN'   | ALTER TABLE DROP COLUMN         | -                              |
| 3     | INSERT                    | DELETE                          | NULL                           |
| 4     | UPDATE                    | UPDATE                          | changed fields of changed rows |
| 5     | ALTER TABLE ALTER COLUMN' | ALTER TABLE ALTER COLUMN        | all rows of altered columns''  |
| 6     | ALTER TABLE DROP COLUMN'  | ALTER TABLE ADD COLUMN + UPDATE | all rows of dropped columns    |
| 7     | DELETE                    | INSERT                          | all fields of deleted rows     |
| 8     | TRUNCATE                  | INSERT                          | all fields of table            |
| 9     | DROP TABLE'               | CREATE TABLE                    | all fields of table (TRUNCATE) |

' Captured by event triggers
'' Only if USING is found in the ALTER COLUMN command 

More details on the logging behaviour are explained in the next
sections.

### 6.2. DML logging

pgMemento uses two logging stages. The first trigger is fired before 
each statement on each audited table. Every transaction is only logged 
once in the `transaction_log` table. Within the trigger procedure the 
corresponding table operations are logged as well in the `table_event_log`
table. A type of table operation (e.g. INSERT, UPDATE, DELETE etc.) is
only logged once per table per transaction. For two or more operations
of the same kind logged data of subsequent events are referenced to the
first first event that has been inserted into `table_event_log`. In the
next chapter you will see why this doesn't produce consistency issues.

The second logging stage is related two the data that has changed. 
Row-level triggers are fired after each operations on the audited tables. 
Within the trigger procedure the corresponding INSERT, UPDATE, DELETE or
TRUNCATE event for the current transaction is queried and each row is 
referenced to it.

For example, an UPDATE command on `table_A` changing the value of some 
rows of `column_B` to `new_value` will appear in the log tables like this:

TRANSACTION_LOG

| ID  | txid_id  | stmt_date                | user_name  | client address  |
| --- |:-------- |:------------------------:|:----------:|:---------------:|
| 1   | 1000000  | 2017-02-22 15:00:00.100  | felix      | ::1/128         |

TABLE_EVENT_LOG

| ID  | transaction_id | op_id | table_operation | schema_name | table_name  | table_relid |
| --- |:--------------:|:-----:|:---------------:|:-----------:|:-----------:|:-----------:|
| 1   | 1000000        | 4     | UPDATE          | public      | table_A     | 44444444    |

ROW_LOG

| ID  | event_id  | audit_id | changes                  |
| --- |:---------:|:--------:|:------------------------:|
| 1   | 1         | 555      | {"column_B":"old_value"} |
| 2   | 1         | 556      | {"column_B":"old_value"} |
| 3   | 1         | 557      | {"column_B":"old_value"} |

As you can see only the changes are logged. DELETE and TRUNCATE commands
would cause logging of complete rows while INSERTs would leave a the 
`changes` field blank. Thus, there is no data redundancy.


### 6.3. DDL logging

Since v0.3 pgMemento supports DDL logging to capture schema changes.
This is important for restoring former table or database states (see
chapter 8). The two tables `audit_table_log` and `audit_column_log`
in the pgMemento schema provide information at what range of transactions
the audited tables and their columns exist. After a table is altered or 
dropped an event trigger is fired to compare the recent state (at
ddl_command_end) with the logs. pgMemento also saves data from all rows
before a `DROP SCHEMA`, `DROP TABLE` or `ALTER TABLE ... DROP COLUMN`
event occurs (at ddl_command_start).

For `ALTER TABLE ... ALTER COLUMN` events data is only logged if the
data type is changed using an explicit transformation between data types
with the signal word `USING`. Logs are not needed if altering the type
worked without an explicit cast definition because it means that the
current state of the data could be used along with a former version of
the table schema. If tables or columns are renamed it is reflected in
the audit tables but data is not logged. The same applies to `ADD COLUMN`
events. As noted in 5.2., `CREATE TABLE` will only fire a trigger if it
has been enabled previously.

**NOTE**: For correct parsing of DDL command, comments inside query
strings that fire event triggers are forbidden and will raise an
exception. At least, since v0.5 DDL changes executed from inside
functions can be extracted correctly.


### 6.4. Query the logs

The logged information can already be of use, e.g. list all transactions 
that had an effect on a certain column by using the `?` operator:

<pre>
SELECT
  t.txid 
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON t.txid = e.transaction_id
JOIN
  pgmemento.row_log r
  ON r.event_id = e.id
WHERE 
  r.audit_id = 4 
  AND (r.changes ? 'column_B');
</pre>

List all rows that once had a certain value by using the `@>` operator:

<pre>
SELECT DISTINCT
  audit_id 
FROM
  pgmemento.row_log
WHERE 
  changes @> '{"column_B": "old_value"}'::jsonb;
</pre>

To get all changes per `audit_id` of one transaction as one row of
JSONB you can use the `pgmemento.jsonb_merge` function as an aggregate
or window function. When combining it with an ordering by the `row_log`
ID it is possible to see the first or the last changes per field.

<pre>
SELECT
  r.audit_id,
  pgmemento.jsonb_merge(r.changes ORDER BY r.id) AS first_changes,
  pgmemento.jsonb_merge(r.changes ORDER BY r.id DESC) AS last_changes
FROM
  pgmemento.row_log r
JOIN
  pgmemento.table_event_log e 
  ON e.id = r.event_id
WHERE
  e.transaction_id = 1000000
GROUP BY
  r.audit_id;
</pre> 


### 6.5. Delete logs and data correction

To delete entries in the audit table pgMemento offers a simple API to
remove logs caused by one transaction (`delete_txid_log`) or one event
(`delete_table_event_log`). The function `delete_audit_table_log`
removes entries from the tables `audit_table_log` and `audit_column_log`.

One has to be sure that deleting logs changes the audit trail of the
data. Historic versions of tuples and tables will get lost. Sometimes,
this is intended, e.g. if there are semantic errors in the data. In this
case the user should be able to see a corrected version of the data also
when querying previous table states. In order to apply corrections
against the complete audit trail call `delete_key` and pass the audit_id
and column name where the correction shall be applied. This removes all
corresponding key-value pairs from the JSONB logs so that the corrected
value will be the only value of the given column for the entire audit
trail. Keep in mind that this action can also produce emtpy logs in the
`row_log` table.


## 7. Revert certain transactions

The logged information can be used to revert certain transactions that
happened in the past. Reinsert deleted rows, remove imported data, 
reverse updates etc. The procedure is called `revert_transaction`. It
loops over each row that was affected by the given transaction. For data
integrity reasons the order of operations and entries in the `row_log`
table is important. Imagine three tables A, B and C, with B and C 
referencing A. Deleting entries in A requires deleting depending rows
in B and C. The order of events in one transaction can look like this:

<pre>
Txid 1000
1. DELETE from C
2. DELETE from A
3. DELETE from B
4. DELETE from A
</pre>

As said, pgMemento can only log one DELETE event on A. So, simply
reverting the events in reverse order won't work here. An INSERT in B 
requires exitsing entries in A.

<pre>
Revert Txid 1000
1. INSERT into B <-- ERROR: foreign key violation
2. INSERT into A
3. INSERT into C
</pre>

By joining against the `audit_tables_dependency` view we can produce the
correct revert order without violating foreign key constraints. B and C
have a higher depth than A. The order will be:

<pre>
Revert Txid 1000
1. INSERT into A
2. INSERT into B
3. INSERT into C
</pre>

For INSERTs and UPDATEs the reverse depth order is used. Reverting also
works if foreign keys are set to `ON UPDATE CASCADE` or `ON DELETE CASCADE`
because the `audit_tables_dependency` produces the correct order anyway.

**NOTE**: Until v0.4, pgMemento tried to revert operations in tables
with self-references through an ordering of audit_ids. But, since this
concept would only work if hierachies have the same order, the idea was
dropped. Now, the ID from the `row_log` table is used. This allows for
reverting operation against any hierarchies, but **ONLY** if non-cascading
foreign keys are used. Otherwise, the user has to write his own revert
script and flip the audit_order.

A range of transactions can be reverted by calling:

<pre>
SELECT pgmemento.revert_transactions(lower_txid, upper_txid);
</pre>

It uses nearly the same query but with an additional ordering by 
transaction ids (DESC). When reverting many transactions an alternative 
procedure can be used called `revert_distinct_transaction`. For each
distinct audit_it only the oldest table operation is applied to make
the revert process faster. It is also provided for transaction ranges.

<pre>
SELECT pgmemento.revert_distinct_transactions(lower_txid, upper_txid);
</pre>

**NOTE**: If tables are created and dropped again during one transaction
or a range of transactions `revert_distinct_transaction` is the better
choice. Otherwise, the `txid_range` columns in tables `audit_table_log`
and `audit_column_log` will be empty (but only if CREATE TABLE events
are logged automatically).


## 8. Restore a past states of tuples, tables and schemas

The main motivation for having an audit trail might not be the ability
to undo certain changes, but to simply browse through the history of 
a tuple, table or even a whole database. When working with additional
columns that specify the lifetime of different data versions (as most
tools introduced in 4. do) this is easy by including these field into
the WHERE clause.

For a generic logging approach with only one central data log table the
biggest challenge is to provide an interface which can be used as easy
and intuitive like the shadow table design. pgMemento offers two ways:
* Previous versions can be restored on-the-fly using a function
* Previous version can restored as a VIEW or a TABLE in a separate 
  database schema to be queried like a normal table.


### 8.1. Thinking in transactions, not timestamps

To address different versions of your data with pgMemento you have to
think in transactions, not timestamps. Although, each transaction is
logged with a timestamp, you still need to know the txid to filter the
data changes. Especially when dealing with concurrent edits filtering
only by timestamps could produce inconsistent views. Establishing
versions based on transactions will produce a state of the data as the
user has seen it when he applied the changes.

**So, when pgMemento provides a function where the user can determine
the state he wants to restore by passing a transaction id (txid) it
addresses the version BEFORE the transaction excluding the changes of
this transaction!**

Nevertheless, as most users of an audit trail solution probably want
to use timestamps to query the history of their database, they could
get the next transaction id found after a given timestamp with this
query:

<pre>
SELECT
  min(txid)
FROM
  pgmemento.transaction_log
WHERE
  stmt_date >= '2017-02-22 16:00:00'
LIMIT 1;
</pre>


### 8.2. Restore internals

In the following, it will be explained in depth how the restore process
works internally? Imagine a time line like this:

`1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8` [Transactions] <br/>
`I -> U -> D -> I -> U -> U -> D -> now` [Operations] <br/>
I = Insert, U = Update, D = Delete

After the first INSERT the row in `TABLE_A` looked like this.

TABLE_A

| ID  | column_B   | column_C | audit_id |
| --- |:----------:|:--------:|:--------:|
| 1   | some_value | abc      | 555      |

Imagine that this row is updated again in transactions 5 and 6
and deleted in transaction 7 e.g.

<pre>
UPDATE table_a SET column_B = 'final_value';
UPDATE table_a SET column_C = 'def';
DELETE FROM table_a WHERE id = 1;
</pre>

In the `row_log` table this would be logged as follows:

| ID  | event_id  | audit_id | changes                                                           |
| --- |:---------:|:--------:|:-----------------------------------------------------------------:|
| ... | ...       | ...      | ...                                                               |
| 4   | 4         | 555      | NULL                                                              |
| ... | ...       | ...      | ...                                                               |
| 66  | 15        | 555      | {"column_B":"some_value"}                                         |
| ... | ...       | ...      | ...                                                               |
| 77  | 21        | 555      | {"column_C":"abc"}                                                |
| ... | ...       | ...      | ...                                                               |
| ... | ...       | ...      | ...                                                               |
| 99  | 81        | 555      | {"ID":1,"column_B":"final_value","column_C":"def","audit_id":555} |
| ... | ...       | ...      | ...                                                               |


#### 8.2.1. Fetching audit_ids
 
For restoring, pgMemento needs to know which entries were valid when
transaction 5 started. This can be done by a simple JOIN between the log
tables querying the last event of each related audit_id using `DISTINCT ON
with ORDER BY audit_id, event_id DESC`. DELETE or TRUNCATE events would
need to filtered out later.

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/fetch_auditids_en.png "Fetching Audit_IDs")

<pre>
SELECT 
  f.audit_id,
  f.event_id,
  f.op_id 
FROM (
  SELECT DISTINCT ON (r.audit_id) 
    r.audit_id,
    r.event_id, 
    e.op_id
  FROM 
    pgmemento.row_log r
  JOIN 
    pgmemento.table_event_log e 
    ON e.id = r.event_id
  JOIN 
    pgmemento.transaction_log t
    ON t.txid = e.transaction_id
  WHERE
    t.txid >= 0 AND t.txid < 5
    AND e.table_relid = 'public.table_a'::regclass::oid
  ORDER BY 
    r.audit_id,
    e.id DESC
) f
WHERE
  f.op_id < 7
</pre>

For `audit_id` 555 this query would tell us that the row did exist before
transaction 5 and that its last event had been an INSERT (`op_id` = 1)
event with the ID 4. As said in 5.2. having a baseline where already
existing data is marked as INSERTed is really important because of this
query. If there is no initial event found for an `audit_id` it will not be
restored.


#### 8.2.2. Find the right historic values

For each fetched `audit_id` a row has to be reconstructed. This is where
things become very tricky because the historic field values can be 
scattered all throughout the `row_log` table due to the pgMemento's
logging behaviour (see example in 6.). For each column we need to find
JSONB objects containing the column's name as a key. As learned in
chapter 6.3 we could seach for e.g. `(changes ? 'column_B')` plus the
`audit_id`. This would give us two entries:

| changes                                                           |
| ----------------------------------------------------------------- |
| {"column_B":"some_value"}                                         |
| {"ID":1,"column_B":"final_value","column_C":"def","audit_id":555} |

By sorting on the internal ID of the `row_log` table we get the correct 
historic order of these logs. The value in the event_id column must be
bigger than the event ID we have extracted in the previous step. 
Otherwise, we would also get the logs of former revisions which are
already outdated by the time transaction 5 happened.

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/fetch_values_en.png "Fetching values")

So, the first entry we find for `column_B` is {"column_B":"some_value"}.
This log has been produced by the UPDATE event of transaction 5. Thus,
before transaction 5 `column_B` had the value "some_value". This is what
we have asked for. We do the same query for all the other columns. For
`column_C` we find the log `{"column_C":"abc"}`. So, the historic value
before transaction 5 has been "abc". For columns ID and `audit_id` there
is only one JSONB log found: The entire row, generated by the DELETE 
query of transaction 7. We can also find other values for the two fields
`column_B` and `column_C` in this log but they were created after
transaction 5.

Imagine if the row would not have been deleted, we would not find any
logs for e.g. the ID column. In this case, we need to query the recent
state of the table. We would have to consider that the table or the
column could have been renamed or that the column could have been dropped.
pgMemento takes this into account. If nothing is found at all (which
would not be reasonable) the value will be `NULL`.


#### 8.2.3. Window functions to bring it all together

Until pgMemento v0.3 the retrieval of historic values was rolled out
in seperate queries for each column. This was too inefficient for a 
quick view into the past. 

<pre>
SELECT
  key1, -- ID
  value1,
  key2, -- column_B
  value2,
  ...
FROM (
  ... -- subquery from 8.2. (extracted event_ids and audit_ids)
) f
JOIN LATERAL (
  SELECT
    q1.key AS key1, -- ID
    q1.value AS value1,
    q2.key AS key2, -- column_B
    q2.value AS value2,
    ...
  FROM 
    (...) q1,
    (SELECT
       -- set constant for column name
       'column_B' AS key,
       -- set value, use COALESCE to handle NULLs
       COALESCE(
         -- get value from JSONB log
         (SELECT
            (changes -> 'column_B') 
          FROM 
            pgmemento.row_log
          WHERE
            audit_id = f.audit_id
            AND event_id > f.event_id
            AND (changes ? 'column_B')
          ORDER BY
            r.id
          LIMIT 1
         ),
         -- if NULL, query recent value
         (SELECT
            to_jsonb(column_B)
          FROM
            public.table_A
          WHERE
            audit_id = f.audit_id
         ),
         -- no logs, no current value = NULL
         NULL
       ) AS value
    ) q2,
    ...
) p ON (true)
</pre>

Since v0.4 pgMemento uses a window function with `FILTER` clauses 
that were introduced in PostgreSQL 9.4. This allows for searching for
different keys on same level of the query. A filter can only be used
in conjunction with an aggregate function. Luckily, with `jsonb_agg`
PostgreSQL offers a suitable function for the JSONB logs. The window
is ordered by the ID of the `row_log` table to get the oldest log first.
The window frame starts at the current row and has no upper boundary.

<pre>
SELECT
  q.key1 , -- ID
  q.value1->>0,
  q.key2, -- column_B
  q.value2->>0,
  ...
FROM (
  SELECT DISTINCT ON (a.audit_id, x.audit_id)
    -- set constant for column name
    'id'::text AS key1,
    -- set value, use COALESCE to handle NULLs
    COALESCE(
      -- get value from JSONB log
      jsonb_agg(a.changes -> 'id')
        FILTER (WHERE a.changes ? 'id')
          OVER (ORDER BY a.id ROWS BETWEEN CURRENT ROW AND CURRENT ROW),
      -- if NULL, query recent value
      to_jsonb(x.id),
      -- no logs, no current value = NULL
      NULL
    ) AS value1,
    'column_B'::text AS key2,
    -- set value, use COALESCE to handle NULLs
    COALESCE(
      jsonb_agg(a.changes -> 'column_B')
        FILTER (WHERE a.changes ? 'column_B')
          OVER (ORDER BY a.id ROWS BETWEEN CURRENT ROW AND CURRENT ROW),
      to_jsonb(x.column_B),
      NULL
    ) AS value2,
    ...
  FROM (
    ... -- subquery from 8.2. (extracted event_ids and audit_ids)
  ) f
  LEFT JOIN
    pgmemento.row_log a 
    ON a.audit_id = f.audit_id
    AND a.event_id > f.event_id
  LEFT JOIN public.table_A x
    ON x.audit_id = f.audit_id
  WHERE
    f.op_id < 7
  ORDER BY
    a.audit_id,
    x.audit_id,
    a.id
) q
</pre>

Now, the `row_log` table and the audited table only appear once in the
query. They have to be joined with an `OUTER JOIN` against the queried
list of valid audit_ids because both could be missing an `audit_id`. As 
said, this is very unlikely. For each `audit_id` only the first entry of 
the result is of interest. This is done again with `DISTINCT ON`. As we
are using a window query the extracted JSONB array for each key can 
contain all further historic values of the `audit_id` found in the logs
(`ROW BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`), no matter if we 
strip out rows with `DISTINCT ON`. As only the first element in the array
(`ORDER BY a.id`) is necessary we shrink the window to only the current
row (`ROW BETWEEN CURRENT ROW AND CURRENT ROW`). The value is extracted
in the upper query with the `->>` operator.


#### 8.2.4. Generating JSONB objects

Now, that we got an alternating list of keys and values we could simply
call the PostgreSQL function jsonb_build_object to produce complete
tuples as JSONB.

<pre>
SELECT 
  jsonb_build_object(
    q.key1 , -- ID
    q.value1->>0,
    q.key2, -- column_B
    q.value2->>0,
    ...
  ) AS log_entry
FROM (
  ... -- query from previous steps
) q
</pre>

To get the whole query described in the last chapters call
`pgmemento.restore_query`. As for the first two arguments it takes two
txids specifying the transaction range you are interested in. Then you
need to name the table and the schema, optionally followed by an 
`audit_id`, if you are only interested in a certain tuple. But, this 
function only returns the query string. You should use it along with
`pgmemento.generate_log_entry` to return single tuples as JSONB objects
or `pgmemento.generate_log_entries` to return a setof JSONB records
(see examples in next chapter).


### 8.3. From JSONB back to relational data

The last step of the restoring process is to bring these generated JSONB
objects into a tabular representation. PostgreSQL offers the function 
`jsonb_populate_record` to do this job.

<pre>
SELECT * FROM jsonb_populate_record(null::table_A, jsonb_object);
</pre>

But it cannot be written just like that because we need to combine it
with a query that returns numerous JSONB objects. There is also the 
function `jsonb_populate_recordset` to return a set of records but it
needs all JSONB objects to be aggregated which is a little overhead.
The solution is to use a `LATERAL JOIN`:

<pre>
SELECT 
  p.* 
FROM 
  generate_log_entries(
    0,
    5,
    'my_table',
    'public'
  ) AS entries
JOIN LATERAL (
  SELECT 
    *
  FROM
    jsonb_populate_record(
       null::public.my_table,
       entries
    )
) p ON (true);
</pre>

With the following query, it is possible to look at all revisions of 
one tuple. As in chapter 8.2.1 we use 0 as the lower boundary for the 
transaction id range to get all revisions.

<pre>
SELECT 
  row_number() OVER () AS revision_no,
  p.*
FROM (
  SELECT
    pgmemento.generate_log_entry(
      1,
      e.transaction_id,
      'my_table',
      'public',
      r.audit_id
    ) AS entry
  FROM 
    pgmemento.row_log r
  JOIN
    pgmemento.table_event_log e 
    ON e.id = r.event_id
  WHERE 
    r.audit_id = 12345
  ORDER BY
    e.transaction_id,
    e.id
) log
JOIN LATERAL ( 
  SELECT
    *
  FROM
    jsonb_populate_record(
      null::public.my_table,
      log.entry
    )
) p ON (true); 
</pre>


### 8.4 Creating restore templates

In the last two query examples the recent table schema has been used as
the template for the `jsonb_populate_record` function. This will only
work if the schema of the table has not changed over time. In order to
produce a correct historic replica of a table, the table schema for the
requested time (transaction) window has to be known. Now, the DDL log
tables are getting important.

The restore functions of pgMemento query the `audit_column_log` table
to put the historic table schema together. A matching template to this
process can be created with the `pgmemento.create_restore_template`
function.

<pre>
SELECT create_restore_template(
  5, 'my_template', 'my_table', 'public', 1
);
</pre>

Again, the first argument specifies the transaction id to return to.
With the second argument you choose the name of the template which
should be used later for `jsonb_populate_record`. Templates are created
as temporary tables. To preserve a table on commit, choose 1 as the last
argument. The template will live as long as the current database session.
The default is 0 which means `ON COMMIT DROP`. This is only useful when
combining the template creation with other process steps during one
transaction (see next chapter). 


### 8.5 Restore table states in separate schemas as VIEWs or TABLEs

For reasons of convenience pgMemento provides the function 
`pgmemento.restore_table_state` which combines the previous steps to
produce a historic VIEW or TABLE in another database schema.

<pre>
SELECT pgmemento.restore_table_state(
  0, 
  5, 
  'my_table',
  'public',
  'target_schema',
  'VIEW',
  1
)
</pre>

This function can also be called on behalf of a whole schema with
`pgmemento.restore_schema_state`. When restoring audited tables as
VIEWs they only exist as long as the current sessions lasts
(`ON COMMIT PRESERVE ROWS`). When creating a new session the restore
procedure must be called again. When restoring the audited tables as
BASE TABLEs, they will remain in the target schema but occupying extra
disk space.

Restoring can be run multiple times against the same schema, if the last
argument of `pgmemento.restore_table_state` is set to 1. This replaces
existing VIEWs or drops restored TABLEs with `pgmemento.drop_table_state`.
It does not matter if the target schema already exist.


## 9. Branching

So far, pgMemento does not enable hierarchical versioning where users can
work in separate branches and merge different versions with each other.
This is a feature, I had in mind since I've started the development. So,
you can find some prerequisites here and there in the code.

* There is only one global sequence for audit_ids. This would be useful
  to reference tuples accross separate branches.
* Function `pgmemento.move_table_state` can be used to copy a whole
  schema. This sets the foundation for intitializing a branch. Probably
  I should use `CREATE TABLE ... LIKE` to copy also constraints, indexes,
  triggers etc.
* There are a couple of functions to add constraints, indexes and
  sequences to a restored state (see next chapter).
* Code from `revert_transaction` might be useful for merging changes
  (ergo logs) into another branch.
* The `audit_tables` VIEW was intended to help for switching the
  production state to a restored schema. With the improvements on
  reverting transaction, this idea has been dropped.


### 9.1. Work with a past state

If past states were restored as tables they do not have primary keys 
or indexes assigned to them. References between tables are lost as well. 
If the user wants to work on the restored table or database state - 
like he would do with the production state - he can use the procedures
`pgmemento.pkey_table_state`, `pgmemento.fkey_table_state` and 
`pgmemento.index_table_state`. These procedures create primary keys,
foreign keys and indexes on behalf of the recent constraints defined
in the production schema. 

Note that if table and/or database structures have changed fundamentally 
over time it might not be possible to recreate constraints and indexes as 
their metadata is not yet logged by pgMemento. 


## 10. Future Plans

Here are some plans I have for the next release:
* Extend test logic to all functions of pgMemento
* Have log tables for primary keys, constraints, indexes etc.
* Have a view to store metadata of additional created schemas
  for former table / database states.

General thoughts:
* Better protection for log tables?
* How hard would it be to enable branching?
* Table partitioning strategy for `row_log` table (maybe [pg_pathman](https://github.com/postgrespro/pg_pathman) can help)
* Build a pgMemento PostgreSQL extension

I would be very happy if there are other PostgreSQL developers out there
who are interested in pgMemento and willing to help me to improve it.
Together we might create a powerful, easy-to-use versioning approach for
PostgreSQL.


## 11. Media

I gave a presentation in german at FOSSGIS 2015:
https://www.youtube.com/watch?v=EqLkLNyI6Yk

I gave another presentation at FOSSGIS-NA 2016:
http://slides.com/fxku/pgmemento_foss4gna16

I presented a more general take on database versioning at FOSS4G 2017 in Boston:
http://slides.com/fxku/foss4g17_dbversion

A demo paper about pgMemento got accepted at the 15th International
Symposium for Spatial and Temporal Databases (SSTD) in Arlington, VA.
You can find the publication [here](https://link.springer.com/chapter/10.1007/978-3-319-64367-0_27).


## 12. Developers

Felix Kunde


## 13. Contact

felix-kunde@gmx.de


## 14. Special Thanks

* Petra Sauer (Beuth University of Applied Sciences) --> For support and discussions on a pgMemento research paper  
* Adam Brusselback --> benchmarking and bugfixing
* Hans-Jürgen Schönig (Cybertech) --> recommend to use a generic JSON auditing
* Christophe Pettus (PGX) --> recommend to only log changes
* Claus Nagel (virtualcitySYSTEMS) --> conceptual advices about logging
* Ugur Yilmaz --> feedback and suggestions
* Maximilian Allies --> For setting up Travis yml script


## 15. Disclaimer

pgMemento IS PROVIDED "AS IS" AND "WITH ALL FAULTS." 
I MAKE NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE 
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR 
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL I BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL, 
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF 
I HAVE BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
STILL, I WOULD FEEL SORRY.
