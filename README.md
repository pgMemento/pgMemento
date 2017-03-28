# pgMemento

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/pgmemento_logo.png "pgMemento Logo")

pgMemento is a versioning approach for PostgreSQL using triggers and server-side
functions in PL/pgSQL.


## Index

1. License
2. About
3. System requirements
4. Background & References
5. How To
6. Future Plans
7. Media
8. Developers
9. Contact
10. Special thanks
11. Disclaimer


## 1. License

The scripts for pgMemento are open source under GNU Lesser General 
Public License Version 3.0. See the file LICENSE for more details. 


## 2. About

pgMemento logs DML changes inside a PostgreSQL database. These logs are bound
to events and transactions and not timestamp-based validity intervals. Because 
of that it is also possible to rollback certain changes of the past and keeping
the database consistent.

pgMemento uses triggers to log the changes. Only deltas between OLD and NEW are
stored in order to save disk space. Having only fragments of rows makes things a
bit more complicated but you will see that it can work quiet well using the
JSONB data type. One log table is used to store the changes of all audited tables.

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/generic_logging.png "Generic logging")

pgMemento provides functions to recreate a former table or database state in a 
separate database schema incl. constraints and indexes. As event triggers are 
capturing any schema changes, the restored table or database will have the layout
of the past state.

pgMemento is not designed for write-instensive databases. It will certainly take
less time to run out of disk space. Nevertheless, obsolete content can simply be
removed from the logs at any time without affecting the versioning mechanism.

pgMemento is written in plain PL/pgSQL. Thus, it can be set up on every machine 
with PostgreSQL 9.5 or higher. I tagged a first version of pgMemento (v0.1) that 
uses the JSON data type and can be used along with PostgreSQL 9.3, but it is slower
and can not handle very big data as JSON strings. Releases v0.2 and v0.3 require 
at least PostgreSQL 9.4. The master uses JSONB functions introduced in PostgreSQL 9.5. 
I recommend to always use the newest version of pgMemento.


## 3. System requirements

* PostgreSQL 9.5


## 4. Background & References

The auditing approach of pgMemento is nothing new. Define triggers to log
changes in your database is a well known practice. There are other tools
out there which can also be used. When I started the development for pgMemento 
I wasn't aware of that there are so many solutions out there (and new ones 
popping up every once in while).

If you want a clearer table structure for logged data, say a history table
for each audited table, have a look at [tablelog](http://pgfoundry.org/projects/tablelog/) by Andreas Scherbaum.
It's easy to query different versions of a row. Restoring former states is
also possible. It writes all the data twice, though. Runs only on Linux.

If you prefer to work with validity intervals for each row try out the
[temporal_tables](http://pgxn.org/dist/temporal_tables/) extension by Vlad Arkhipov or the
[table_version](http://pgxn.org/dist/table_version) extension by Jeremy Palmer.
[This talk](http://pgday.ru/files/papers/9/pgday.2015.magnus.hagander.tardis_orm.pdf) by Magnus Hagander goes in a similar direction.

If you like the idea of generic logging, but you prefer hstore over JSONB
check out [audit trigger 91plus](http://wiki.postgresql.org/wiki/audit_trigger_91plus) by Craig Ringer.
It does not provide functions to restore previous database state or to 
rollback certain transactions.

If you want to use a tool, that's proven to run in production for several 
years take a closer look at [Cyan Audit](http://pgxn.org/dist/cyanaudit/) by Moshe Jacobsen.
Logs are structured on columnar level, so auditing can also be switched off
for certain columns. DDL changes on tables are caught by an event trigger.
Rollbacks of transactions are possible for single tables. 

If you think the days for using triggers for auditing are numbered because 
of the new logical decoding feature of PostgreSQL you are probably right.
But this technology is still young and there are not many tools out there
that provide the same functionality like pgMemento. A notable implementation
is [Logicaldecoding](https://github.com/sebastian-r-schmidt/logicaldecoding) by Sebastian R. Schmidt.
[pgaudit](https://github.com/2ndQuadrant/pgaudit) by 2ndQuadrant and its [fork](https://github.com/pgaudit/pgaudit) by David Steele
are only logging transaction metadata at the moment and not the data itself.


## 5. How To

### 5.1. Add pgMemento to a database

A brief introduction about the different SQL files:
* `DDL_LOG.sql` enables logging of schema changes (DDL statements)
* `LOG_UTIL.sql` provides some helpe functions for handling the audited information
* `REVERT.sql` contains procedures to rollback changes of a certain transaction and
* `SCHEMA_MANAGEMENT.sql` includes functions to define constraints in the schema where tables have been restored
* `SETUP.sql` contains DDL scripts for tables and basic setup functions
* `VERSIONING.sql` is necessary to restore past table states

Run the `INSTALL_PGMEMENTO.sql` script with the psql client of PostgreSQL.
Now a new schema will appear in your database called `pgmemento`. As of
version 0.4 the `pgmemento` consist of 5 log tables and 2 view:

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
auditing (comma-separated list). As for the third parameter you can choose
if newly created tables shall be enabled for auditing automatically.
`INIT.sql` also creates event triggers for the database to track schema
changes of audited tables.

Auditing can also be enabled manually for single tables using the following
function, which adds an additional audit_id column to the table and creates
triggers that are fired during DML changes.

<pre>
SELECT pgmemento.create_table_audit(
  'table_A',
  'public'
);
</pre>

If `INIT.sql` has not been used event triggers can be created by calling
the following procedure:

<pre>
SELECT pgmemento.create_schema_event_trigger(1);
</pre>

By passing a 1 to the procedure an additional event trigger for 
`CREATE TABLE` events is created.

**ATTENTION:** It is important to generate a proper baseline on which a
table/database versioning can reflect on. Before you begin or continue
to work with the database and change its content, define the present state
as the initial versioning state by executing the procedure
`pgmemento.log_table_state` (or `pgmemento.log_schema_state`). 
For each row in the audited tables another row will be written to the 
'row_log' table telling the system that it has been 'inserted' at the 
timestamp the procedure has been executed. Depending on the number of 
tables to alter and on the amount of data that has to be defined as 
INSERTed this process can take a while.

**HINT:** When setting up a new database I would recommend to start 
pgMemento after bulk imports. Otherwise the import will be slower and 
several different timestamps might appear in the transaction_log table.

Logging can be stopped and restarted by running the `STOP_AUDITING.sql`
and `START_AUDITING.sql` scripts. Note that theses scripts do not affect
(remove) the audit_id column in the logged tables.


### 5.3. Logging behaviour

#### 5.3.1. DML logging

pgMemento uses two logging stages. The first trigger is fired before each
statement on each audited table. Every transaction is only logged once in
the `transaction_log` table. Within the trigger procedure the corresponding
table operations are logged as well in the `table_event_log` table. Only
one INSERT, UPDATE, DELETE and TRUNCATE event can be logged per table per 
transaction. So, if two operations of the same kind are applied against one
table during one transaction the logged data is mapped to the first event
that has been inserted into `table_event_log`. In the next chapter you will
see why this won't produce any consistency issues.

The second logging stage is related two the data that has changed. Row-level
triggers are fired after each operations on the audited tables. Within the 
trigger procedure the corresponding INSERT, UPDATE, DELETE or TRUNCATE event
for the current transaction is queried and each row if mapped against it.

For example, an UPDATE command on 'table_A' changing the value of some 
rows of 'column_B' to 'new_value' will appear in the log tables like this:

TRANSACTION_LOG

| ID  | txid_id  | stmt_date                | user_name  | client address  |
| --- |:-------- |:------------------------:|:----------:|:---------------:|
| 1   | 1000000  | 2015-02-22 15:00:00.100  | felix      | ::1/128         |

TABLE_EVENT_LOG

| ID  | transaction_id | op_id | table_operation | schema_name | table_name  | table_relid |
| --- |:--------------:|:-----:|:---------------:|:-----------:|:-----------:|:-----------:|
| 1   | 1000000        | 2     | UPDATE          | public      | table_A     | 44444444    |

ROW_LOG

| ID  | event_id  | audit_id | changes                  |
| --- |:---------:|:--------:|:------------------------:|
| 1   | 1         | 555      | {"column_B":"old_value"} |
| 2   | 1         | 556      | {"column_B":"old_value"} |
| 3   | 1         | 557      | {"column_B":"old_value"} |

As you can see only the changes are logged. DELETE and TRUNCATE commands
would cause logging of the complete rows while INSERTs would leave a 
blank field for the 'changes' column. Thus, there is no data redundancy.

The logged information can already be of use, e.g. list all transactions 
that had an effect on a certain column by using the ? operator:

<pre>
SELECT t.txid 
  FROM pgmemento.transaction_log t
  JOIN pgmemento.table_event_log e ON t.txid = e.transaction_id
  JOIN pgmemento.row_log r ON r.event_id = e.id
  WHERE 
    r.audit_id = 4 
  AND 
    (r.changes ? 'column_B');
</pre>

List all rows that once had a certain value by using the @> operator:

<pre>
SELECT DISTINCT audit_id 
  FROM pgmemento.row_log
  WHERE 
    changes @> '{"column_B": "old_value"}'::jsonb;
</pre>


#### 5.3.1. DDL logging

Since v0.3 pgMemento supports DDL logging to capture schema changes.
This is important for restoring former table or database states (see 5.5).
The two tables `audit_table_log` and `audit_column_log` in the pgMemento
schema provide information at what range of transactions the audited 
tables and their columns exist. After a table is altered or dropped an
event trigger is fired to compare the recent state (at ddl_command_end) 
with the logs.

pgMemento also saves data before DROP COLUMN, DROP TABLE or DROP SCHEMA 
events occur (at ddl_command_start). Data is not logged if tables or 
columns are renamed or if the data type of columns is altered. But the
DDL log tables are updated. Renamed tables can be traced by its internal
OID (relid), altered columns by their ordinal position.

**ATTENTION:** So far, changing the data type of columns will not produce
data logs either.


### 5.4. Revert certain transactions

The logged information can be used to revert certain transactions that
happened in the past. Reinsert deleted rows, remove imported data etc.
The procedure is called `revert_transaction`.

The procedure loops over each row that was affected by the given 
transaction. For data integrity reasons the order of operations and 
audit_ids is important. Imagine three tables A, B and C, with B and C
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

For INSERTs and UPDATEs the reverse depth order is used. The same
distinctionis used when resolving self-references on tables. A parent
element must be inserted before the tuples that are referencing it. 
Therefore, it has got a lower audit_id value. When reverting INSERTs 
(younger) tuples with a higher audit_id need to be deleted first. 
When reverting DELETEs (older) tuples with a lower audit_id need to be
reinserted first. The ordering of audit_ids is partitioned by the
diffenrent events.

Reverting also works if foreign keys are set to ON UPDATE CASCADE or
ON DELETE CASCADE because the `audit_tables_dependency` produces the
correct order anyway and cross-referencing tuples in one table would
belong to the same event.

A range of transactions can be reverted by calling:

<pre>
SELECT pgmemento.revert_transactions(lower_txid, upper_txid);
</pre>

It uses nearly the same query but additionally ordered by transaction ids
(DESC). When reverting many transactions an alternative procedure can be
used called `revert_distinct_transaction`. For each distinct audit_it only
the oldest table operation is applied to make the revert process faster.
It is also provided for transaction ranges.

<pre>
SELECT pgmemento.revert_distinct_transactions(lower_txid, upper_txid);
</pre>


### 5.5. Restore a past state of your database

A table state is restored with the procedure `pgmemento.restore_table_state
(start_from_txid, end_at_txid, 'name_of_audited_table', 'name_of_audited_schema', 'name_for_target_schema', 'VIEW', 0)`: 
* With a given range of transaction ids the user specifies the time slot he is interested in.
  If the first value is lower than the first txid in the transaction_log table a complete replica
  of the table - how it was when second given txid had been executed - is created. Note that only
  with this setting you are able to have a correct view on a past state of your database (if the
  procedure is run against every table of the database).
* The result is written to another schema specified by the user. 
* Tables can be restored as VIEWs (default) or TABLEs. 
* If chosen VIEW the procedure can be executed again (e.g. by using another transaction id)
  and replaces the old view(s) if the last parameter is specified as 1.
* A whole database state might be restored with `pgmemento.restore_schema_state`.

How does the restoring work? Well, imagine a time line like this:

1_2_3_4_5_6_7_8_9_10 [Transactions] <br/>
I_U_D_I_U_U_U_I_D_now [Operations] <br/>
I = Insert, U = Update, D = Delete

Let me tell you how a record looked liked at date x of one sample row 
I will use in the following:

TABLE_A

| ID  | column_B  | column_C | audit_id |
| --- |:---------:|:--------:|:--------:|
| 1   | new_value | abc      | 555      |

Imagine that this row is updated again in transactions 6 and 7 and 
deleted at last in transaction 9. In the 'row_log' table this would
be logged as follows:

| ID  | event_id  | audit_id | changes                                                           |
| --- |:---------:|:--------:|:-----------------------------------------------------------------:|
| ... | ...       | ...      | ...                                                               |
| 66  | 15        | 555      | {"column_B":"new_value"}                                          |
| ... | ...       | ...      | ...                                                               |
| 77  | 21        | 555      | {"column_C":"abc"}                                                |
| ... | ...       | ...      | ...                                                               |
| ... | ...       | ...      | ...                                                               |
| 99  | 21        | 555      | {"ID":1,"column_B":"final_value","column_C":"def","audit_id":555} |
| ... | ...       | ...      | ...                                                               |

#### 5.5.1. The next transaction after date x

If the user just wants to restore a past table/database state by using
a timestamp he will need to find out which is the next transaction 
happened to be after date x:

<pre>
WITH get_next_txid AS (
  SELECT txid FROM pgmemento.transaction_log
  WHERE stmt_date >= '2015-02-22 16:00:00' LIMIT 1
)
SELECT pgmemento.restore_schema_state(
  txid,
  'public',
  'test',
  'VIEW',
  ARRAY['not_this_table'], ['not_that_table'],
  1
) FROM get_next_txid;
</pre>

The resulting transaction has the ID 6.


#### 5.5.2. Fetching audit_ids (done internally)
 
I need to know which entries were valid before transaction 6 started.
This can be done by simple JOIN of the log tables querying for audit_ids.
But still, two steps are necessary:
* find out which audit_ids belong to DELETE and TRUNCATE operations 
  (op_id > 2) before transaction 6 => excluded_ids
* find out which audit_ids appear before transaction 6 and not belong
  to the excluded ids of step 1 => valid_ids
* note that `t.txid &gt; 1` reflects the first parameter for procedure `pgmemento.restore_table_state`

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/fetch_auditids_en.png "Fetching Audit_IDs")

<pre>
WITH
  excluded_ids AS (
    SELECT DISTINCT r.audit_id
    FROM pgmemento.row_log r
    JOIN pgmemento.table_event_log e ON r.event_id = e.id
    JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
    WHERE t.txid &gt; 1 AND t.txid &lt; 6
      AND e.table_relid = 'public.table_A'::regclass::oid
	  AND e.op_id &gt; 2
  ),
  valid_ids AS (  
    SELECT DISTINCT y.audit_id
    FROM pgmemento.row_log y
    JOIN pgmemento.table_event_log e ON y.event_id = e.id
    JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
    LEFT OUTER JOIN excluded_ids n ON n.audit_id = y.audit_id
    WHERE t.txid &gt; 1 AND t.txid &lt; 6
      AND e.table_relid = 'public.table_A'::regclass::oid
      AND (
        n.audit_id IS NULL
        OR
        y.audit_id != n.audit_id
      )
  )
SELECT audit_id FROM valid_ids ORDER BY audit_id;
</pre>


#### 5.5.3. Generate entries from JSONB logs (done internally)

For each fetched audit_id a row has to be reconstructed. This is done by
searching the values of each column of the given table. If the key is not 
found in the row_log table, the recent state of the table is queried.

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/fetch_values_en.png "Fetching values")

<pre>
SELECT 
  'column_B' AS key, 
  COALESCE(
    (SELECT (r.changes -> 'column_B') 
       FROM pgmemento.row_log r
       JOIN pgmemento.table_event_log e ON r.event_id = e.id
       JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
       WHERE t.txid >= 6
         AND r.audit_id = f.audit_id
         AND (r.changes ? 'column_B')
         ORDER BY r.id LIMIT 1
    ),
    (SELECT COALESCE(to_json(column_B), NULL)::jsonb
       FROM schema_Z.table_A
       WHERE audit_id = f.audit_id
    )
  ) AS value;
</pre>

By the end I would have a series of keys and values, like for example:
* '{"ID":1}' (--> first entry found in 'row_log' for column 'ID' has ID 99)
* '{"column_B":"new_value"}' (--> first entry found in 'row_log' for column 'ID' has ID 66)
* '{"column_C":"abc"}' (--> first entry found in 'row_log' for column 'ID' has ID 77)
* '{"audit_id":555}' (--> first entry found in 'row_log' for column 'ID' has ID 99)

These fragments can be put in alternating order and passed to the 
`jsonb_build_object` function to generate a complete replica of the 
row as JSONB.

<pre>
<font color='lightgreen'>-- end of WITH block, that collects valid audit_ids</font>
)
SELECT v.log_entry FROM valid_ids f 
JOIN LATERAL ( <font color='lightgreen'>-- for each audit_id do:</font>
  SELECT jsonb_build_object( 
    q1.key, q1.value, 
    q2.key, q2.value,
    ...
    ) AS log_entry 
  FROM ( <font color='lightgreen'>-- query for values</font>
    SELECT q.key, q.value FROM ( 
      SELECT 'id' AS key, r.changes -> 'id' 
      FROM pgmemento.row_log r 
      ...
      )q 
    ) q1,
    SELECT q.key, q.value FROM (
      SELECT 'column_B' AS key, r.changes -> 'column_B'
      FROM pgmemento.row_log r ...
      )q 
    ) q2, 
    ...
) v ON (true)
ORDER BY f.audit_id
</pre>


#### 5.5.4. Recreate tables from JSONB logs (done internally)

The last step on the list would be to bring the generated JSONB objects
into a tabular representation. PostgreSQL offers the function 
`jsonb_populate_record` to do this job.

<pre>
SELECT * FROM jsonb_populate_record(null::table_A, jsonb_object);
</pre>

But it cannot be written just like that because we need to combine it with
a query that returns numerous JSONB objects. There is also the function
`jsonb_populate_recordset` to return a set of records but it needs all
JSONB objects to be aggregated which is a little overhead. The solution
is to use a LATERAL JOIN:

<pre>
<font color='lightgreen'>-- previous steps are executed within a WITH block</font>
)
SELECT p.* FROM restore rq
  JOIN LATERAL (
    SELECT * FROM jsonb_populate_record(
      null::table_A, <font color='lightgreen'>-- template table</font>
      rq.log_entry   <font color='lightgreen'>-- reconstructed row as JSONB</font>
    )
  ) p ON (true)
</pre>

This is also the moment when the DDL log tables are becoming relevant.
In order to produce a correct historic replica of a table, the table 
schema for the requested time (transaction) window has to be known.
Note, that tables might also change their structure during the requested
period. This is not handled at the moment. Only the upper boundary is
used to query the `audit_column_log` table to reconstruct an historic
template. 

The template tables are created as temporary tables. This means when 
restoring the audited tables as VIEWs they only exist as long as the
current sessions lasts (ON COMMIT PRESERVE ROWS). When creating a new
session the restore procedure has to be called again. It doesn't matter
if the target schema already exist. When restoring the audited tables
as BASE TABLEs, they will of course remain in the target schema but
requiring extra disk space.


#### 5.5.5. Restore revisions of a certain tuple

It is also possible to restore only revisions of a certain tuple with the
function `pgmemento.get_log_entry`. It requires a txid ID, the audit_id of
the tuple and the corresponding table and schema name.

<pre>
WITH get_log_entries AS (
  SELECT pgmemento.generate_log_entry(e.transaction_id,r.audit_id,'my_table','public') AS entry
  FROM pgmemento.row_log r
  JOIN pgmemento.table_event_log e ON e.id = r.event_id
    WHERE r.audit_id = 12345
      ORDER BY e.transaction_id DESC, e.id DESC
)
SELECT j.* FROM get_log_entries i
JOIN LATERAL ( 
  SELECT * FROM jsonb_populate_record(null::public.my_table, i.entry)
) j ON (true); 
</pre>


#### 5.5.6. Work with the past state

If past states were restored as tables they do not have primary keys 
or indexes assigned to them. References between tables are lost as well. 
If the user wants to work on the restored table or database state - 
like he would do with the production state - he can use the procedures
`pgmemento.pkey_table_state`, `pgmemento.fkey_table_state` and `pgmemento.index_table_state`. 
These procedures create primary keys, foreign keys and indexes on behalf of 
the recent constraints defined in the production schema. 

Note that if table and/or database structures have changed fundamentally 
over time it might not be possible to recreate constraints and indexes as 
their metadata is not yet logged by pgMemento. 


### 5.6. Uninstall pgMemento

In order to stop and remove pgMemento simply run the `UNINSTALL_PGMEMENTO.sql`
script.


## 6. Future Plans

First of all I want to to share my idea with the PostgreSQL community
and discuss the scripts in order to improve them. Let me know what you
think of it.

I would be very happy if there are other PostgreSQL developers out 
there who are interested in pgMemento and willing to help me to improve it.
Together we might create a powerful, easy-to-use versioning approach 
for PostgreSQL.

However, here are some plans I have for the near future:
* Do more benchmarking
* Table partitioning strategy for row_log table (maybe [pg_pathman](https://github.com/postgrespro/pg_pathman) can help)
* Have log tables for primary keys, constraints, indexes etc.
* Have a view to store metadata of additional created schemas
  for former table / database states.
* Better protection for log tables?
* Build a pgMemento PostgreSQL extension


## 7. Media

I gave a presentation in german at FOSSGIS 2015:
https://www.youtube.com/watch?v=EqLkLNyI6Yk

I gave another presentation in FOSSGIS-NA 2016:
http://slides.com/fxku/pgmemento_foss4gna16


## 8. Developers

Felix Kunde


## 9. Contact

felix-kunde@gmx.de


## 10. Special Thanks

* Adam Brusselback --> benchmarking and bugfixing
* Hans-Jürgen Schönig (Cybertech) --> recommend to use a generic JSON auditing
* Christophe Pettus (PGX) --> recommend to only log changes
* Claus Nagel (virtualcitySYSTEMS) --> conceptual advices about logging
* Ollyc (Stackoverflow) --> Query to list all foreign keys of a table
* Denis de Bernardy (Stackoverflow, mesoconcepts) --> Query to list all indexes of a table
* Ugur Yilmaz --> feedback and suggestions


## 11. Disclaimer

pgMemento IS PROVIDED "AS IS" AND "WITH ALL FAULTS." 
I MAKE NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE 
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR 
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL I BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL, 
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF 
I HAVE BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
STILL, I WOULD FEEL SORRY.
