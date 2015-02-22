pgMemento
=====

pgMemento is a versioning approach for PostgreSQL using PL/pgSQL functions


0. Index
--------

1. License
2. About
3. System requirements
4. Background & References
5. How To
6. Future Plans
7. Developers
8. Contact
9. Special thanks
10. Disclaimer


1. License
----------

The scripts for pgMemento are open source under GNU Lesser General 
Public License Version 3.0. See the file LICENSE for more details. 


2. About
--------

Memento. Isn't there a movie called like this? About losing memories?
And the plot is presented in reverse order, right? With pgMemento it is similar.
Databases have no memories of what happened in the past unless it is written
down somewhere. From these notes it can figure out, how a past state might have
looked like.

pgMemento is a bunch of PL/pgSQL scripts that enable auditing of a PostgreSQL
database. I take extensive use of JSON/JSONB functions to log my data. Thus
version 9.4 or higher is needed. I also PL/V8 to work with the JSON-logs on
the server side. This extension has to be installed on the server. Downloads
can be found [here](http://pgxn.org/dist/plv8/1.4.3/) [or here](http://www.postgresonline.com/journal/archives/341-PLV8-binaries-for-PostgreSQL-9.4-windows-both-32-bit-and-64-bit.html).

As I'm on schemaless side with JSON one audit table is enough to save 
all changes of all my tables. I do not need to care a lot about the 
structure of my tables.

For me, the main advantage using JSON is the ability to convert table rows
to one column and populate them back to sets of records without losing 
information on the data types. This is very handy when creating a past
table state. I think the same applies to the PostgreSQL extension hstore
so pgMemento could also be realized using hstore except JSONB.

But taking a look into the past of the database is not the main motivation.
In the future everybody will use logical decoding for that, I guess. With pgMemento
the user shall be able to rollback certain transactions happended in the past.
I want to design a logic that checks if a transactions can be reverted in order
that it fits to following transactions, eg. a DELETE is simple, but what about
reverting an UPDATE on data that has been deleted later anyway? 

For further reading please consider that pgMemento has neither been tested nor
benchmarked a lot by myself.


3. System requirements
----------------------

* PostgreSQL 9.4
* PL/V8


4. Background & References
--------------------------

The auditing approach of pgMemento is nothing new. Define triggers to log
changes in your database is a well known practice. There are other tools
out there which can also be used I guess. When I started the development
for pgMemento I wasn't aware of that there are so many solutions out there
(and new ones popping up every once in while). I haven't tested any of 
them *shameonme* and can not tell you exactly how they differ from pgMemento.

I might have directly copied some elements of these tools, therefore I'm
now referencing tools where I looked up details:

* [audit trigger 91plus](http://wiki.postgresql.org/wiki/audit_trigger_91plus) by ringerc
* [tablelog](http://pgfoundry.org/projects/tablelog/) by Andreas Scherbaum
* [Cyan Audit](http://pgxn.org/dist/cyanaudit/) by Moshe Jacobsen
* [wingspan-auditing] (https://github.com/wingspan/wingspan-auditing) by Gary Sieling
* [pgaudit](https://github.com/2ndQuadrant/pgaudit) by 2ndQuadrant


5. How To
-----------------

### 5.1. Add pgMemento to a database

Run the `SETUP.sql` script to create the schema `pgmemento` with tables
and functions. `VERSIONING.sql` is necessary to restore past table
states and `INDEX_SCHEMA.sql` includes funtions to define constraints
in the schema where the tables / schema state has been restored (in order 
they have been restored as tables).


### 5.2. Start pgMemento

The functions can be used to intialize auditing for single tables or a 
complete schema e.g.

`SELECT pgmemento.create_schema_audit('public', ARRAY['not_this_table'], ['not_that_table']);`

This function creates triggers and an additional column named audit_id
for all tables in the 'public' schema except for tables 'not_this_table' 
and 'not_that_table'. Now changes on the audited tables write information
to the tables 'pgmemento.transaction_log', 'pgmemento.table_event_log' 
and 'pgmemento.row_log'.

When setting up a new database I would recommend to start pgMemento after
bulk imports. Otherwise the import will be slower and several different 
timestamps might appear in the transaction_log table.

ATTENTION: It is important to generate a proper baseline on which a
table/database versioning can reflect on. Before you begin or continue
to work with the database and change contents, define the present state
as the initial versioning state by executing the procedure
`pgmemento.log_table_state` (or `pgmemento.log_schema_state`). 
For each row in the audited tables another row will be written to the 
row_log table telling the system that it has been 'inserted' at the 
timestamp the procedure has been executed.


### 5.3. Have a look at the logged information

For example, an UPDATE command on 'table_A' changing the value of some 
rows of 'column_B' to 'new_value' will appear in the log tables like this:

TRANSACTION_LOG

| txid_id  | stmt_date                | user_name  | client address  |
| -------- |:------------------------:|:----------:|:---------------:|
| 1        | 2015-02-22 15:00:00.100  | felix      | ::1/128         |

TABLE_EVENT_LOG

| ID  | transaction_id | op_id | table_operation | schema_name | table_name  | table_relid
| --- |:--------------:|:-----:|:---------------:|:-----------:|:------------------------:|
| 1   | 1              | 2     | UPDATE          | public      | table_A     | 44444444   |


ROW_LOG

| ID  | event_id  | audit_id | changes                  |
| --- |:---------:|:--------:|:------------------------:|
| 1   | 1         | 555      | {"column_B":"old_value"} |
| 2   | 1         | 556      | {"column_B":"old_value"} |
| 3   | 1         | 557      | {"column_B":"old_value"} |

As you can see only the changes are logged. DELETE and TRUNCATE commands
would cause logging of the complete rows while INSERTs would leave a 
blank field for the 'changes' column.


### 5.4. Restore a past state of your database

A table state is restored with the procedure `pgmemento.restore_table_state
(transaction_id, 'name_of_audited_table', 'name_of_audited_schema', 'name_for_target_schema', 'VIEW', 0)`: 
* With a given transaction id the user requests the state of a given table before that transaction.
* The result is written to another schema specified by the user. 
* It can be written to VIEWs (default) or TABLEs. 
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


#### 5.4.1. The next transaction after date x

If the user just wants to restore a past table/database state by using
a timestamp he will need to find out which is the next transaction 
happened to be after date x:

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

Let's say the resulting transaction has the ID 6.


#### 5.4.2. Fetching audit_ids (done internally)
 
I need to know which entries were valid before transaction 6 started.
This can be done by simple JOIN of the log tables querying for audit_ids.
But still, two steps are necessary:
* find out which audit_ids belong to DELETE and TRUNCATE operations 
  (op_id > 2) before transaction 6 => excluded_ids
* find out which audit_ids appear before transaction 6 and not belong
  to the excluded ids of step 1 => valid_ids

WITH 
  excluded_ids AS (
    SELECT DISTINCT r.audit_id
    FROM pgmemento.row_log r
    JOIN pgmemento.table_event_log e ON r.event_id = e.id
    JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
    WHERE t.txid < 6 
      AND e.table_relid = 'public.table_A'::regclass::oid 
	  AND e.op_id > 2
  ), 
  valid_ids AS (
    SELECT DISTINCT y.audit_id
    FROM pgmemento.row_log y
    JOIN pgmemento.table_event_log e ON y.event_id = e.id
    JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
    LEFT OUTER JOIN excluded_ids n ON n.audit_id = y.audit_id
    WHERE t.txid < 6
    AND e.table_relid = 'public.table_A'::regclass::oid 
    AND (
      n.audit_id IS NULL 
      OR 
      y.audit_id != n.audit_id)
  )
SELECT audit_id FROM valid_ids ORDER BY audit_id;


#### 5.4.3. Generate entries from JSONB logs (done internally)

For each fetched audit_id the function 'pgmemento.generate_log_entry' is 
executed. It iterates over all column names of a given table and searches 
for the first appearance in the 'changes' column of the 'pgmemento.row_log' 
table for or after a given transaction (>=). AS JSONB is used GIN indexing
is of benefit here. If no log corresponding to the column name exists in
the row_log table, the recent state of the table is queried.

The column name and the found value together form a JSONB object by using
the 'json_object_agg' function of PostgreSQL. On each iteration these JSONB 
objects are concatenated with the 'pgmemento.concat_json' function (PL/V8) 
to create a complete replica of the table row for the requested date.

SELECT json_object_agg('column_B',
  COALESCE(
    (SELECT (r.changes -> 'column_B') 
       FROM pgmemento.row_log r
       JOIN pgmemento.table_event_log e ON r.event_id = e.id
       JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
       WHERE t.txid >= 6 
         AND r.audit_id = 555
         AND (r.changes ? 'column_B')
         ORDER BY r.id LIMIT 1
    ),
    (SELECT COALESCE(to_json(column_B), NULL)::jsonb 
       FROM public.table_A a 
       WHERE a.audit_id = 555
    )
  )
)::jsonb


#### 5.4.4. Recreate tables from JSONB logs

PostgreSQL offers the functions 'json_populate_recordset' which converts
a setof anyelement (e.g. an array) into records, ergo a table. All created
JSONB-like records from step 5.4.3. are aggregated with the 'json_agg' function
and passed to 'json_populate_recordset' that will return a table that looks
like the table state at date x.


#### 5.4.5. Table templates

If I would use 'table_A' as the template for `json_populate_recordset` 
I would receive the correct order and the correct data types of columns but 
could not exclude columns that did not exist at date x. I need to know the 
structure of 'table_A' at the requested date, too.

Unfortunately pgMemento can yet not perform logging of DDL commands, like
`ALTER TABLE [...]`. It forces the user to do a manual column backup. 
By executing the procedure `pgmemento.create_table_template` an empty copy
of the audited table is created in the pgmemento schema (concatenated with
an internal ID). Every created table is documented (with timestamp) in 
'pgmemento.table_templates' which is queried when restoring former table 
states.

IMPORTANT: A table template has to be created before the audited table
is altered.

### 5.5. Work with the past state

If past states were restored as tables they do not have primary keys 
or indexes assigned to them. References between tables are lost as well. 
If the user wants to work on the restored table or database state - 
like he would do with the recent state - he can use the procedures
`pgmemento.pkey_table_state`, `pgmemento.fkey_table_state` and `pgmemento.index_table_state`. 
These procedures create primary keys, foreign keys and indexes on behalf of 
the recent constraints defined in the certain schema (e.g. 'public'). 
If table and/or database structures have changed fundamentally over time 
it might not be possible to recreate constraints and indexes as their
metadata is not logged by pgMemento. 


6. Future Plans
--------------------------------------

First of all I want to to share my idea with the PostgreSQL community
and discuss the scripts in order to improve them. Let me know what you
think of it.

I would be very happy if there are other PostgreSQL developers out 
there who are interested in pgMemento and willing to help me to improve it.
Together we might create a powerful, easy-to-use versioning approach 
for PostgreSQL.

However, here are some plans I have for the near future:
* Have another table to store metadata of additional created schemas
  for former table / database states.
* Develop a method to revert specific changes e.g. connected to a 
  transaction_id, date, user etc. I've already developped procedures
  to merge a whole schema into another schema, e.g. to be able to do 
  a full rollback on a database (see `REVERT.sql`). But the
  approach is a bit over the top if I just want to revert one 
  transaction.
* Develop an alternative way to the row-based 'generate_log_entry' function
  to have a faster restoring process.

  
6. Developers
-------------

Felix Kunde


7. Contact
----------

fkunde@virtualcitysystems.de


8. Special Thanks
-----------------

* Adam Brusselback --> benchmarking and bugfixing
* Hans-Jürgen Schönig (Cybertech) --> recommend to use a generic JSON auditing
* Christophe Pettus (PGX) --> recommend to only log changes
* Claus Nagel (virtualcitySYSTEMS) --> conceptual advices about logging
* Ollyc (Stackoverflow) --> Query to list all foreign keys of a table
* Denis de Bernardy (Stackoverflow, mesoconcepts) --> Query to list all indexes of a table


9. Disclaimer
--------------

pgMemento IS PROVIDED "AS IS" AND "WITH ALL FAULTS." 
I MAKE NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE 
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR 
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL I BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL, 
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF 
I HAVE BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
STILL, I WOULD FEEL SORRY.