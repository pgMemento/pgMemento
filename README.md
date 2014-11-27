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
database. I take extensive use of JSON functions to log my data. Thus
version 9.3 or higher is needed.

As I'm on schemaless side with JSON one audit table is enough to save 
all changes of all my tables. I do not need to care a lot about the 
structure of my tables.

For me, the main advantage using JSON is the ability to convert table rows
to one column and populate them back to sets of records without losing 
information on the data types. This is very handy when creating a past
table state. I think the same applies to the PostgreSQL extension hstore
so pgMemento could also be realized using hstore except JSON.

For further reading please consider that pgMemento has neither been tested nor
benchmarked a lot by myself.


3. System requirements
----------------------

* PostgreSQL 9.3 


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

* [pgMemento trigger 91plus](http://wiki.postgresql.org/wiki/pgMemento_trigger_91plus) by ringerc
* [tablelog](http://pgfoundry.org/projects/tablelog/) by Andreas Scherbaum
* [Cyan pgMemento](http://pgxn.org/dist/cyanaudit/) by Moshe Jacobsen


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
to the tables 'pgmemento.transaction_log' and 'pgmemento.audit_log'.

When setting up a new database I would recommend to start pgMemento after
bulk imports. Otherwise the import will be slower and several different 
timestamps might appear in the audit_log table. The more timestamps are 
recorded the more queries are necessary to restore a table state.

ATTENTION: It is important to generate a proper baseline on which a
table/database versioning can reflect on. Before you beginn or continue
to work with the database and change contents, define the present state
as the initial versioning state by executing the procedure
`pgmemento.log_table_state` (or `pgmemento.log_schema_state`). For each row in the
audited tables another row will be written to the audit_log table
telling the system that it has been 'inserted' at the timestamp the
procedure has been executed.


### 5.3. Have a look at the logged information

For example, if you have run an UPDATE command on 'table_A' changing the
value of some rows of 'column_B' to 'new_value' the following entries will 
appear in the log tables:

TRANSACTION_LOG

| ID  | tx_id    | operation | schema | table_name | relid    | timestamp               | user  | client address | applicatopn |
| --- |:--------:|:---------:|:------:|:----------:|:--------:|:-----------------------:|:-----:|:--------------:|:-----------:|
| 100 | 11111111 | UPDATE    | public | table_A    | 22222222 | 2014-05-22 15:00:00.100 | felix | 192.168.0.0/32 | pgAdmin III | 

AUDIT_LOG

| ID  | tx_id    | relid    | timestamp               | audit_id | table_content            |
| --- |:--------:|:--------:|:-----------------------:|:--------:|:------------------------:|
| 500 | 11111111 | 22222222 | 2014-05-22 15:00:00.100 | 70       | {"column_B":"old_value"} |
| 501 | 11111111 | 22222222 | 2014-05-22 15:00:00.100 | 71       | {"column_B":"old_value"} |
| 502 | 11111111 | 22222222 | 2014-05-22 15:00:00.100 | 72       | {"column_B":"old_value"} |

As you can see only the changes are logged. DELETE and TRUNCATE commands
would cause logging of the complete rows while INSERTs would leave a 
blank field for the 'table_content' column.


### 5.4. Restore a past state of your database

A table state is restored with the procedure `pgmemento.restore_table_state
('timestamp_x', 'name_of_audited_table', 'public', 'name_for_target_schema', 'VIEW', 0)`. 
A whole database state might be restored with `pgmemento.restore_schema_state`.
The result is written to another schema specified by the user. It 
can be written to VIEWs (default) or TABLEs. If chosen VIEW you are able
to execute the procedure again to update the target schema by passing a
new timestamp. The old view(s) will be replaced. Simply specifiy the last
parameter of the fuction (integer) as 1.

How does the restoring work? Well, imagine a time line like this:

1______2___3__4___5______6_7_8__9___10 [Timestamps] <br/>
I______U___D__I___U_x____U_U_I__D__now [Operations] <br/>
I = Insert, U = Update, D = Delete, x = the date I'm interested in

Let me tell you how a record of one example row I'll use in the following
looked liked at date x:

TABLE_A

| ID  | column_B  | column_C | audit_id |
| --- |:---------:|:--------:|:--------:|
| 1   | new_value | abc      | 70       |


#### 5.4.1. Fetching audit_ids
 
I need to know which entries were valid at the date I've requested (x).
I look up the transaction_log table to see if I could find an entry for
date x and table 'table_A'. No? Ok, I've rounded up the timestamp, 
that's why. So dear transaction_log table, tell me what is the next 
older timestamp you got for 'table_A'. 

Timestamp 5. An UPDATE statement happened there. Get me all the rows 
affected by this transaction from the audit_log table (where table_name
is 'table_A'), but I'm only interested in their audit_ids.

Timestamp 4. Ah yes, an INSERT command. Again get me all the audit_ids 
but leave out the ones I already have or to say it in a different way:
Leave out the ones that appear after this timestamp (>) until my queried
date (<=).

Timestamp 3. An DELETE statement? I'm not interested in anything what 
happened there. The rows were deleted an do not appear at my queried 
date. The same would apply for TRUNCATE events.

Timestamp 2. An UPDATE again. Do the same like at timestamp 4.

Timestamp 1. OK, INSERT - Again: Get the IDs, leave out IDs that appear 
during the last timestamps.


#### 5.4.2. Generate entries from JSON logs

Ok, now that I know, which entries were valid at date x let's perform 
the PostgreSQL function `json_populate_recordset` on the column 
'table_content' in the 'audit_log' table using the fetched audit_ids to 
transform JSON back to tables. But wait, in some rows I only got this 
fragment `{"column_B":"old_value"}`. Well of course, it's just the 
change not the complete row. What to do?

We have to check the timestamps after date x in ascending order and 
collect each JSON diff to build up complete records of a past state. 
If some values have changed multiple times corresponding key-value 
pairs in newer JSON diffs are ignored as we are only interested in the
oldest version. 

So at timestamp 6 a first JSON diff is just fetched, e.g. 
`{"column_B":"new_value"}`. 

At timestamp 7 another update happened and a JSON diff is fetched 
again, e.g. `{"column_B":"newer_value";"column_C":"abc"}`, which has 
to be merged into the JSON log we've already got. The result would be
`{"column_B":"new_value;"column_C":"abc"}`.

Timestamp 8 does not appear in our timestamp query because it refers to 
an INSERT operation which just adds new entries with their own audit_ids.

As for timestamp 9 some rows were deleted which logs the whole content
of the affected tuples (as we remember from 5.3.). Finally, the 
audited table itself is also queried against the audit_id because for 
rows that have not been deleted the last JSON diff has to be generated 
from the recent version of the table entry.

The final result of our long journey would be the following JSON object:
`{"column_B":"newer_value";"column_C":"abc";"id":1";"audit_id":70;"new_attribute":"extra"}`

As you can see the ordering of columns is a little disarranged and 
a new column found its way to our JSON log. Both problems do not matter
when performing `json_populate_recordset` as it uses a table template 
that defines the table structure. This leads to a another important 
aspect of pgMemento.


#### 5.4.3. Table templates

If I would use 'table_A' as the template for `json_populate_recordset` 
I would receive the correct order of columns but could not exclude new
columns. I need to know the structure of 'table_A' at date x, too.

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