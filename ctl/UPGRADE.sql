-- UPGRADE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script alters the existing tables to follo the new transaction ID
-- logging scheme of v0.6.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                       | Author
-- 0.1.0     2018-07-23   initial commit                                      FKun
--

-- alter existing tables
ALTER TABLE pgmemento.transaction_log
  RENAME COLUMN client_name TO client_id;

ALTER TABLE pgmemento.transaction_log
  ADD COLUMN process_id INTEGER,
  ADD COLUMN client_port INTEGER,
  ADD COLUMN application_name TEXT,
  ADD COLUMN session_info JSONB;

ALTER TABLE pgmemento.table_event_log
  ADD COLUMN transaction_id2 INTEGER,
  ADD COLUMN table_operation2 VARCHAR(18);

UPDATE pgmemento.table_event_log e
  SET transaction_id2 = t.id
  FROM pgmemento.transaction_log t
  WHERE e.transaction_id = t.txid;

UPDATE pgmemento.table_event_log
  SET table_operation2 = table_operation;

ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_txid_fk2
    FOREIGN KEY (transaction_id2)
    REFERENCES pgmemento.transaction_log (id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

CREATE UNIQUE INDEX table_event_log_unique_idx2 ON pgmemento.table_event_log USING BTREE (transaction_id2, table_relid, op_id);

ALTER TABLE pgmemento.table_event_log
  DROP CONSTRAINT table_event_log_txid_fk;

DROP INDEX table_event_log_unique_idx;

ALTER TABLE pgmemento.table_event_log
  DROP COLUMN transaction_id,
  DROP COLUMN table_operation2;

ALTER TABLE pgmemento.table_event_log
  RENAME COLUMN transaction_id2 TO transaction_id;
  
ALTER TABLE pgmemento.table_event_log
  RENAME COLUMN table_operation2 TO table_operation;

ALTER TABLE pgmemento.table_event_log
  RENAME CONSTRAINT table_event_log_txid_fk2 TO table_event_log_txid_fk;

ALTER INDEX table_event_log_unique_idx2 RENAME TO table_event_log_unique_idx;

UPDATE pgmemento.audit_table_log atl
  SET txid_range = numrange(t1.id, t2.id, '[)')
  FROM pgmemento.audit_table_log a
  JOIN pgmemento.transaction_log t1
    ON lower(a.txid_range) = t1.txid
  LEFT JOIN pgmemento.transaction_log t2
    ON upper(a.txid_range) = t2.txid
    WHERE a.id = atl.id;

UPDATE pgmemento.audit_column_log acl
  SET txid_range = numrange(t1.id, t2.id, '[)')
  FROM pgmemento.audit_column_log a
  JOIN pgmemento.transaction_log t1
    ON lower(a.txid_range) = t1.txid
  LEFT JOIN pgmemento.transaction_log t2
    ON upper(a.txid_range) = t2.txid
    WHERE a.id = acl.id;

-- create indexes new in v0.6
CREATE INDEX transaction_log_session_idx ON pgmemento.transaction_log USING GIN (session_info);
