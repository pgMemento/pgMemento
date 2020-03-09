-- CTL.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script to start auditing for a given database schema
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.1.0     2020-03-09   initial commit                                 FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   init(schema_name TEXT DEFAULT 'public'::text, log_state BOOLEAN DEFAULT TRUE, include_new BOOLEAN DEFAULT FALSE,
*     trigger_create_table BOOLEAN DEFAULT TRUE, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   start(schema_name TEXT DEFAULT 'public'::text, include_new BOOLEAN DEFAULT FALSE,
*     except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   stop(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   version(OUT full_version TEXT, OUT major_version INTEGER, OUT minor_version INTEGER, OUT build_id TEXT) RETURNS RECORD
*
***********************************************************/

CREATE OR REPLACE FUNCTION pgmemento.init(
  schema_name TEXT DEFAULT 'public'::text,
  log_state BOOLEAN DEFAULT TRUE,
  include_new BOOLEAN DEFAULT FALSE,
  trigger_create_table BOOLEAN DEFAULT TRUE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
BEGIN
  -- create event trigger to log schema changes
  PERFORM pgmemento.create_schema_event_trigger($4, $3);

  -- start auditing for tables in given schema'
  PERFORM pgmemento.create_schema_audit(quote_ident($1), $2, $3, $5);

  RETURN format('pgMemento is initialized on %s schema.', schema_name);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.start(
  schema_name TEXT DEFAULT 'public'::text,
  include_new BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
BEGIN
  SELECT pgmemento.create_schema_log_trigger($1, $2, $3);

  RETURN format('pgMemento is started on %s schema.', schema_name);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.stop(
  schema_name TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
BEGIN
  SELECT pgmemento.drop_schema_log_trigger($1, $2);

  RETURN format('pgMemento is stopped on %s schema.', schema_name);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.version(
  OUT full_version TEXT,
  OUT major_version INTEGER,
  OUT minor_version INTEGER,
  OUT build_id TEXT
  ) RETURNS RECORD AS
$$
SELECT 'pgMemento 0.7', 0, 7, '47'::text;
$$
LANGUAGE sql;