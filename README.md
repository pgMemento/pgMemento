# pgMemento

[![Build Status](https://travis-ci.org/pgMemento/pgMemento.svg?branch=master)](https://travis-ci.org/pgMemento/pgMemento)

![alt text](https://github.com/pgMemento/pgMemento/blob/master/material/pgmemento_logo.png "pgMemento Logo")

pgMemento provides an audit trail for your data inside a PostgreSQL
database using triggers and server-side functions written in PL/pgSQL.
It also tracks DDL changes to enable schema versioning and offers
powerful algorithms to restore or repair past revisions.


## Index

1. License
2. System requirements
3. Documentation
4. Media
5. Developers
6. Special thanks
7. Disclaimer


## 1. License

The scripts for pgMemento are open source under GNU Lesser General 
Public License Version 3.0. See the file LICENSE for more details. 


## 2. System requirements

* PostgreSQL 9.5
* PL/pgSQL language


## 3. Documetation

Documentation can be found in the [wiki](https://github.com/pgMemento/pgMemento/wiki/Home) section of this repository.


## 4. Media

I presented pgMemento at [FOSSGIS 2015](https://www.youtube.com/watch?v=EqLkLNyI6Yk) (in german)
and at [FOSSGIS-NA 2016](http://slides.com/fxku/pgmemento_foss4gna16) .
At [FOSS4G 2017](http://slides.com/fxku/foss4g17_dbversion) I gave a more general overview on database versioning techniques.

Slides of the most up-to-date presentation (which are hopefully even easier to follow) can be found
[here](https://www.postgresql.eu/events/pgconfde2018/schedule/session/1963-auditing-mit-jsonb-pro-und-kontra/).
I gave the talk at the german PostgreSQL conference 2018, but the slides are in english.

A demo paper about pgMemento has been accepted at the 15th International
Symposium for Spatial and Temporal Databases (SSTD) 2017 in Arlington, VA.
You can find the publication [here](https://link.springer.com/chapter/10.1007/978-3-319-64367-0_27).


## 5. Developers

Felix Kunde (felix-kunde [at] gmx.de)

I would be very happy if there are other PostgreSQL developers out there
who are interested in pgMemento and willing to help me to improve it.
Together we might create a powerful, easy-to-use versioning approach for
PostgreSQL.


## 6. Special Thanks

* Petra Sauer --> For support and discussions on a pgMemento research paper  
* Hans-Jürgen Schönig --> recommend to use a generic JSON auditing
* Christophe Pettus --> recommend to only log changes
* Claus Nagel --> conceptual advices about logging
* Ugur Yilmaz --> feedback and suggestions
* Maximilian Allies --> For setting up Travis yml script
* Steve --> coming up with the idea of a `session_info` field
* Adam Brusselback --> benchmarking and bugfixing
* Franco Ricci --> bugfixing


## 7. Disclaimer

pgMemento IS PROVIDED "AS IS" AND "WITH ALL FAULTS." 
I MAKE NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE 
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR 
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL I BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL, 
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF 
I HAVE BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
STILL, I WOULD FEEL SORRY.
