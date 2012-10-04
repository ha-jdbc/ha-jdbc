#	HA-JDBC: High-Availability JDBC

##	Overview

HA-JDBC is a JDBC proxy that provides light-weight, transparent, fault tolerant clustering capability to any underlying JDBC driver.

##	Features

*	Supports any database accessible via JDBC.
*	High fault tolerance - a database cluster can lose a node without failing/corrupting any transactions.
*	Live activation/deactivation allows for maintenance/upgrading of a database node without loss of service.
*	Improves performance of concurrent read-access by distributing load across individual nodes.
*	Works with Java 1.6 and 1.7 and fully supports JDBC 4.1.
*	Pluggable strategies for synchronizing a failed database node.
*	Exposes JMX management interface to allow administration of databases and clusters.
*	Ability to add/subtract database nodes to/from a cluster at runtime.
*	Can be configured to auto-activate failed database nodes during scheduled off-peak times.
*	Open source (LGPL).

##	Related Software

Not overly fond of HA-JDBC? Check out these alternative projects:

pgpool-II
:	Replication middleware for PostgreSQL

Tungsten Replicator
:	A high performance data replication engine for MySQL

MySQL Cluster
:	Fault tolerant database architecture using NDB storage engine

H2 Clustering
:	A simple clustering/high-availability mechanism for H2
