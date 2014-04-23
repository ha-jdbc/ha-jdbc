#	Documentation

##	Introduction

HA-JDBC is a JDBC proxy that enables a Java application to transparently access a cluster of identical databases through the JDBC API.

![Database cluster access via HA-JDBC](images/ha-jdbc.png)

HA-JDBC has the following advantages over normal JDBC:

High-Availability
:	The database cluster is available to service requests so long as at least one database node is active.

Fault Tolerance
:	Because HA-JDBC operates via the JDBC API, it is transaction-aware and can survive a database node failure without failing or corrupting current transactions.

Scalability
:	By balancing read requests across databases, HA-JDBC can meet increasing load by scaling horizontally (i.e. adding database nodes).


##	Runtime requirements

*	Java 1.6+
*	Type IV JDBC 4.x driver for underlying databases
*	Optional dependency JARs can be downloaded from the [Dependencies](dependencies.html) page.
*	Configuration XML file per database cluster or bootstrapped programmatic configuration

##	Configuration

HA-JDBC is typically configured via XML file.
The full schema definitions for past and present versions of HA-JDBC are enumerated on the [XML Schemas](schemas.html) page.


###	<a name="xml"/>XML

The algorithm used to locate the configuration file resource at runtime is as follows:

1.	Determine the potentially parameterized resource name from one of the following sources:
	1.	A `config` property passed to `DriverManager.getConnection(String, Properties)`, or the `config` property of the `DataSource`, `ConnectionPoolDataSource`, or `XADataSource`.
	1.	The ha-jdbc.*cluster-id*.configuration system property.
	1.	Use default value: `ha-jdbc-{0}.xml`
1.	Format the parameterized resource name using the identifier of the cluster.
1.	Convert the formatted resource name to a URL. If the resource is not a URL, search for the resource in the classpath using the following class loaders:
	1.	Thread context class loader
	1.	The classloader used to load HA-JDBC
	1.	System class loader


###	<a name="database"/>Defining databases

The general syntax for defining the databases composing an HA-JDBC cluster is as follows:

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster>
			<database id="..." location="..." weight="#">
				<user>...</user>
				<password>...</password>
				<property name="...">...</property>
				<!-- Additional properties -->
			</database>
		</cluster>
	</ha-jdbc>

id
:	Uniquely identifies this database within the cluster.

location
:	In general, this describes the location of the database.
	For Driver-based clusters, this specifies the JDBC url of the database.
	For DataSource-based cluster, this specifies either:
	*	The class name of the DataSource implementation (from which a new instance will be created).
	*	The JNDI name of the pre-bound DataSource.

weight
:	Defines the relative weight of this database node.
	If undefined, weight is assumed to be 1.
	See [Balancer](#balancer) section for details.

user
:	The user name used by HA-JDBC to connect to the database for synchronization and meta-data caching purposes.
	This user should have administrative privileges.  This differs from the database user used by your application (which likely has CRUD or read-only permissions).

password
:	The password for the above database user.

property
:	Defines a set of properties, the semantics of which depend on the cluster access pattern.
	For Driver-based clusters, these properties are passed to the corresponding call to `Driver.connect(String, Properties)` method.
	For DataSource-based clusters, if the database name specified a:
	*	class name, then these properties are interpreted as JavaBean properties used to initialize the DataSource instance.
		e.g.
		
			<database id="db1" location="org.postgresql.ds.PGSimpleDataSource">
				<property name="serverName">server1</property>
				<property name="portNumber">5432</property>
				<property name="databaseName">database</property>
			</database>
	*	JNDI name, then these properties are used as JNDI environment properties when constructing the initial context.
		e.g.
		
			<database id="db1" location="java:comp/env/jdbc/db1">
				<property name="java.naming.provider.url">...</property>
			</database>

###	<a name="dialect"/>Dialect

The dialect attribute of a cluster is used to adapt HA-JDBC to a specific database vendor.
HA-JDBC includes dialects for the following databases:
<table>
	<tr>
		<th>Dialect</th>
		<th>Database vendor</th>
		<th>Sequences</th>
		<th>Identity columns</th>
		<th>Dump/restore</th>
	</tr>
	<tr>
		<td>**db2**</td>
		<td>IBM DB2</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
		<td></td>
	</tr>
	<tr>
		<td>**derby**</td>
		<td>Apache Derby</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
		<td></td>
	</tr>
	<tr>
		<td>**firebird**</td>
		<td>Firebird, InterBase</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**h2**</td>
		<td>H2 Database</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**hsqldb**</td>
		<td>HSQLDB</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**ingres**</td>
		<td>Ingres</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**maxdb**</td>
		<td>MySQL MaxDB, SAP DB</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**mckoi**</td>
		<td>Mckoi SQL Database</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**mysql**</td>
		<td>MySQL</td>
		<td></td>
		<td></td>
		<td>&#x2714;</td>
	</tr>
	<tr>
		<td>**oracle**</td>
		<td>Oracle Database</td>
		<td>&#x2714;</td>
		<td></td>
		<td></td>
	</tr>
	<tr>
		<td>**postgresql**</td>
		<td>PostgreSQL</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
		<td>&#x2714;</td>
	</tr>
	<tr>
		<td>**sybase**</td>
		<td>Sybase</td>
		<td></td>
		<td>&#x2714;</td>
		<td></td>
	</tr>
	<tr>
		<td>***standard***</td>
		<td>SQL-92 compliant database</td>
		<td></td>
		<td></td>
		<td></td>
	</tr>
</table>

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster dialect="postgresql">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="balancer"/>Balancer

When executing a read request from the cluster, HA-JDBC uses the configured balancer strategy to determine which database should service the request.
Each database can define a weight to affect how it is prioritized by the balancer.
If no weight is specified for a given database, it is assumed to be 1.

N.B. In general, a node with a weight of 0 will never service a request unless it is the last node in the cluster.

By default, HA-JDBC supports 4 types of balancers:

simple
:	Requests are always sent to the node with the highest weight.

random
:	Requests are sent to a random node.
	Node weights affect the probability that a given node will be chosen.
	The probability that a node will be chosen = *weight* / *total-weight*.

round-robin
:	Requests are sent to each node in succession. A node of weight *n* will receive *n* requests before the balancer moves on to the next node.

*load*
:	Requests are sent to the node with the smallest load.
	Node weights affect the calculated load of a given node.
	The load of a node = *concurrent-requests* / *weight*.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster balancer="simple">
			<!-- Read requests will always prefer db1 -->
			<database id="db1" location="..." weight="2"><!-- ... --></database>
			<database id="db2" location="..." weight="1"><!-- ... --></database>
		</cluster>
	</ha-jdbc>


###	<a name="sync"/>Synchronization strategies

Defines a strategy for synchronizing a database before activation.
A cluster may define multiple synchronization strategies, however, one of them must be designated as the `default-sync`.
The default synchronization strategy is used when synchronization is triggered by auto-activation.
The others are only used during manual database activation.

HA-JDBC supports the following strategies by default.
As of version 3.0, synchronization strategies are defined by identifier alone, not by class name.
If the strategy exposes any JavaBean properties, these can be overridden via nested `property` elements.

passive
:	Does nothing.
	Use this strategy for read-only clusters, or if you know for a fact that the target database is already in sync.

dump-restore
:	Performs a native dump/restore from the source to the target database.
	To use this strategy, the dialect in use must support it (see [Dialect.getDumpRestoreSupport()](apidocs/net/sf/hajdbc/dialect/Dialect.html)).
	Unlike the other sync strategies, this strategy can synchronize both the schema and data.

full
:	Truncates each table in the target database and inserts data from the source database.
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**fetchSize**</td>
			<td>0</td>
			<td>Controls the maximum number of rows to fetch from the source database at a time.</td>
		</tr>
		<tr>
			<td>**maxBatchSize**</td>
			<td>100</td>
			<td>Controls the maximum number of insert/update/delete statements to execute within a batch.</td>
		</tr>
	</table>
	
diff
:	Performs a full table scan of source table vs target table, and performs necessary insert/update/delete.
	Supports the following properties:
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**versionPattern**</td>
			<td></td>
			<td>
				Specifies a regular expression matching the column name of a last update timestamp (i.e. version) column.
				If specified, a version comparison column can be used to determine whether a given row requires updating, instead of a full column scan.
			</td>
		</tr>
		<tr>
			<td>**fetchSize**</td>
			<td>0</td>
			<td>Controls the maximum number of rows to fetch from the source database at a time.</td>
		</tr>
		<tr>
			<td>**maxBatchSize**</td>
			<td>100</td>
			<td>Controls the maximum number of insert/update/delete statements to execute within a batch.</td>
		</tr>
	</table>

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<sync id="full">
			<property name="fetchSize">1000</property>
		</sync>
		<sync id="diff">
			<property name="fetchSize">1000</property>
			<property name="versionPattern">version</property>
		</sync>
		<cluster default-sync="diff"><!-- ... --></cluster>
	</ha-jdbc>


###	<a name="state"/>Cluster state management

The state manager component is responsible for storing the active status of each database in the cluster, as well as any durability state.
During startup, HA-JDBC fetches its initial cluster state is fetched either from another server, if HA-JDBC is configured to be **distributable**, or if the configured state manager is persistent.
If no state is found, all accessible databases are presumed to be active.
To ignore (i.e. clear) the locally persisted cluster state at startup, start HA-JDBC using the *ha-jdbc.state.clear=true* system property.

HA-JDBC includes the following state manager implementations:

simple
:	A non-persistent state manager that stores cluster state in memory.

	e.g.

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="simple"/>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

*sql*
:	A persistent state manager that uses an embedded database.
	This provider supports the following properties, in addition to properties to manipulate connection pooling behavior.
	The complete list of pooling properties and their default values are available in the [Apache Commons Pool documentation][commons-pool] documentation.
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**urlPattern**</td>
			<td>
				<div class="nobr">jdbc:h2:{1}/{0}</div><br/>
				<div class="nobr">jdbc:hsqldb:{1}/{0}</div><br/>
				<div class="nobr">jdbc:derby:{1}/{0};create=true</div>
			</td>
			<td>
				A MessageFormat pattern indicating the JDBC url of the embedded database.
				The pattern can accept 2 parameters:
				<ol>
					<li>The cluster identifier</li>
					<li>`$HOME/.ha-jdbc`</li>
				</ol>
			</td>
		</tr>
		<tr>
			<td>**user**</td>
			<td></td>
			<td>Authentication user name for the embedded database.</td>
		</tr>
		<tr>
			<td>**password**</td>
			<td></td>
			<td>Authentication password for the above user.</td>
		</tr>
	</table>
	e.g.

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="sql">
				<property name="urlPattern">jdbc:h2:/temp/ha-jdbc/{0}</property>
			</state>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

berkeleydb
:	A persistent state manager that uses a BerkeleyDB database.
	This provider supports the following properties, in addition to properties to manipulate connection pooling behavior.
	The complete list of pooling properties and their default values are available in the [Apache Commons Pool documentation][commons-pool] documentation..
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**locationPattern**</td>
			<td>
				{1}/{0}
			</td>
			<td>
				A MessageFormat pattern indicating the base location of the embedded database.
				The pattern can accept 2 parameters:
				<ol>
					<li>The cluster identifier</li>
					<li>`$HOME/.ha-jdbc`</li>
				</ol>
			</td>
		</tr>
	</table>
	e.g.

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="berkeleydb">
				<property name="locationPattern">/tmp/{0}</property>
			</state>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

sqlite
:	A persistent state manager that uses a SQLite database.
	This provider supports the following properties, in addition to properties to manipulate connection pooling behavior.
	The complete list of pooling properties and their default values are available in the [Apache Commons Pool documentation][commons-pool] documentation.
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**locationPattern**</td>
			<td>
				{1}/{0}
			</td>
			<td>
				A MessageFormat pattern indicating the base location of the embedded database.
				The pattern can accept 2 parameters:
				<ol>
					<li>The cluster identifier</li>
					<li>`$HOME/.ha-jdbc`</li>
				</ol>
			</td>
		</tr>
	</table>
	e.g.

		<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
			<state id="sqlite">
				<property name="locationPattern">/tmp/{0}</property>
			</state>
			<cluster><!-- ... --></cluster>
		</ha-jdbc>

###	<a name="durability"/>Durability

As of version 3.0, HA-JDBC support a configurable durability level for user transactions.
When enabled, HA-JDBC will track transactions, such that, upon restart, following a crash, it can detect and recover from any partial commits (i.e. where a given transaction completed on some but not all databases in the cluster).
The durability persistence mechanism is determined by the state manager configuration.
By default, HA-JDBC includes support for the following durability levels:

none
:	No invocations are tracked.
	This durability level can neither detect nor recover from mid-commit crashes.
	This level offers the best performance, but offers no protection from crashes.
	Read-only database clusters should use this level.

*coarse*
:	Tracks cluster invocations only, but not per-database invokers.
	This durability level can detect, but not recover from, mid-commit crashes.
	Upon recovery, if any cluster invocations still exist in the log, all slave database will be deactivated and must be reactivated manually.
	This level offers a compromise between performance and resiliency.

fine
:	Tracks cluster invocations as well as per-database invokers.
	This durability level can both detect and recover from mid-commit crashes.
	Upon recovery, if any cluster invocations still exist in the log, only those slave database on which a given transaction did not complete will be deactivated.
	While this level is the slowest, it ensures the highest level of resiliency from crashes.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster durability="fine">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="distributed"/>Distributed capabilities

Indicates that database clusters defined in this file will be accessed by multiple JVMs.
By default, HA-JDBC supports the following providers:

*jgroups*
:	Uses a JGroups channel to broadcast cluster membership changes to other peer nodes.
	The JGroups provider recognizes the following properties:
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**stack**</td>
			<td>`udp-sync.xml`</td>
			<td>
				Defines one of the following:
				<ul>
					<li>Name of a system resource containing the JGroups XML configuration.</li>
					<li>URL of the JGroups XML configuration file.</li>
					<li>Path of the JGroups XML configuration on the local file system.</li>
					<li>Legacy protocol stack property string.</li>
				</ul>
				See the [JGroups wiki][jgroups] for assistance with customizing the protocol stack.
			</td>
		</tr>
		<tr>
			<td>**timeout**</td>
			<td>60000</td>
			<td>Indicates the number of milliseconds allowed for JGroups operations.</td>
		</tr>
	</table>

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<distributable id="jgroups">
			<property name="stack">udp.xml</property>
		</distributable>
		<cluster><!-- ... --></cluster>
	</ha-jdbc>


###	<a name="meta-data"/>Database meta-data caching

HA-JDBC makes extensive use of database meta data.
For performance purposes, this information should be cached whenever possible.
By default, HA-JDBC includes the following meta-data-cache options:

none
:	Meta data is loaded when requested and not cached.

lazy
:	Meta data is loaded and cached per database as it is requested.

shared-lazy
:	Meta data is loaded and cached as it is requested.

*eager*
:	All necessary meta data is loaded and cached per database during HA-JDBC initialization.

shared-eager
:	All necessary meta data is loaded and cached during HA-JDBC initialization.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster meta-data-cache="shared-eager">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="password"/>Password Obfuscation

Since HA-JDBC's configuration file contains references to database passwords, some users may want to obfuscate these.
To indicate that a password uses an obfuscation mechanism, use a ":" to indicate the appropriate decoder.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster ...>
			<database id="db1" location="jdbc:mysql://server:port/db1">
				<user>admin</user>
				<password>?:wzAkF0hlYUeGhfzRQIxYAQ==</password>
			</database>
			<database id="db2"><!-- ... --></database>
		</cluster>
	</ha-jdbc>

The following decoding mechanism are currently supported:

16
:	Decodes passwords using hexidecimal decoding.

64
:	Decodes passwords using Base64 decoding.

?
:	Decodes passwords using a symmetric encryption key from a keystore.
	The following system properties can be used to customize the properties of the key and/or keystore:
	<table>
		<tr>
			<th>System property</th>
			<th>Default value</th>
		</tr>
		<tr>
			<td>ha-jdbc.keystore.file</td>
			<td>`$HOME/.keystore`</td>
		</tr>
		<tr>
			<td>ha-jdbc.keystore.type</td>
			<td>jceks</td>
		</tr>
		<tr>
			<td>ha-jdbc.keystore.password</td>
			<td><em>none</em></td>
		</tr>
		<tr>
			<td>ha-jdbc.key.alias</td>
			<td>ha-jdbc</td>
		</tr>
		<tr>
			<td>ha-jdbc.key.password</td>
			<td><em>required</em></td>
		</tr>
	</table>
	Use the following command to generate encrypted passwords for use in your config file:

		java -classpath ha-jdbc.jar net.sf.hajdbc.codec.crypto.CipherCodecFactory [password]


###	<a name="unique-ids"/>Unique Identifiers

Most applications that write information to a database require some kind of primary key generation mechanism.
Databases typically provide 2 mechanisms for doing this, both of which are supported by HA-JDBC (if the configured <a href="#dialect">dialect</a> supports it): database sequences and identity (i.e. auto-incrementing) columns.

It is important to note the performance implications when using sequences and/or identity columns in conjunction with HA-JDBC.
Both algorithms introduce per statement regular expression matching and mutex costs in HA-JDBC, the latter being particularly costly for distributed environments.
Because of their performance impact, support for both sequences and identity columns can be disabled via the **detect-sequences** and **detect-identity-columns** cluster attributes, respectively.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster detect-sequences="false" detect-identity-columns="false">
			<!-- ... -->
		</cluster>
	</ha-jdbc>

Fortunately, the performance penalty for sequences can be mitigated via what Hibernate calls a Sequence-HiLo algorithm.

For best performance, HA-JDBC recommends using a table-based high-low or UUID algorithm so that statement parsing and locking costs can be avoided.
Object-relation mapping (ORM) frameworks (e.g. Hibernate, OpenJPA, etc.) typically include implementations of these mechanisms.


###	Customizing HA-JDBC

HA-JDBC supports a number of extension points.
Most components support custom implementations, including:

*	[net.sf.hajdbc.balancer.BalancerFactory](apidocs/net/sf/hajdbc/balancer/BalancerFactory.html)
*	[net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory](apidocs/net/sf/hajdbc/cache/DatabaseMetaDataCacheFactory.html)
*	[net.sf.hajdbc.codec.CodecFactory](apidocs/net/sf/hajdbc/codec/CodecFactory.html)
*	[net.sf.hajdbc.dialect.DialectFactory](apidocs/net/sf/hajdbc/dialect/DialectFactory.html)
*	[net.sf.hajdbc.distributed.CommandDispatcherFactory](apidocs/net/sf/hajdbc/distributed/CommandDispatcherFactory.html)
*	[net.sf.hajdbc.durability.DurabilityFactory](apidocs/net/sf/hajdbc/durability/DurabilityFactory.html)
*	[net.sf.hajdbc.lock.LockManagerFactory](apidocs/net/sf/hajdbc/lock/LockManagerFactory.html)
*	[net.sf.hajdbc.state.StateManagerFactory](apidocs/net/sf/hajdbc/state/StateManagerFactory.html)
*	[net.sf.hajdbc.SynchronizationStrategy](apidocs/net/sf/hajdbc/SynchronizationStrategy.html)

In general, to configure HA-JDBC with a custom component:

1.	Create an implementation of the custom component.
	All extension points implement a getId() method which indicates the identifier by which this instance will be referenced in the configuration file.
1.	Create a `META-INF/services/interface-name` file containing the fully qualified class name of the custom implementation.
1.	Reference your custom component by identifier in your ha-jdbc xml file.

e.g.

	package org.myorg;
	
	public class CustomDialectFactory implements net.sf.hajdbc.dialect.DialectFactory {
		@Override
		public String getId() {
			return "custom";
		}
	
		@Override
		public net.sf.hajdbc.dialect.Dialect createDialect() {
			return new StandardDialect() {
				// Override methods to customize behavior
			};
		}
	}

`META-INF/services/net.sf.hajdbc.dialect.DialectFactory`

	org.myorg.CustomDialectFactory

`ha-jdbc.xml`

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster dialect="custom" ...>
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	Programmatic configuration

As of version 3.0, an HA-JDBC cluster can be configured programmatically.

e.g.

	// Define each database in the cluster
	DriverDatabase db1 = new DriverDatabase();
	db1.setId("db1");
	db1.setLocation("jdbc:hsqldb:mem:db1");
	db1.setUser("sa");
	db1.setPassword("");
	
	DriverDatabase db2 = new DriverDatabase();
	db2.setId("db2");
	db2.setLocation("jdbc:hsqldb:mem:db2");
	db2.setUser("sa");
	db2.setPassword("");
	
	// Define the cluster configuration itself
	DriverDatabaseClusterConfiguration config = new DriverDatabaseClusterConfiguration();
	// Specify the database composing this cluster
	config.setDatabases(Arrays.asList(db1, db2));
	// Define the dialect
	config.setDialectFactory(new HSQLDBDialectFactory());
	// Don't cache any meta data
	config.setDatabaseMetaDataCacheFactory(new SimpleDatabaseMetaDataCacheFactory());
	// Use an in-memory state manager
	config.setStateManagerFactory(new SimpleStateManagerFactory());
	// Make the cluster distributable
	config.setDispatcherFactory(new JGroupsCommandDispatcherFactory());
	
	// Register the configuration with the HA-JDBC driver
	net.sf.hajdbc.sql.Driver.setConfigurationFactory("mycluster", new SimpleDatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>(config));
	
	// Database cluster is now ready to be used!
	Connection connection = DriverManager.getConnection("jdbc:ha-jdbc:mycluster", "sa", "");


## Using HA-JDBC

Application access an HA-JDBC cluster either via the Driver or DataSource.


###	Driver-based access

To access a specific cluster via the Driver, HA-JDBC must be configured to access your databases accordingly.

e.g.

`ha-jdbc-mycluster.xml`

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster>
			<database id="db1" location="jdbc:postgresql://server1:port1/database1">
				<user>pgadmin</user>
				<password>password</password>
			</database>
			<database id="db2" location="jdbc:postgresql://server2:port2/database2">
				<user>pgadmin</user>
				<password>password</password>
			</database>
		</cluster>
	</ha-jdbc>

HA-JDBC connection can then be established via the appropriate JDBC URL:

	java.sql.Connection connection = java.sql.DriverManager.getConnection("jdbc:ha-jdbc:mycluster", "user", "password");


###	DataSource-based access

To access a specific cluster via a DataSource, HA-JDBC must be configured to access your databases via DataSources.
HA-JDBC provides a DataSource, ConnectionPoolDataSource, and XADataSource implementations, depending on which resource you plan to proxy.

e.g.

`ha-jdbc-mycluster.xml`

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster>
			<database id="db1" location="org.postgresql.ds.PGSimpleDataSource">
				<user>pgadmin</user>
				<password>password</password>
				<property name="serverName">server1</property>
				<property name="portNumber">5432</property>
				<property name="databaseName">database</property>
			</database>
			<database id="db2" location="org.postgresql.ds.PGSimpleDataSource">
				<user>pgadmin</user>
				<password>password</password>
				<property name="serverName">server2</property>
				<property name="portNumber">5432</property>
				<property name="databaseName">database</property>
			</database>
		</cluster>
	</ha-jdbc>

To deploy the HA-JDBC DataSource in Tomcat, for example:

`context.xml`

	<Context>
		<!-- ... -->
		<Resource name="jdbc/mycluster" type="net.sf.hajdbc.sql.DataSource"
		          factory="org.apache.naming.factory.BeanFactory"
		          closeMethod="stop"
		          cluster="mycluster"
		          config="file:///path/to/ha-jdbc-mycluster.xml"/>
		<!-- ... -->
	</Context>

To make the DataSource referenceable from your application:

`web.xml`

	<web-app>
		<!-- ... -->
		<resource-env-ref>
			<resource-env-ref-name>jdbc/mycluster</resource-env-ref-name>
			<resource-env-ref-type>javax.sql.DataSource</resource-env-ref-type>
		</resource-env-ref>
		<!-- ... -->
	</web-app>

You can then access the cluster via:

	javax.naming.Context context = new javax.naming.InitialContext();
	javax.sql.DataSource ds = (javax.sql.DataSource) context.lookup("java:comp/env/jdbc/mycluster");
	java.sql.Connection connection = ds.getConnection("user", "password");


###	Handling JDBC statements

####	Database Reads

Database reads (e.g. SELECT statements) are handled using the following algorithm:

1.	Obtain the next database from the <a href="#balancer">balancer</a>.
1.	Execute the statement against this database.
1.	If the statement execution succeeded, return the result to the caller.
1.	If the statement execution failed, we analyze the caught exception.
	1.	If the exception is <a href="#failure">determined to be a failure</a>:
		1.	Deactivate the database.
		1.	Repeat using the next available database.
	1.	If the exception is determined *not* to be a failure, the exception is thrown back to the caller.

Alternatively, database writes can be configured to execute against both the master and backup databases concurrently.
While this will result in better performance, it will cause deadlocking if multiple application threads attempt to update the same database row.
If your use case is compatible with this limitation, you can enable parallel writes via the **transaction-mode** attribute.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<cluster transaction-mode="parallel">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


####	Database Writes

By default, database writes (e.g. INSERT/UPDATE/DELETE statements) are handled using the following algorithm:

1.	Execute the statement against the master database.
1.	If the statement execution failed *and* the exception is <a href="#failure">determined to be a failure</a>:
	1.	Deactivate the master database
	1.	Repeat using a new master.
1.	Otherwise, if the statement execute succeeded *or* the exception was *not* determined to be a failure.
	1.	Execute the statement against the backup databases in parallel.
	1.	Compare the result from the master database against the results from the backup databases.
	1.	If the result from a backup database does not match the result from the master database, deactivate that backup database.

###	<a name="failure"/>Handling Failures

To determine whether a given exception is due to a database failure, we consult the configured dialect.
The default implementation returns the following:

Dialect.indicatesFailure(SQLException)
:	The exception is determined to be a failure if the exception is an instance of `java.sql.SQLNonTransientConnectionException`.

Dialect.indicatesFailure(XAException)
:	The exception is determined to be a failure if the error code of the exception is `XAException.XAER_RMFAIL`.

Any dialect can override this behavior, perhaps by inspecting vendor codes, SQL states, etc.

If HA-JDBC determines that a given database has failed, the database is deactivated.
The process of deactivating a database is as follows:

1.	Log An ERROR message.
1.	Remove the database from the set of active databases.
1.	Persist the new cluster state via the <a href="#state">Cluster State Manager</a>.
1.	If the database cluster is **distributable**, broadcast the database deactivation to other servers.

Databases can also be deactivated manually via <a href="#jmx">JMX</a>.

You can optionally configure HA-JDBC to proactively detect database failures via the **failure-detect-schedule** attribute.
The value of this attribute defines a cron expression, which specifies the schedule a database cluster will detect failed databases and deactivate them.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<!-- Failure detection will run every minute -->
		<cluster failure-detect-schedule="0 * * ? * *">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	Restoring Failed Database Nodes

The process of (re)activating a database is as follows:

1.	Test that the target database is indeed alive.
1.	Acquire a lock that blocks all clients from writing to the database cluster.
1.	Synchronize the target database with the master database using a given synchronization strategy.
1.	Add the target database to the set of active databases.
1.	Persist the new cluster state via the <a href="#state">Cluster State Manager</a>.
	*	If the database cluster is **distributable**, broadcast the database activation to other servers.
1.	Release the lock acquired in step 2.

In general, database synchronization is an intensive and intrusive task.
To maintain database consistency, each database node in the cluster is read locked (i.e. writes are blocked) until synchronization completes.
Since synchronization may take anywhere from seconds to hours (depending on the size of your database and synchronization strategy employed), if your database cluster is used in a high write volume environment, it is recommended that activation only be performed during off-peak hours.

Alternatively, HA-JDBC can attempt to activate any inactive databases automatically via the **auto-activate-schedule** attribute.
If specified, HA-JDBC will automatically attempt to activate database nodes that are inactive, but alive, according to the specified cron schedule.

e.g.

	<ha-jdbc xmlns="urn:ha-jdbc:cluster:3.0">
		<!-- Auto-activation will run every day at 2:00 AM -->
		<cluster auto-activate-schedule="0 0 2 ? * *">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	<a name="jmx"/>HA-JDBC Administration

####	Database Cluster Management

By default, database clusters are registered with the platform mbean server using the following object name:

net.sf.hajdbc:type=DatabaseCluster,cluster=*cluster-id*

#####	Management Attributes

id
:	Indicates the unique identifier of this database cluster.

active
:	Indicates whether or not this database cluster is active.

version
:	Indicate the version of HA-JDBC in use.

activeDatabases
:	Enumerates the currently active databases in this database cluster.

inactiveDatabases
:	Enumerates the currently inactive databases in this database cluster.

defaultSynchronizationStrategy
:	Indicates the default synchronization strategy for this database cluster.

synchronizationStrategies
:	Enumerates the synchronization strategies available to this database cluster.


#####	Management Operations

isAlive(String databaseId)
:	Indicates whether the specified database is responsive and able to be activated.

activate(String databaseId)
:	Activates the specified database using the default synchronization strategy.

activate(String databaseId, String syncId)
:	Activates the specified database using the specified synchronization strategy.

deactivate(String databaseId)
:	Deactivates the specified database.

add(String databaseId)
:	Adds a new database to the cluster using the specified identifier.
	The database will remain inactive until fully specified and activated.
	To complete the database description use the net.sf.hajdbc:type=Database,cluster=*cluster-id*,database=*database-id* mbean.

remove(String databaseId)
:	Removes the specified database from the cluster.  Only inactive databases may be removed from the cluster.

flushMetaDataCache()
:	Flushed the internal cache of database meta data.



[commons-pool]: http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericObjectPool.html "Apache Commons Pool"
[jgroups]: http://community.jboss.org/wiki/JGroups "JGroups"
