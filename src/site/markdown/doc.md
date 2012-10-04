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
*	Configuration XML file per database cluster or bootstrapped programmatic configuration


##	Configuration

###	XML

The algorithm used to locate the configuration file resource at runtime is as follows:

1.	Determine the potentially parameterized resource name from one of the following sources:
	1.	A `config` property passed to `DriverManager.getConnection(String, Properties)`, or the `config` property of the `DataSource`, `ConnectionPoolDataSource`, or `XADataSource`.
	1.	The `ha-jdbc.cluster-id.configuration` system property.
	1.	Use default value: `ha-jdbc.{0}.xml`
1.	Format the parameterized resource name using the identifier of the cluster.
1.	Convert the formatted resource name to a URL. If the resource is not a URL, search for the resource in the classpath using the following class loaders:
	1.	Thread context class loader
	1.	The classloader used to load HA-JDBC
	1.	System class loader


###	Dialect

The dialect attribute of a cluster determines the SQL syntax to use for a given database operation.
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
		<td>**standard**</td>
		<td>SQL-92 compliant database</td>
		<td></td>
		<td></td>
		<td></td>
	</tr>
</table>

e.g.

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<cluster dialect="postgresql">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	Balancer

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

load
:	Requests are sent to the node with the smallest load.
	Node weights affect the calculated load of a given node.
	The load of a node = *concurrent-requests* / *weight*.

e.g.

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<cluster balancer="simple">
			<!-- Read requests will always prefer db1 -->
			<database id="db1" weight="2"><!-- ... --></database>
			<database id="db2" weight="1"><!-- ... --></database>
		</cluster>
	</ha-jdbc>


###	Synchronization strategies

Defines a strategy for synchronizing a database before activation.
If the strategy contains JavaBean properties, you can override their default values.

passive
:	Does nothing.
	Use this strategy for read-only clusters, or if you know for a fact that the target database is already in sync.

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
:	Performs a full table scan of source table vs destination table, and performs necessary insert/update/delete.
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

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<sync id="full">
			<property name="fetchSize">1000</property>
		</sync>
		<sync id="diff">
			<property name="fetchSize">1000</property>
			<property name="versionPattern">version</property>
		</sync>
		<cluster default-sync="diff"><!-- ... --></cluster>
	</ha-jdbc>


###	Cluster state management

simple
:	A non-persistent state manager that stores cluster state in memory.

sql
:	A persistent state manager that uses an embedded database.
	This provider supports the following properties.
	<table>
		<tr>
			<th>Property</th>
			<th>Default</th>
			<th>Description</th>
		</tr>
		<tr>
			<td>**urlPattern**</td>
			<td>
				<nobr>jdbc:h2:{1}/{0}</nobr><br/>
				<nobr>jdbc:hsqldb:{1}/{0}</nobr><br/>
				<nobr>jdbc:derby:{1}/{0};create=true</nobr>
			</td>
			<td>
				A MessageFormat pattern indicating the JDBC url of the embedded database.
				The pattern can accept 2 parameters:
				<ol>
					<li>The cluster identifier - typically used as the database name.</li>
					<li>The home directory of the system user - typically used to indicate the location of the embedded database.</li>
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
	You can also override several properites to manipulate connection pooling behavior.
	The complete list and their default values are available in the [Apache Commons Pool documentation][commons-pool].

e.g.

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<state id="sql">
			<property name="urlPattern">jdbc:hsqldb:{1}/{0}</property>
		</state>
		<cluster><!-- ... --></cluster>
	</ha-jdbc>


###	Durability

As of version 2.1, HA-JDBC support a configurable durability level for user transactions.
When enabled, HA-JDBC will track transactions, such that, upon restart, following a crash, it can detect and recover from any partial commits.
The durability persistence mechanism is determined by the state manager configuration.
By default, HA-JDBC includes support for the following durability levels:

none
:	No invocations are tracked.
	This durability level can neither detect nor recover from mid-commit crashes.
	This level offers the best performance, but offers no protection from crashes.
	Read-only database clusters should use this level.

coarse
:	Tracks cluster invocations only, but not per-database invocations.
	This durability level can detect, but not recover from, mid-commit crashes.
	Upon recovery, if any cluster invocations still exist in the log, all slave database will be deactivated and must be reactivated manually.
	This level offers a compromise between performance and resiliency.

fine
:	Tracks cluster invocations as well as per-database invokers.
	This durability level can both detect and recover from mid-commit crashes.
	Upon recovery, if any cluster invocations still exist in the log, all slave database will be deactivated and must be reactivated manually.
	While this level is the slowest, it ensures the highest level of resilency from crashes.

e.g.

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<cluster durability="fine">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	Distributed capabilities

Indicates that database clusters defined in this file will be accessed by multiple JVMs.
By default, HA-JDBC supports the following providers:

jgroups
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
			<td>udp-sync.xml</td>
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

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<distributable id="jgroups">
			<property name="stack">udp.xml</property>
		</distributable>
		<cluster><!-- ... --></cluster>
	</ha-jdbc>


###	Database meta-data caching

HA-JDBC makes extensive use of database meta data.
For performance purposes, this information should be cached whenever possible.
By default, HA-JDBC includes the following meta-data-cache options:

none
:	Meta data is loaded when requested and not cached.

lazy
:	Meta data is loaded and cached per database as it is requested.

shared-lazy
:	Meta data is loaded and cached as it is requested.

eager
:	All necessary meta data is loaded and cached per database during HA-JDBC initialization.

shared-eager
:	All necessary meta data is loaded and cached during HA-JDBC initialization.

e.g.

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<cluster meta-data-cache="shared-eager">
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	Password Obfuscation

Since HA-JDBC's configuration file contains references to database passwords, some users may want to obfuscate these.
To indicate that a password uses an obfuscation mechanism, use a ":" to indicate the appropriate decoder.

e.g.

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<cluster ...>
			<database id="db1">
				<name>jdbc:h2:mem:db1</name>
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
			<td>$HOME/.keystore</td>
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


###	Customizing HA-JDBC

HA-JDBC supports a number of extension points.
Most components support custom implementations, including:

*	net.sf.hajdbc.balancer.BalancerFactory
*	net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory
*	net.sf.hajdbc.codec.CodecFactory
*	net.sf.hajdbc.dialect.DialectFactory
*	net.sf.hajdbc.distributed.CommandDispatcherFactory
*	net.sf.hajdbc.durability.DurabilityFactory
*	net.sf.hajdbc.lock.LockManagerFactory
*	net.sf.hajdbc.state.StateManagerFactory
*	net.sf.hajdbc.SynchronizationStrategy

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

	<ha-jdbc xmlns="urn:sourceforge:ha-jdbc:2.1">
		<cluster dialect="custom" ...>
			<!-- ... -->
		</cluster>
	</ha-jdbc>


###	Programmatic configuration

As of version 2.1, an HA-JDBC cluster can be configured programmatically.

e.g.

	// Define each database in the cluster
	DriverDatabase db1 = new DriverDatabase();
	db1.setId("db1");
	db1.setName("jdbc:hsqldb:mem:db1");
	db1.setUser("sa");
	db1.setPassword("");
	
	DriverDatabase db2 = new DriverDatabase();
	db2.setId("db2");
	db2.setName("jdbc:hsqldb:mem:db2");
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
	for (java.sql.Driver driver: DriverManager.getDrivers()) {
		if (driver instanceof Driver) {
			((Driver) driver).getConfigurationFactories().put("mycluster", new SimpleDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(config));
		}
	}
	
	// Database cluster is now ready to be used!
	Connection connection = DriverManager.getConnection("jdbc:ha-jdbc:mycluster", "sa", "");

[commons-pool]: http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericObjectPool.html "Apache Commons Pool"
[jgroups]: http://community.jboss.org/wiki/JGroups "JGroups"