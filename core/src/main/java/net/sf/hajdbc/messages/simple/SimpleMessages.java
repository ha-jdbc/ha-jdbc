package net.sf.hajdbc.messages.simple;

import java.beans.PropertyDescriptor;
import java.text.MessageFormat;
import java.util.Set;

import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamReader;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.Version;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.distributed.Member;
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.xml.XMLStreamFactory;

public class SimpleMessages implements Messages
{
	@Override
	public String unsupportedNamespace(XMLStreamReader reader)
	{
		Location location = reader.getLocation();
		return this.tr("Unsupported namespace <{2} xmlns=\"{3}\"/> found at [{0}:{1}]", location.getLineNumber(), location.getColumnNumber(), reader.getLocalName(), reader.getNamespaceURI());
	}

	@Override
	public String unexpectedElement(XMLStreamReader reader)
	{
		Location location = reader.getLocation();
		return this.tr("Unexpected <{2}/> attribute found at [{0}:{1}]", location.getLineNumber(), location.getColumnNumber(), reader.getLocalName());
	}
	
	@Override
	public String unexpectedAttribute(XMLStreamReader reader, int index)
	{
		Location location = reader.getLocation();
		return this.tr("Unexpected <{2} {3}=\"...\"/> attribute found at [{0}:{1}]", location.getLineNumber(), location.getColumnNumber(), reader.getLocalName(), reader.getAttributeLocalName(index));
	}
	
	@Override
	public String invalidAttribute(XMLStreamReader reader, int index)
	{
		Location location = reader.getLocation();
		return this.tr("Invalid value for <{2} {3}=\"{4}\"/> found at [{0}:{1}]", location.getLineNumber(), location.getColumnNumber(), reader.getLocalName(), reader.getAttributeLocalName(index), reader.getAttributeValue(index));
	}

	@Override
	public String missingRequiredAttribute(XMLStreamReader reader, String attribute)
	{
		Location location = reader.getLocation();
		return this.tr("<{2} {3}=\"...\"/> attribute is missing at [{0}:{1}]", location.getLineNumber(), location.getColumnNumber(), reader.getLocalName(), attribute);
	}

	@Override
	public String invalidJavaBeanProperty(Class<?> bean, String property)
	{
		return this.tr("{1} is not a valid JavaBean property of {0}", bean.getName(), property);
	}

	@Override
	public String invalidJavaBeanPropertyValue(PropertyDescriptor descriptor, String value)
	{
		return this.tr("\"{1}\" is not a valid value for {0}", descriptor.getName(), value);
	}

	@Override
	public String invalidSyncStrategy(String strategy, Set<String> strategies)
	{
		return this.tr("{0} is not one of the available synchronization strategies: {1}", strategy, strategies);
	}

	@Override
	public String noSyncStrategies()
	{
		return this.tr("A database cluster must define at least one synchronization strategy");
	}

	@Override
	public String noDatabases()
	{
		return this.tr("A database cluster must define at least one database");
	}

	@Override
	public String resourceNotFound(String resource)
	{
		return this.tr("Failed to locate {0}", resource);
	}

	@Override
	public String init(Version version, XMLStreamFactory factory)
	{
		return this.tr("Initializing HA-JDBC {0} from {1}", version, factory);
	}

	@Override
	public <Z, D extends Database<Z>> String notActive(DatabaseCluster<Z, D> cluster)
	{
		return this.tr("Database cluster {0} is not active", cluster);
	}

	@Override
	public <Z, D extends Database<Z>> String noActiveDatabases(DatabaseCluster<Z, D> cluster)
	{
		return this.tr("Database cluster {0} has no active databases", cluster);
	}

	@Override
	public <Z, D extends Database<Z>> String activated(DatabaseCluster<Z, D> cluster, D database)
	{
		return this.tr("Activated database {1} from cluster {0}", cluster, database);
	}

	@Override
	public <Z, D extends Database<Z>> String deactivated(DatabaseCluster<Z, D> cluster, D database)
	{
		return this.tr("Deactivated database {1} from cluster {0}", cluster, database);
	}

	@Override
	public <Z, D extends Database<Z>> String inconsistent(DatabaseCluster<Z, D> cluster, D database, Object expected, Object actual)
	{
		return this.tr("Deactivated database {1} from cluster {0} due to inconsistent operations results: expected [{2}], actual [{3}]", cluster, database, expected, actual);
	}

	@Override
	public <Z, D extends Database<Z>> String stillActive(DatabaseCluster<Z, D> cluster, D database)
	{
		return this.tr("Database {1} from cluster {0} could not be removed because it is still active.", cluster, database);
	}

	@Override
	public <Z, D extends Database<Z>> String invalidDatabase(DatabaseCluster<Z, D> cluster, String id)
	{
		return this.tr("{1} is not a valid database of cluster {0}", cluster, id);
	}

	@Override
	public <Z, D extends Database<Z>> String activationFailed(DatabaseCluster<Z, D> cluster, D database)
	{
		return this.tr("Failed to activate database {1} from cluster {0}", cluster, database);
	}

	@Override
	public <Z, D extends Database<Z>> String proxyCreationFailed(DatabaseCluster<Z, D> cluster, D database, Class<?> proxyClass)
	{
		return this.tr("Deactivated database {1} from cluster {0} due to {2} proxy creation failure", cluster, database, proxyClass.getName());
	}

	@Override
	public <Z, D extends Database<Z>> String schemaLookupFailed(DatabaseCluster<Z, D> cluster, String table)
	{
		return this.tr("Failed to locate schema for table {1} for cluster {0}.  Verify implementation of {2}.getDefaultSchemas()", cluster, table, cluster.getDialect().getClass().getName());
	}

	@Override
	public <Z, D extends Database<Z>> String synchronizationBegin(DatabaseCluster<Z, D> cluster, D database, SynchronizationStrategy strategy)
	{
		return this.tr("Starting synchronization of database {1} from cluster {0} using {2} synchronization strategy", cluster, database, strategy);
	}

	@Override
	public <Z, D extends Database<Z>> String synchronizationEnd(DatabaseCluster<Z, D> cluster, D database, SynchronizationStrategy strategy)
	{
		return this.tr("Completed synchronization of database {1} from cluster {0} using {2} synchronization strategy", cluster, database, strategy);
	}

	@Override
	public <Z, D extends Database<Z>> String registerDriverFailed(Class<?> driverClass)
	{
		return this.tr("Failed to register driver {0}", driverClass.getName());
	}

	@Override
	public String noLocation(String database)
	{
		return this.tr("Database {0} does not define a location", database);
	}

	@Override
	public String invalidLocation(String database, String location)
	{
		return this.tr("Database {0} defines an invalid location: {1}", database, location);
	}

	@Override
	public <Z, D extends Database<Z>> String initialClusterState(DatabaseCluster<Z, D> cluster, Set<String> state, Member member)
	{
		return this.tr("Database cluster {0} will use initial cluster state {1} obtained from {2}", cluster, state, member);
	}

	@Override
	public <Z, D extends Database<Z>> String initialClusterState(DatabaseCluster<Z, D> cluster, Set<String> state)
	{
		return this.tr("Database cluster {0} will use initial cluster state {1}", cluster, state);
	}

	@Override
	public <Z, D extends Database<Z>> String initialClusterStateEmpty(DatabaseCluster<Z, D> cluster)
	{
		return this.tr("Database cluster {0} found no initial cluster state", cluster);
	}

	@Override
	public String primaryKeyRequired(SynchronizationStrategy strategy, TableProperties table)
	{
		return this.tr("Table {1} does not define a primary key, a requirement of the {0} synchronization strategy", strategy, table.getName());
	}

	@Override
	public String insertCount(TableProperties table, int count)
	{
		return this.tr("Inserted {1} rows into {0}", table.getName(), count);
	}

	@Override
	public String updateCount(TableProperties table, int count)
	{
		return this.tr("Updated {1} rows in {0}", table.getName(), count);
	}

	@Override
	public String deleteCount(TableProperties table, int count)
	{
		return this.tr("Deleted {1} rows from {0}", table.getName(), count);
	}

	@Override
	public String dumpRestoreNotSupported(Dialect dialect)
	{
		return this.tr("The {0} dialect does not yet implement dump-restore support", dialect.getClass().getName());
	}

	@Override
	public <Z, D extends Database<Z>> String sequenceOutOfSync(SequenceProperties sequence, D activeDatabase, long activeValue, D database, long value)
	{
		return this.tr("Next value ({2}) for sequence {0} from database {1} does not match next value ({4}) from database {3}", sequence, activeDatabase, activeValue, database, value);
	}

	@Override
	public String noEmbeddedDriverFound()
	{
		return this.tr("Failed to detect an embedded database driver on the classpath");
	}

	@Override
	public <Z, D extends Database<Z>> String clusterStatePersistence(DatabaseCluster<Z, D> cluster, String url)
	{
		return this.tr("State for database cluster {0} will be persisted to {1}", cluster, url);
	}

	protected String tr(String message)
	{
		return message;
	}

	protected String tr(String pattern, Object... args)
	{
		return MessageFormat.format(pattern, args);
	}
}
