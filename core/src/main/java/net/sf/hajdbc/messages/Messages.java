package net.sf.hajdbc.messages;

import java.beans.PropertyDescriptor;
import java.util.Set;

import javax.xml.stream.XMLStreamReader;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.Version;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.distributed.Member;
import net.sf.hajdbc.xml.XMLStreamFactory;

public interface Messages
{
	// XML parsing
	String resourceNotFound(String resource);
	String init(Version version, XMLStreamFactory factory);

	String unsupportedNamespace(XMLStreamReader reader);
	String unexpectedAttribute(XMLStreamReader reader, int index);
	String invalidAttribute(XMLStreamReader reader, int index);
	String missingRequiredAttribute(XMLStreamReader reader, String attribute);
	String unexpectedElement(XMLStreamReader reader);

	String invalidJavaBeanProperty(Class<?> bean, String property);
	String invalidJavaBeanPropertyValue(PropertyDescriptor descriptor, String value);
	String noSyncStrategies();
	String invalidSyncStrategy(String strategy, Set<String> strategies);
	String noDatabases();
	String noLocation(String databaseId);
	String invalidLocation(String databaseId, String location);

	String primaryKeyRequired(SynchronizationStrategy strategy, TableProperties table);

	String insertCount(TableProperties table, int count);
	String updateCount(TableProperties table, int count);
	String deleteCount(TableProperties table, int count);

	String dumpRestoreNotSupported(Dialect dialect);

	String noEmbeddedDriverFound();

	<Z, D extends Database<Z>> String invalidDatabase(DatabaseCluster<Z, D> cluster, String id);
	<Z, D extends Database<Z>> String notActive(DatabaseCluster<Z, D> cluster);
	<Z, D extends Database<Z>> String noActiveDatabases(DatabaseCluster<Z, D> cluster);
	<Z, D extends Database<Z>> String activated(DatabaseCluster<Z, D> cluster, D database);
	<Z, D extends Database<Z>> String deactivated(DatabaseCluster<Z, D> cluster, D database);
	<Z, D extends Database<Z>> String inconsistent(DatabaseCluster<Z, D> cluster, D database, Object actual, Object expected);

	<Z, D extends Database<Z>> String stillActive(DatabaseCluster<Z, D> cluster, D database);
	<Z, D extends Database<Z>> String activationFailed(DatabaseCluster<Z, D> cluster, D database);
	<Z, D extends Database<Z>> String proxyCreationFailed(DatabaseCluster<Z, D> cluster, D database, Class<?> proxyClass);
	<Z, D extends Database<Z>> String schemaLookupFailed(DatabaseCluster<Z, D> cluster, String table);

	<Z, D extends Database<Z>> String synchronizationBegin(DatabaseCluster<Z, D> cluster, D database, SynchronizationStrategy strategy);
	<Z, D extends Database<Z>> String synchronizationEnd(DatabaseCluster<Z, D> cluster, D database, SynchronizationStrategy strategy);
	
	<Z, D extends Database<Z>> String registerDriverFailed(Class<?> driverClass);

	<Z, D extends Database<Z>> String initialClusterState(DatabaseCluster<Z, D> cluster, Set<String> state, Member member);
	<Z, D extends Database<Z>> String initialClusterState(DatabaseCluster<Z, D> cluster, Set<String> state);
	<Z, D extends Database<Z>> String initialClusterStateEmpty(DatabaseCluster<Z, D> cluster);

	<Z, D extends Database<Z>> String clusterStatePersistence(DatabaseCluster<Z, D> cluster, String url);

	<Z, D extends Database<Z>> String sequenceOutOfSync(SequenceProperties sequence, D activeDatabase, long activeValue, D database, long value);
}
