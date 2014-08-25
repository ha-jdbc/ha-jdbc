/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.messages;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLStreamReader;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.Version;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.distributed.Command;
import net.sf.hajdbc.distributed.Member;
import net.sf.hajdbc.logging.LoggingProvider;
import net.sf.hajdbc.util.Matcher;
import net.sf.hajdbc.xml.XMLStreamFactory;

public interface Messages
{
	String logging(LoggingProvider provider);
	
	String annotationMissing(Class<?> targetClass, Class<? extends Annotation> annotationClass);

	String serviceNotFound(Class<?> serviceClass);
	<T> String serviceNotFound(Class<T> serviceClass, Matcher<T> matcher);
	<T> String multipleServicesFound(Class<T> serviceClass, Matcher<T> matcher, List<T> matches);

	String status(ProcessBuilder processBuilder, int status);

	// XML parsing
	String resourceNotFound(String resource);
	String init(Version version, XMLStreamFactory factory);

	String unsupportedNamespace(XMLStreamReader reader);
	String unexpectedAttribute(XMLStreamReader reader, int index);
	String invalidAttribute(XMLStreamReader reader, int index);
	String missingRequiredAttribute(XMLStreamReader reader, String attribute);
	String unexpectedElement(XMLStreamReader reader);

	// Configuration
	String invalidJavaBeanProperty(Class<?> bean, String property);
	String invalidJavaBeanPropertyValue(PropertyDescriptor descriptor, String value);
	String noSyncStrategies();
	String invalidSyncStrategy(String strategy, Set<String> strategies);
	String noDatabases();
	String noLocation(String databaseId);
	String invalidLocation(String databaseId, String location);

	// Sync strategies
	String primaryKeyRequired(SynchronizationStrategy strategy, TableProperties table);

	String insertCount(TableProperties table, int count);
	String updateCount(TableProperties table, int count);
	String deleteCount(TableProperties table, int count);

	String dumpRestoreNotSupported(Dialect dialect);

	String noEmbeddedDriverFound();

	String noDurabilityPhase(Method method);

	<Z, D extends Database<Z>> String start(DatabaseCluster<Z, D> cluster);
	<Z, D extends Database<Z>> String stop(DatabaseCluster<Z, D> cluster);
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

	String sendCommandToClusterFailed(Command<?, ?> command);
	String sendCommandToMemberFailed(Command<?, ?> command, Member member);
	String executeCommandFailed(Command<?, ?> command, Member member);
}
