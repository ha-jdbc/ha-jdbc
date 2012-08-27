/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import net.sf.hajdbc.balancer.BalancerFactory;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory;
import net.sf.hajdbc.codec.CodecFactory;
import net.sf.hajdbc.dialect.DialectFactory;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.durability.DurabilityFactory;
import net.sf.hajdbc.management.MBeanRegistrar;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;

import org.quartz.CronExpression;

/**
 * @author Paul Ferraro
 */
public interface DatabaseClusterConfiguration<Z, D extends Database<Z>> extends Serializable
{
	CommandDispatcherFactory getDispatcherFactory();
	
	/**
	 * Returns the databases of this cluster indexed by identifier
	 * @return a map of databases
	 * @throws IllegalArgumentException if no database exists with the specified identifier
	 */
	Map<String, D> getDatabaseMap();
	
	Map<String, SynchronizationStrategy> getSynchronizationStrategyMap();
	
	String getDefaultSynchronizationStrategy();
	
	/**
	 * Returns the Balancer implementation used by this database cluster.
	 * @return an implementation of <code>Balancer</code>
	 */
	BalancerFactory getBalancerFactory();

	TransactionMode getTransactionMode();
	
	ExecutorServiceProvider getExecutorProvider();
	
	/**
	 * Returns a dialect capable of returning database vendor specific values.
	 * @return an implementation of <code>Dialect</code>
	 */
	DialectFactory getDialectFactory();
	
	/**
	 * Returns a StateManager for persisting database cluster state.
	 * @return a StateManager implementation
	 */
	StateManagerFactory getStateManagerFactory();
	
	/**
	 * Returns a DatabaseMetaData cache.
	 * @return a <code>DatabaseMetaDataCache</code> implementation
	 */
	DatabaseMetaDataCacheFactory getDatabaseMetaDataCacheFactory();

	DurabilityFactory getDurabilityFactory();

	/**
	 * Indicates whether or not sequence detection is enabled for this cluster.
	 * @return true, if sequence detection is enabled, false otherwise.
	 */
	boolean isSequenceDetectionEnabled();
	
	/**
	 * Indicates whether or not identity column detection is enabled for this cluster.
	 * @return true, if identity column detection is enabled, false otherwise.
	 */
	boolean isIdentityColumnDetectionEnabled();
	
	/**
	 * Indicates whether or not non-deterministic CURRENT_DATE SQL functions will be evaluated to deterministic static values.
	 * @return true, if temporal SQL replacement is enabled, false otherwise.
	 */
	boolean isCurrentDateEvaluationEnabled();
	
	/**
	 * Indicates whether or not non-deterministic CURRENT_TIME functions will be evaluated to deterministic static values.
	 * @return true, if temporal SQL replacement is enabled, false otherwise.
	 */
	boolean isCurrentTimeEvaluationEnabled();
	
	/**
	 * Indicates whether or not non-deterministic CURRENT_TIMESTAMP functions will be evaluated to deterministic static values.
	 * @return true, if temporal SQL replacement is enabled, false otherwise.
	 */
	boolean isCurrentTimestampEvaluationEnabled();
	
	/**
	 * Indicates whether or not non-deterministic RAND() functions will be replaced by evaluated to static values.
	 * @return true, if temporal SQL replacement is enabled, false otherwise.
	 */
	boolean isRandEvaluationEnabled();

	CronExpression getFailureDetectionExpression();
	
	CronExpression getAutoActivationExpression();
	
	ThreadFactory getThreadFactory();
	
	CodecFactory getCodecFactory();
	
	MBeanRegistrar<Z, D> getMBeanRegistrar();
	
	boolean isFairLocking();
	
	TransactionIdentifierFactory<? extends Object> getTransactionIdentifierFactory();
	
	boolean isEmptyClusterAllowed();
}
