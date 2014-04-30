/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.IdentityColumnSupport;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.lock.LockManager;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractSQLProxyFactory<Z, D extends Database<Z>, P, T> extends AbstractTransactionalProxyFactory<Z, D, P, T> implements SQLProxyFactory<Z, D, P, T>
{
	protected AbstractSQLProxyFactory(P parent, ProxyFactory<Z, D, P, SQLException> parentMap, Invoker<Z, D, P, T, SQLException> invoker, Map<D, T> map, TransactionContext<Z, D> context)
	{
		super(parent, parentMap, invoker, map, context);
	}

	@Override
	public String evaluate(final String rawSQL)
	{
		String sql = rawSQL;
		
		long now = System.currentTimeMillis();
		
		DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
		Dialect dialect = cluster.getDialect();
		
		if (cluster.isCurrentTimestampEvaluationEnabled())
		{
			sql = dialect.evaluateCurrentTimestamp(sql, new java.sql.Timestamp(now));
		}
		
		if (cluster.isCurrentDateEvaluationEnabled())
		{
			sql = dialect.evaluateCurrentDate(sql, new java.sql.Date(now));
		}
		
		if (cluster.isCurrentTimeEvaluationEnabled())
		{
			sql = dialect.evaluateCurrentTime(sql, new java.sql.Time(now));
		}
		
		if (cluster.isRandEvaluationEnabled())
		{
			sql = dialect.evaluateRand(sql);
		}
		
		return sql;
	}
	
	@Override
	public List<Lock> extractLocks(String sql) throws SQLException
	{
		return this.extractLocks(Collections.singleton(sql));
	}
	
	protected List<Lock> extractLocks(Collection<String> statements) throws SQLException
	{
		Set<String> identifierSet = new TreeSet<>();
		DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
		
		for (String sql: statements)
		{
			if (cluster.isSequenceDetectionEnabled())
			{
				SequenceSupport support = cluster.getDialect().getSequenceSupport();
				
				if (support != null)
				{
					String sequence = support.parseSequence(sql);
					
					if (sequence != null)
					{
						identifierSet.add(sequence);
					}
				}
			}
			
			if (cluster.isIdentityColumnDetectionEnabled())
			{
				IdentityColumnSupport support = cluster.getDialect().getIdentityColumnSupport();
				
				if (support != null)
				{
					String table = support.parseInsertTable(sql);
					
					if (table != null)
					{
						TableProperties tableProperties = this.getDatabaseProperties().findTable(table);
						
						if (tableProperties == null)
						{
							throw new SQLException(this.messages.schemaLookupFailed(cluster, table));
						}
						
						if (!tableProperties.getIdentityColumns().isEmpty())
						{
							identifierSet.add(tableProperties.getName().getDMLName());
						}
					}
				}
			}
		}
		
		List<Lock> lockList = new ArrayList<>(identifierSet.size());
		
		if (!identifierSet.isEmpty())
		{
			LockManager lockManager = cluster.getLockManager();
			
			for (String identifier: identifierSet)
			{
				lockList.add(lockManager.writeLock(identifier));
			}
		}
		
		return lockList;
	}

	private DatabaseProperties getDatabaseProperties() throws SQLException
	{
		DatabaseCluster<Z, D> cluster = this.getDatabaseCluster();
		D database = cluster.getBalancer().primary();
		return cluster.getDatabaseMetaDataCache().getDatabaseProperties(database, this.getConnection(database));
	}
	
	@Override
	public boolean isSelectForUpdate(String sql) throws SQLException
	{
		return this.getDatabaseProperties().supportsSelectForUpdate() ? this.getDatabaseCluster().getDialect().isSelectForUpdate(sql) : false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		return this.getDatabaseProperties().locatorsUpdateCopy();
	}
}
