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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.io.InputSinkRegistryImpl;
import net.sf.hajdbc.io.InputSinkStrategy;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.util.Resources;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractStatementProxyFactory<Z, D extends Database<Z>, S extends Statement> extends AbstractInputSinkRegistryProxyFactory<Z, D, Connection, S>
{
	private final List<Invoker<Z, D, S, ?, SQLException>> batchInvokers = new LinkedList<Invoker<Z, D, S, ?, SQLException>>();
	private final List<String> batch = new LinkedList<String>();
	
	@SuppressWarnings("unchecked")
	protected AbstractStatementProxyFactory(Connection parent, ProxyFactory<Z, D, Connection, SQLException> parentFactory, Invoker<Z, D, Connection, S, SQLException> invoker, Map<D, S> map, TransactionContext<Z, D> context)
	{
		super(parent, parentFactory, invoker, map, context, new InputSinkRegistryImpl<Object>((InputSinkStrategy<Object>) parentFactory.getDatabaseCluster().getInputSinkStrategy()));
	}

	@Override
	public Connection getConnection(D database) throws SQLException
	{
		return this.get(database).getConnection();
	}

	public List<Lock> getBatchLocks() throws SQLException
	{
		return this.extractLocks(this.batch);
	}
	
	public void addBatchSQL(String sql)
	{
		this.batch.add(sql);
	}

	public List<String> getBatch()
	{
		return this.batch;
	}

	public void clearBatch()
	{
		this.batch.clear();
	}
	
	public void addBatchInvoker(Invoker<Z, D, S, ?, SQLException> invoker)
	{
		this.batchInvokers.add(invoker);
	}
	
	public void clearBatchInvokers()
	{
		this.batchInvokers.clear();
	}
	
	@Override
	public void replay(D database, S object) throws SQLException
	{
		super.replay(database, object);
		
		for (Invoker<Z, D, S, ?, SQLException> invoker: this.batchInvokers)
		{
			this.logger.log(Level.TRACE, "Replaying {1}.{2} against database {0}", database, object.getClass().getName(), invoker);

			try
			{
				invoker.invoke(database, object);
			}
			catch (Throwable e)
			{
				this.getExceptionFactory().createException(e);
			}
		}
	}

	@Override
	public void close(D database, S statement)
	{
		Resources.close(statement);
	}
}
