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
package net.sf.hajdbc.sql.pool;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.AbstractTransactionalProxyFactory;
import net.sf.hajdbc.sql.LocalTransactionContext;
import net.sf.hajdbc.sql.ProxyFactory;

/**
 * 
 * @author Paul Ferraro
 */
public abstract class AbstractPooledConnectionProxyFactory<Z, D extends Database<Z>, C extends PooledConnection> extends AbstractTransactionalProxyFactory<Z, D, Z, C>
{
	private Map<ConnectionEventListener, Invoker<Z, D, C, ?, SQLException>> connectionEventListenerInvokers = new HashMap<>();
	private Map<StatementEventListener, Invoker<Z, D, C, ?, SQLException>> statementEventListenerInvokers = new HashMap<>();
	
	protected AbstractPooledConnectionProxyFactory(Z parentProxy, ProxyFactory<Z, D, Z, SQLException> parent, Invoker<Z, D, Z, C, SQLException> invoker, Map<D, C> map)
	{
		super(parentProxy, parent, invoker, map, new LocalTransactionContext<>(parent.getDatabaseCluster()));
	}

	public void addConnectionEventListener(ConnectionEventListener listener, Invoker<Z, D, C, ?, SQLException> invoker)
	{
		this.connectionEventListenerInvokers.put(listener, invoker);
	}
	
	public void removeConnectionEventListener(ConnectionEventListener listener)
	{
		this.connectionEventListenerInvokers.remove(listener);
	}
	
	public void addStatementEventListener(StatementEventListener listener, Invoker<Z, D, C, ?, SQLException> invoker)
	{
		this.statementEventListenerInvokers.put(listener, invoker);
	}
	
	public void removeStatementEventListener(StatementEventListener listener)
	{
		this.statementEventListenerInvokers.remove(listener);
	}
	
	@Override
	public void replay(D database, C connection) throws SQLException
	{
		super.replay(database, connection);
		
		for (Invoker<Z, D, C, ?, SQLException> invoker: this.connectionEventListenerInvokers.values())
		{
			invoker.invoke(database, connection);
		}
		for (Invoker<Z, D, C, ?, SQLException> invoker: this.statementEventListenerInvokers.values())
		{
			invoker.invoke(database, connection);
		}
	}

	@Override
	public void close(D database, C connection) throws SQLException
	{
		connection.close();
	}

	@SuppressWarnings("unused")
	private static class ConnectionEventListenerFilter<Z, D extends Database<Z>, C extends PooledConnection> implements ConnectionEventListener
	{
		private AbstractPooledConnectionProxyFactory<Z, D, C> proxyFactory;
		private final D database;
		private final ConnectionEventListener listener;
		
		ConnectionEventListenerFilter(AbstractPooledConnectionProxyFactory<Z, D, C> proxyFactory, D database, ConnectionEventListener listener)
		{
			this.proxyFactory = proxyFactory;
			this.database = database;
			this.listener = listener;
		}
		
		@Override
		public void connectionClosed(ConnectionEvent event)
		{
			ConnectionEvent e = this.getEvent(event);
			
			if (e != null)
			{
				this.listener.connectionClosed(e);
			}
		}

		@Override
		public void connectionErrorOccurred(ConnectionEvent event)
		{
			ConnectionEvent e = this.getEvent(event);
			
			if (e != null)
			{
				this.listener.connectionErrorOccurred(e);
			}
		}
		
		private ConnectionEvent getEvent(ConnectionEvent event)
		{
			Object source = event.getSource();
			C connection = this.proxyFactory.get(this.database);
			
			if (Proxy.isProxyClass(source.getClass()) && Proxy.getInvocationHandler(source) instanceof AbstractPooledConnectionInvocationHandler)
			{
				return new ConnectionEvent(connection, event.getSQLException());
			}
			
			return event.getSource().equals(connection) ? event : null;
		}
	}
	
	@SuppressWarnings("unused")
	private static class StatementEventListenerFilter<Z, D extends Database<Z>, C extends PooledConnection> implements StatementEventListener
	{
		private AbstractPooledConnectionProxyFactory<Z, D, C> proxyFactory;
		private final D database;
		private final StatementEventListener listener;
		
		StatementEventListenerFilter(AbstractPooledConnectionProxyFactory<Z, D, C> proxyFactory, D database, StatementEventListener listener)
		{
			this.proxyFactory = proxyFactory;
			this.database = database;
			this.listener = listener;
		}
		
		@Override
		public void statementClosed(StatementEvent event)
		{
			StatementEvent e = this.getEvent(event);
			
			if (e != null)
			{
				this.listener.statementClosed(e);
			}
		}

		@Override
		public void statementErrorOccurred(StatementEvent event)
		{
			StatementEvent e = this.getEvent(event);
			
			if (e != null)
			{
				this.listener.statementErrorOccurred(e);
			}
		}
		
		private StatementEvent getEvent(StatementEvent event)
		{
			Object source = event.getSource();
			C connection = this.proxyFactory.get(this.database);
			
			if (Proxy.isProxyClass(source.getClass()) && Proxy.getInvocationHandler(source) instanceof AbstractPooledConnectionInvocationHandler)
			{
				return new StatementEvent(connection, event.getStatement(), event.getSQLException());
			}
			
			return source.equals(connection) ? event : null;
		}
	}
}
