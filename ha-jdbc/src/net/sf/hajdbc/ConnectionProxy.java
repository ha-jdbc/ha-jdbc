/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ConnectionProxy extends SQLProxy implements java.sql.Connection
{
	public ConnectionProxy(SQLProxy connectionFactory, Operation operation) throws java.sql.SQLException
	{
		super(connectionFactory, operation);
	}
	
	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return new Integer(connection.getHoldability());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return new Integer(connection.getTransactionIsolation());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.clearWarnings();
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Connection#close()
	 */
	public void close() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.close();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Connection#commit()
	 */
	public void commit() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.commit();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.rollback();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return new Boolean(connection.getAutoCommit());
			}
		};
		
		return ((Boolean) this.executeGet(operation)).booleanValue();
	}

	/**
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return new Boolean(connection.isClosed());
			}
		};
		
		return ((Boolean) this.executeGet(operation)).booleanValue();
	}

	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return new Boolean(connection.isReadOnly());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(final int holdability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.setHoldability(holdability);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(final int transactionIsolation) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.setTransactionIsolation(transactionIsolation);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(final boolean autoCommit) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.setAutoCommit(autoCommit);
				
				return null;
			}
		};
		
		this.executeWrite(operation);

		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	public void setReadOnly(final boolean readOnly) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.setReadOnly(readOnly);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.getCatalog();
			}
		};
		
		return (String) this.executeRead(operation);
	}

	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(final String catalog) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.setCatalog(catalog);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.getMetaData();
			}
		};
		
		return (DatabaseMetaData) this.executeRead(operation);
	}

	/**
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.getWarnings();
			}
		};
		
		return (SQLWarning) this.executeGet(operation);
	}

	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	public Savepoint setSavepoint() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.setSavepoint();
			}
		};
		
		return new SavepointProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		final SavepointProxy savepointProxy = (SavepointProxy) savepoint;
		
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				Savepoint savepoint = (Savepoint) savepointProxy.getSQLObject(database);
				
				connection.releaseSavepoint(savepoint);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(Savepoint savepoint) throws SQLException
	{
		final SavepointProxy savepointProxy = (SavepointProxy) savepoint;
		
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				Savepoint savepoint = (Savepoint) savepointProxy.getSQLObject(database);
				
				connection.rollback(savepoint);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Connection#createStatement()
	 */
	public Statement createStatement() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.createStatement();
			}
		};
		
		return this.isReadOnly() ? (Statement) this.executeGet(operation) : new StatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? (Statement) this.executeGet(operation) : new StatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? (Statement) this.executeGet(operation) : new StatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map getTypeMap() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.getTypeMap();
			}
		};
		
		return (Map) this.executeGet(operation);
	}

	/**
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	public void setTypeMap(final Map map) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				connection.setTypeMap(map);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public String nativeSQL(final String sql) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.nativeSQL(sql);
			}
		};
		
		return (String) this.executeGet(operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public CallableStatement prepareCall(final String sql) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareCall(sql);
			}
		};
		
		return this.isReadOnly() ? (CallableStatement) this.executeRead(operation) : new CallableStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? (CallableStatement) this.executeRead(operation) : new CallableStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? (CallableStatement) this.executeRead(operation) : new CallableStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public PreparedStatement prepareStatement(final String sql) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql);
			}
		};
		
		return this.isReadOnly() ? (PreparedStatement) this.executeRead(operation) : new PreparedStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, autoGeneratedKeys);
			}
		};
		
		return this.isReadOnly() ? (PreparedStatement) this.executeRead(operation) : new PreparedStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? (PreparedStatement) this.executeRead(operation) : new PreparedStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? (PreparedStatement) this.executeRead(operation) : new PreparedStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnIndexes);
			}
		};
		
		return this.isReadOnly() ? (PreparedStatement) this.executeRead(operation) : new PreparedStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnNames);
			}
		};
		
		return this.isReadOnly() ? (PreparedStatement) this.executeRead(operation) : new PreparedStatementProxy(this, operation);
	}

	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public Savepoint setSavepoint(final String name) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, Connection connection) throws SQLException
			{
				return connection.setSavepoint(name);
			}
		};
		
		return new SavepointProxy(this, operation);
	}
}
