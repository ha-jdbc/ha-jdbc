/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <D> SQL object that created this connection - either java.sql.Driver or javax.sql.DataSource
 * @since   1.0
 */
public class Connection<D> extends SQLObject<D, java.sql.Connection, D> implements java.sql.Connection
{
	private FileSupport fileSupport;
	
	/**
	 * Constructs a new ConnectionProxy.
	 * @param connectionFactory a proxy to an object that can create connections
	 * @param operation an operation that will create Connections
	 * @param fileSupport a file support object
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public Connection(ConnectionFactory<D> connectionFactory, Operation<D, D, java.sql.Connection> operation, FileSupport fileSupport) throws java.sql.SQLException
	{
		super(connectionFactory, operation, connectionFactory.getDatabaseCluster().getNonTransactionalExecutor());
		
		this.fileSupport = fileSupport;
	}
	
	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database<D> database, java.sql.Connection connection) throws SQLException
			{
				connection.clearWarnings();
				
				return null;
			}	
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Connection#close()
	 */
	public void close() throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database<D> database, java.sql.Connection connection) throws SQLException
			{
				connection.close();
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
		
		this.fileSupport.close();
	}

	/**
	 * @see java.sql.Connection#commit()
	 */
	public void commit() throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.commit();
				
				return null;
			}	
		};
		
		this.executeTransactionalWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#createStatement()
	 */
	public java.sql.Statement createStatement() throws SQLException
	{
		ConnectionOperation<D, java.sql.Statement> operation = new ConnectionOperation<D, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement();
			}	
		};
		
		return this.isReadOnly() ? this.executeReadFromDriver(operation) : new Statement<D>(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation<D, java.sql.Statement> operation = new ConnectionOperation<D, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency);
			}	
		};
		
		return this.isReadOnly() ? this.executeReadFromDriver(operation) : new Statement<D>(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation<D, java.sql.Statement> operation = new ConnectionOperation<D, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
			}	
		};
		
		return this.isReadOnly() ? this.executeReadFromDriver(operation) : new Statement<D>(this, operation);
	}

	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException
	{
		ConnectionOperation<D, Boolean> operation = new ConnectionOperation<D, Boolean>()
		{
			public Boolean execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getAutoCommit();
			}	
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog() throws SQLException
	{
		ConnectionOperation<D, String> operation = new ConnectionOperation<D, String>()
		{
			public String execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getCatalog();
			}	
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException
	{
		ConnectionOperation<D, Integer> operation = new ConnectionOperation<D, Integer>()
		{
			public Integer execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getHoldability();
			}	
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException
	{
		ConnectionOperation<D, DatabaseMetaData> operation = new ConnectionOperation<D, DatabaseMetaData>()
		{
			public DatabaseMetaData execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getMetaData();
			}	
		};
		
		return this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException
	{
		ConnectionOperation<D, Integer> operation = new ConnectionOperation<D, Integer>()
		{
			public Integer execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getTransactionIsolation();
			}	
		};
		
		return this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		ConnectionOperation<D, Map<String, Class<?>>> operation = new ConnectionOperation<D, Map<String, Class<?>>>()
		{
			public Map<String, Class<?>> execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getTypeMap();
			}	
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		ConnectionOperation<D, SQLWarning> operation = new ConnectionOperation<D, SQLWarning>()
		{
			public SQLWarning execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getWarnings();
			}	
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException
	{
		ConnectionOperation<D, Boolean> operation = new ConnectionOperation<D, Boolean>()
		{
			public Boolean execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.isClosed();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly() throws SQLException
	{
		ConnectionOperation<D, Boolean> operation = new ConnectionOperation<D, Boolean>()
		{
			public Boolean execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.isReadOnly();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public String nativeSQL(final String sql) throws SQLException
	{
		ConnectionOperation<D, String> operation = new ConnectionOperation<D, String>()
		{
			public String execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.nativeSQL(sql);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public java.sql.CallableStatement prepareCall(final String sql) throws SQLException
	{
		ConnectionOperation<D, java.sql.CallableStatement> operation = new ConnectionOperation<D, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new CallableStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation<D, java.sql.CallableStatement> operation = new ConnectionOperation<D, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new CallableStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation<D, java.sql.CallableStatement> operation = new ConnectionOperation<D, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new CallableStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException
	{
		ConnectionOperation<D, java.sql.PreparedStatement> operation = new ConnectionOperation<D, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		ConnectionOperation<D, java.sql.PreparedStatement> operation = new ConnectionOperation<D, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, autoGeneratedKeys);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation<D, java.sql.PreparedStatement> operation = new ConnectionOperation<D, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation<D, java.sql.PreparedStatement> operation = new ConnectionOperation<D, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException
	{
		ConnectionOperation<D, java.sql.PreparedStatement> operation = new ConnectionOperation<D, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnIndexes);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException
	{
		ConnectionOperation<D, java.sql.PreparedStatement> operation = new ConnectionOperation<D, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnNames);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement<D>(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(final java.sql.Savepoint savepoint) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.releaseSavepoint(Savepoint.class.cast(savepoint));
				
				return null;
			}
		};
		
		this.executeTransactionalWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.rollback();
				
				return null;
			}
		};
		
		this.executeTransactionalWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(final java.sql.Savepoint savepoint) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.rollback(Savepoint.class.cast(savepoint));
				
				return null;
			}
		};
		
		this.executeTransactionalWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(final boolean autoCommit) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setAutoCommit(autoCommit);
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);

		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(final String catalog) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setCatalog(catalog);
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(final int holdability) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setHoldability(holdability);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	public void setReadOnly(final boolean readOnly) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setReadOnly(readOnly);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	public java.sql.Savepoint setSavepoint() throws SQLException
	{
		ConnectionOperation<D, java.sql.Savepoint> operation = new ConnectionOperation<D, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.setSavepoint();
			}
		};
		
		return new Savepoint<D>(this, operation);
	}

	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public java.sql.Savepoint setSavepoint(final String name) throws SQLException
	{
		ConnectionOperation<D, java.sql.Savepoint> operation = new ConnectionOperation<D, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.setSavepoint(name);
			}
		};
		
		return new Savepoint<D>(this, operation);
	}

	/**
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(final int transactionIsolation) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setTransactionIsolation(transactionIsolation);
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	public void setTypeMap(final Map<String, Class<?>> map) throws SQLException
	{
		ConnectionOperation<D, Void> operation = new ConnectionOperation<D, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setTypeMap(map);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}
	
	/**
	 * @return a file support object
	 */
	public FileSupport getFileSupport()
	{
		return this.fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLObject#close(java.lang.Object)
	 */
	@Override
	protected void close(java.sql.Connection connection) throws SQLException
	{
		connection.close();
	}
	
	private static interface ConnectionOperation<D, R> extends Operation<D, java.sql.Connection, R>
	{
		// No additional methods, just simplify the type parameters
	}
}
