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
import net.sf.hajdbc.SQLObject;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <P> 
 * @since   1.0
 */
public class Connection<P> extends SQLObject<java.sql.Connection, P> implements java.sql.Connection
{
	private FileSupport fileSupport;
	
	/**
	 * Constructs a new ConnectionProxy.
	 * @param object a proxy to an object that can create connections
	 * @param operation an operation that will create Connections
	 * @param fileSupport a file support object
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public Connection(SQLObject<P, ?> object, Operation<P, java.sql.Connection> operation, FileSupport fileSupport) throws java.sql.SQLException
	{
		super(object, operation);
		
		this.fileSupport = fileSupport;
	}
	
	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
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
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.close();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
		
		this.fileSupport.close();
	}

	/**
	 * @see java.sql.Connection#commit()
	 */
	public void commit() throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.commit();
				
				return null;
			}	
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#createStatement()
	 */
	public java.sql.Statement createStatement() throws SQLException
	{
		Operation<java.sql.Connection, java.sql.Statement> operation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement();
			}	
		};
		
		return this.isReadOnly() ? this.executeReadFromDriver(operation) : new Statement<java.sql.Statement>(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.Statement> operation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency);
			}	
		};
		
		return this.isReadOnly() ? this.executeReadFromDriver(operation) : new Statement<java.sql.Statement>(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.Statement> operation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
			}	
		};
		
		return this.isReadOnly() ? this.executeReadFromDriver(operation) : new Statement<java.sql.Statement>(this, operation);
	}

	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException
	{
		Operation<java.sql.Connection, Boolean> operation = new Operation<java.sql.Connection, Boolean>()
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
		Operation<java.sql.Connection, String> operation = new Operation<java.sql.Connection, String>()
		{
			public String execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getCatalog();
			}	
		};
		
		return this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException
	{
		Operation<java.sql.Connection, Integer> operation = new Operation<java.sql.Connection, Integer>()
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
		Operation<java.sql.Connection, DatabaseMetaData> operation = new Operation<java.sql.Connection, DatabaseMetaData>()
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
		Operation<java.sql.Connection, Integer> operation = new Operation<java.sql.Connection, Integer>()
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
	public Map getTypeMap() throws SQLException
	{
		Operation<java.sql.Connection, Map> operation = new Operation<java.sql.Connection, Map>()
		{
			public Map execute(Database database, java.sql.Connection connection) throws SQLException
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
		Operation<java.sql.Connection, SQLWarning> operation = new Operation<java.sql.Connection, SQLWarning>()
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
		Operation<java.sql.Connection, Boolean> operation = new Operation<java.sql.Connection, Boolean>()
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
		Operation<java.sql.Connection, Boolean> operation = new Operation<java.sql.Connection, Boolean>()
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
		Operation<java.sql.Connection, String> operation = new Operation<java.sql.Connection, String>()
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
		Operation<java.sql.Connection, java.sql.CallableStatement> operation = new Operation<java.sql.Connection, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new CallableStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.CallableStatement> operation = new Operation<java.sql.Connection, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new CallableStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.CallableStatement> operation = new Operation<java.sql.Connection, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new CallableStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, autoGeneratedKeys);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnIndexes);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnNames);
			}
		};
		
		return this.isReadOnly() ? this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation, sql);
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(final java.sql.Savepoint savepoint) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.releaseSavepoint(Savepoint.class.cast(savepoint));
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.rollback();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(final java.sql.Savepoint savepoint) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.rollback(Savepoint.class.cast(savepoint));
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(final boolean autoCommit) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setAutoCommit(autoCommit);
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);

		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(final String catalog) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setCatalog(catalog);
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(final int holdability) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
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
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
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
		Operation<java.sql.Connection, java.sql.Savepoint> operation = new Operation<java.sql.Connection, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.setSavepoint();
			}
		};
		
		return new Savepoint(this, operation);
	}

	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public java.sql.Savepoint setSavepoint(final String name) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.Savepoint> operation = new Operation<java.sql.Connection, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.setSavepoint(name);
			}
		};
		
		return new Savepoint(this, operation);
	}

	/**
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(final int transactionIsolation) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
		{
			public Void execute(Database database, java.sql.Connection connection) throws SQLException
			{
				connection.setTransactionIsolation(transactionIsolation);
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	public void setTypeMap(final Map map) throws SQLException
	{
		Operation<java.sql.Connection, Void> operation = new Operation<java.sql.Connection, Void>()
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
}
