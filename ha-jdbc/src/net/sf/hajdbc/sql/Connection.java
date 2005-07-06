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
 * @since   1.0
 */
public class Connection extends SQLObject implements java.sql.Connection
{
	private FileSupport fileSupport;
	
	/**
	 * Constructs a new ConnectionProxy.
	 * @param object a proxy to an object that can create connections
	 * @param operation an operation that will create Connections
	 * @param fileSupport a file support object
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public Connection(SQLObject object, Operation operation, FileSupport fileSupport) throws java.sql.SQLException
	{
		super(object, operation);
		
		this.fileSupport = fileSupport;
	}
	
	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement();
			}
		};
		
		return this.isReadOnly() ? (java.sql.Statement) this.executeReadFromDriver(operation) : new Statement(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? (java.sql.Statement) this.executeReadFromDriver(operation) : new Statement(this, operation);
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? (java.sql.Statement) this.executeReadFromDriver(operation) : new Statement(this, operation);
	}

	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return new Boolean(connection.getAutoCommit());
			}
		};
		
		return ((Boolean) this.executeReadFromDriver(operation)).booleanValue();
	}

	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getCatalog();
			}
		};
		
		return (String) this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return new Integer(connection.getHoldability());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getMetaData();
			}
		};
		
		return (DatabaseMetaData) this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return new Integer(connection.getTransactionIsolation());
			}
		};
		
		return ((Integer) this.executeReadFromDatabase(operation)).intValue();
	}

	/**
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map getTypeMap() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getTypeMap();
			}
		};
		
		return (Map) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.getWarnings();
			}
		};
		
		return (SQLWarning) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return new Boolean(connection.isClosed());
			}
		};
		
		return ((Boolean) this.executeReadFromDriver(operation)).booleanValue();
	}

	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly() throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return new Boolean(connection.isReadOnly());
			}
		};
		
		return ((Boolean) this.executeReadFromDriver(operation)).booleanValue();
	}

	/**
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public String nativeSQL(final String sql) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.nativeSQL(sql);
			}
		};
		
		return (String) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public java.sql.CallableStatement prepareCall(final String sql) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql);
			}
		};
		
		return this.isReadOnly() ? (java.sql.CallableStatement) this.executeReadFromDatabase(operation) : new CallableStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? (java.sql.CallableStatement) this.executeReadFromDatabase(operation) : new CallableStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? (java.sql.CallableStatement) this.executeReadFromDatabase(operation) : new CallableStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql);
			}
		};
		
		return this.isReadOnly() ? (java.sql.PreparedStatement) this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, autoGeneratedKeys);
			}
		};
		
		return this.isReadOnly() ? (java.sql.PreparedStatement) this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
			}
		};
		
		return this.isReadOnly() ? (java.sql.PreparedStatement) this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
			}
		};
		
		return this.isReadOnly() ? (java.sql.PreparedStatement) this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnIndexes);
			}
		};
		
		return this.isReadOnly() ? (java.sql.PreparedStatement) this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	public java.sql.PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException
	{
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection.prepareStatement(sql, columnNames);
			}
		};
		
		return this.isReadOnly() ? (java.sql.PreparedStatement) this.executeReadFromDatabase(operation) : new PreparedStatement(this, operation);
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException
	{
		final Savepoint savepointProxy = (Savepoint) savepoint;
		
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				java.sql.Savepoint savepoint = (java.sql.Savepoint) savepointProxy.getObject(database);
				
				connection.releaseSavepoint(savepoint);
				
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
	public void rollback(java.sql.Savepoint savepoint) throws SQLException
	{
		final Savepoint proxy = (Savepoint) savepoint;
		
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				java.sql.Savepoint savepoint = (java.sql.Savepoint) proxy.getObject(database);
				
				connection.rollback(savepoint);
				
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
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
