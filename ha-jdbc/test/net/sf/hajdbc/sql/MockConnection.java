/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2005 Paul Ferraro
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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;

/**
 * Mock connection that creates mock statements
 * @author  Paul Ferraro
 * @since   1.1
 */
public class MockConnection implements Connection
{
	/**
	 * @see java.sql.Connection#createStatement()
	 */
	public Statement createStatement() throws SQLException
	{
		return new MockStatement();
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public PreparedStatement prepareStatement(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public CallableStatement prepareCall(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public String nativeSQL(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(boolean arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#commit()
	 */
	public void commit() throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#close()
	 */
	public void close() throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	public void setReadOnly(boolean arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(String arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(int arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public Statement createStatement(int arg0, int arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map getTypeMap() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	public void setTypeMap(Map arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(int arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	public Savepoint setSavepoint() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public Savepoint setSavepoint(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(Savepoint arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(Savepoint arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException
	{
		return null;
	}

	private class MockStatement implements Statement
	{
		/**
		 * @see java.sql.Statement#executeQuery(java.lang.String)
		 */
		public ResultSet executeQuery(String arg0) throws SQLException
		{
			return null;
		}

		/**
		 * @see java.sql.Statement#executeUpdate(java.lang.String)
		 */
		public int executeUpdate(String arg0) throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#close()
		 */
		public void close() throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#getMaxFieldSize()
		 */
		public int getMaxFieldSize() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#setMaxFieldSize(int)
		 */
		public void setMaxFieldSize(int arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#getMaxRows()
		 */
		public int getMaxRows() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#setMaxRows(int)
		 */
		public void setMaxRows(int arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#setEscapeProcessing(boolean)
		 */
		public void setEscapeProcessing(boolean arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#getQueryTimeout()
		 */
		public int getQueryTimeout() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#setQueryTimeout(int)
		 */
		public void setQueryTimeout(int arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#cancel()
		 */
		public void cancel() throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#getWarnings()
		 */
		public SQLWarning getWarnings() throws SQLException
		{
			return null;
		}

		/**
		 * @see java.sql.Statement#clearWarnings()
		 */
		public void clearWarnings() throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#setCursorName(java.lang.String)
		 */
		public void setCursorName(String arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#execute(java.lang.String)
		 */
		public boolean execute(String arg0) throws SQLException
		{
			return false;
		}

		/**
		 * @see java.sql.Statement#getResultSet()
		 */
		public ResultSet getResultSet() throws SQLException
		{
			return null;
		}

		/**
		 * @see java.sql.Statement#getUpdateCount()
		 */
		public int getUpdateCount() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#getMoreResults()
		 */
		public boolean getMoreResults() throws SQLException
		{
			return false;
		}

		/**
		 * @see java.sql.Statement#setFetchDirection(int)
		 */
		public void setFetchDirection(int arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#getFetchDirection()
		 */
		public int getFetchDirection() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#setFetchSize(int)
		 */
		public void setFetchSize(int arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#getFetchSize()
		 */
		public int getFetchSize() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#getResultSetConcurrency()
		 */
		public int getResultSetConcurrency() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#getResultSetType()
		 */
		public int getResultSetType() throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#addBatch(java.lang.String)
		 */
		public void addBatch(String arg0) throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#clearBatch()
		 */
		public void clearBatch() throws SQLException
		{
		}

		/**
		 * @see java.sql.Statement#executeBatch()
		 */
		public int[] executeBatch() throws SQLException
		{
			return null;
		}

		/**
		 * @see java.sql.Statement#getConnection()
		 */
		public Connection getConnection() throws SQLException
		{
			return null;
		}

		/**
		 * @see java.sql.Statement#getMoreResults(int)
		 */
		public boolean getMoreResults(int arg0) throws SQLException
		{
			return false;
		}

		/**
		 * @see java.sql.Statement#getGeneratedKeys()
		 */
		public ResultSet getGeneratedKeys() throws SQLException
		{
			return null;
		}

		/**
		 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
		 */
		public int executeUpdate(String arg0, int arg1) throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
		 */
		public int executeUpdate(String arg0, int[] arg1) throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
		 */
		public int executeUpdate(String arg0, String[] arg1) throws SQLException
		{
			return 0;
		}

		/**
		 * @see java.sql.Statement#execute(java.lang.String, int)
		 */
		public boolean execute(String arg0, int arg1) throws SQLException
		{
			return false;
		}

		/**
		 * @see java.sql.Statement#execute(java.lang.String, int[])
		 */
		public boolean execute(String arg0, int[] arg1) throws SQLException
		{
			return false;
		}

		/**
		 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
		 */
		public boolean execute(String arg0, String[] arg1) throws SQLException
		{
			return false;
		}

		/**
		 * @see java.sql.Statement#getResultSetHoldability()
		 */
		public int getResultSetHoldability() throws SQLException
		{
			return 0;
		}
	}
}
