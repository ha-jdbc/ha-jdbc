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

public class MockConnection implements Connection
{
	public Statement createStatement() throws SQLException
	{
		return new MockStatement();
	}

	public PreparedStatement prepareStatement(String arg0) throws SQLException
	{
		return null;
	}

	public CallableStatement prepareCall(String arg0) throws SQLException
	{
		return null;
	}

	public String nativeSQL(String arg0) throws SQLException
	{
		return null;
	}

	public void setAutoCommit(boolean arg0) throws SQLException
	{
	}

	public boolean getAutoCommit() throws SQLException
	{
		return false;
	}

	public void commit() throws SQLException
	{
	}

	public void rollback() throws SQLException
	{
	}

	public void close() throws SQLException
	{
	}

	public boolean isClosed() throws SQLException
	{
		return false;
	}

	public DatabaseMetaData getMetaData() throws SQLException
	{
		return null;
	}

	public void setReadOnly(boolean arg0) throws SQLException
	{
	}

	public boolean isReadOnly() throws SQLException
	{
		return false;
	}

	public void setCatalog(String arg0) throws SQLException
	{
	}

	public String getCatalog() throws SQLException
	{
		return null;
	}

	public void setTransactionIsolation(int arg0) throws SQLException
	{
	}

	public int getTransactionIsolation() throws SQLException
	{
		return 0;
	}

	public SQLWarning getWarnings() throws SQLException
	{
		return null;
	}

	public void clearWarnings() throws SQLException
	{
	}

	public Statement createStatement(int arg0, int arg1) throws SQLException
	{
		return null;
	}

	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
	{
		return null;
	}

	public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException
	{
		return null;
	}

	public Map getTypeMap() throws SQLException
	{
		return null;
	}

	public void setTypeMap(Map arg0) throws SQLException
	{
	}

	public void setHoldability(int arg0) throws SQLException
	{
	}

	public int getHoldability() throws SQLException
	{
		return 0;
	}

	public Savepoint setSavepoint() throws SQLException
	{
		return null;
	}

	public Savepoint setSavepoint(String arg0) throws SQLException
	{
		return null;
	}

	public void rollback(Savepoint arg0) throws SQLException
	{
	}

	public void releaseSavepoint(Savepoint arg0) throws SQLException
	{
	}

	public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException
	{
		return null;
	}

	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
	{
		return null;
	}

	public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException
	{
		return null;
	}

	public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException
	{
		return null;
	}

	public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException
	{
		return null;
	}

	public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException
	{
		return null;
	}

	private class MockStatement implements Statement
	{
		public ResultSet executeQuery(String arg0) throws SQLException
		{
			return null;
		}

		public int executeUpdate(String arg0) throws SQLException
		{
			return 0;
		}

		public void close() throws SQLException
		{
		}

		public int getMaxFieldSize() throws SQLException
		{
			return 0;
		}

		public void setMaxFieldSize(int arg0) throws SQLException
		{
		}

		public int getMaxRows() throws SQLException
		{
			return 0;
		}

		public void setMaxRows(int arg0) throws SQLException
		{
		}

		public void setEscapeProcessing(boolean arg0) throws SQLException
		{
		}

		public int getQueryTimeout() throws SQLException
		{
			return 0;
		}

		public void setQueryTimeout(int arg0) throws SQLException
		{
		}

		public void cancel() throws SQLException
		{
		}

		public SQLWarning getWarnings() throws SQLException
		{
			return null;
		}

		public void clearWarnings() throws SQLException
		{
		}

		public void setCursorName(String arg0) throws SQLException
		{
		}

		public boolean execute(String arg0) throws SQLException
		{
			return false;
		}

		public ResultSet getResultSet() throws SQLException
		{
			return null;
		}

		public int getUpdateCount() throws SQLException
		{
			return 0;
		}

		public boolean getMoreResults() throws SQLException
		{
			return false;
		}

		public void setFetchDirection(int arg0) throws SQLException
		{
		}

		public int getFetchDirection() throws SQLException
		{
			return 0;
		}

		public void setFetchSize(int arg0) throws SQLException
		{
		}

		public int getFetchSize() throws SQLException
		{
			return 0;
		}

		public int getResultSetConcurrency() throws SQLException
		{
			return 0;
		}

		public int getResultSetType() throws SQLException
		{
			return 0;
		}

		public void addBatch(String arg0) throws SQLException
		{
		}

		public void clearBatch() throws SQLException
		{
		}

		public int[] executeBatch() throws SQLException
		{
			return null;
		}

		public Connection getConnection() throws SQLException
		{
			return null;
		}

		public boolean getMoreResults(int arg0) throws SQLException
		{
			return false;
		}

		public ResultSet getGeneratedKeys() throws SQLException
		{
			return null;
		}

		public int executeUpdate(String arg0, int arg1) throws SQLException
		{
			return 0;
		}

		public int executeUpdate(String arg0, int[] arg1) throws SQLException
		{
			return 0;
		}

		public int executeUpdate(String arg0, String[] arg1) throws SQLException
		{
			return 0;
		}

		public boolean execute(String arg0, int arg1) throws SQLException
		{
			return false;
		}

		public boolean execute(String arg0, int[] arg1) throws SQLException
		{
			return false;
		}

		public boolean execute(String arg0, String[] arg1) throws SQLException
		{
			return false;
		}

		public int getResultSetHoldability() throws SQLException
		{
			return 0;
		}
	}
}
