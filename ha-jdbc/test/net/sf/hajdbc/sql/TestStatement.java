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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;

public class TestStatement
{
	private Statement statement;
	private java.sql.Statement statement1;
	private java.sql.Statement statement2;
	
	/**
	 * @testng.configuration beforeTestMethod = "true"
	 */
	public void setUp() throws Exception
	{
		Class.forName("net.sf.hajdbc.sql.Driver");

		java.sql.Connection connection = DriverManager.getConnection("jdbc:ha-jdbc:cluster", "sa", "");
		
		java.sql.Statement statement = connection.createStatement();
		statement.execute("CREATE TEMP TABLE test (id INTEGER PRIMARY KEY)");
		statement.close();
		
		this.statement = (Statement) connection.createStatement();
		
		DatabaseCluster databaseCluster = DatabaseClusterFactory.getInstance().getDatabaseCluster("cluster");
		Database database1 = databaseCluster.getDatabase("database1");
		Database database2 = databaseCluster.getDatabase("database2");
		
		this.statement1 = (java.sql.Statement) this.statement.getObject(database1);
		this.statement2 = (java.sql.Statement) this.statement.getObject(database2);
	}

	/**
	 * @testng.configuration afterTestMethod = "true"
	 */
	public void tearDown() throws Exception
	{
		this.statement.close();
		
		java.sql.Connection connection = this.statement.getConnection();
		
		if (!connection.isClosed())
		{
			connection.close();
		}
	}

	private int countRows(java.sql.Statement statement) throws SQLException
	{
		java.sql.ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM test");
		
		resultSet.next();
		
		int rows = resultSet.getInt(1);
		
		resultSet.close();
		
		return rows;
	}
	
	/**
	 * @testng.test
	 */
	public void testAddBatch()
	{
		try
		{
			this.statement.addBatch("INSERT INTO test (id) VALUES (1)");
			this.statement.addBatch("INSERT INTO test (id) VALUES (2)");
			
			int count1 = this.countRows(this.statement1);
			int count2 = this.countRows(this.statement2);
			
			assert count1 == 0;
			assert count2 == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testCancel()
	{
		try
		{
			this.statement.cancel();
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testClearBatch()
	{
		try
		{
			this.statement.addBatch("INSERT INTO test (id) VALUES (1)");
			this.statement.addBatch("INSERT INTO test (id) VALUES (2)");
			this.statement.clearBatch();
			this.statement.addBatch("INSERT INTO test (id) VALUES (3)");
			int[] counts = this.statement.executeBatch();
			
			assert counts.length == 1;
			assert counts[0] == 1;
			
			int count1 = this.countRows(this.statement1);
			int count2 = this.countRows(this.statement2);
			
			assert count1 == 1;
			assert count2 == 1;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testClearWarnings()
	{
		try
		{
			this.statement.clearWarnings();
		}
		catch (java.sql.SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testClose()
	{
		try
		{
			this.statement.close();
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testExecute()
	{
		try
		{
			this.statement.execute("INSERT INTO test (id) VALUES (1)");
			
			int count1 = this.countRows(this.statement1);
			int count2 = this.countRows(this.statement2);
			
			assert count1 == 1;
			assert count2 == 1;
			
			try
			{
				this.statement.execute("INSERT INTO test (id) VALUES (1)", 1);
				
				assert false : "HSQLDB does not support execute(String, int)";
			}
			catch (SQLException e)
			{
				assert true;
			}

			try
			{
				this.statement.execute("INSERT INTO test (id) VALUES (1)", new int[] { 1 });
				
				assert false : "HSQLDB does not support execute(String, int[])";
			}
			catch (SQLException e)
			{
				assert true;
			}

			try
			{
				this.statement.execute("INSERT INTO test (id) VALUES (1)", new String[] { "id" });
				
				assert false : "HSQLDB does not support execute(String, String[])";
			}
			catch (SQLException e)
			{
				assert true;
			}
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testExecuteBatch()
	{
		try
		{
			this.statement.addBatch("INSERT INTO test (id) VALUES (1)");
			this.statement.addBatch("INSERT INTO test (id) VALUES (2)");
			int[] counts = this.statement.executeBatch();
			
			assert counts.length == 2;
			assert counts[0] == 1;
			assert counts[1] == 1;
			
			int count1 = this.countRows(this.statement1);
			int count2 = this.countRows(this.statement2);
			
			assert count1 == 2;
			assert count2 == 2;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testExecuteQuery()
	{
		try
		{
			java.sql.ResultSet resultSet = this.statement.executeQuery("SELECT count(*) FROM test");
			
			assert !ResultSet.class.isInstance(resultSet);
			
			boolean next = resultSet.next();
			
			assert next;
			
			int count = resultSet.getInt(1);
			
			assert count == 0;
			
			next = resultSet.next();
			
			assert !next;
			
			resultSet.close();
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testExecuteUpdate()
	{
		try
		{
			int count = this.statement.executeUpdate("INSERT INTO test (id) VALUES (1)");
			
			assert count == 1;
			
			int count1 = this.countRows(this.statement1);
			int count2 = this.countRows(this.statement2);
			
			assert count1 == 1;
			assert count2 == 1;
			
			try
			{
				this.statement.executeUpdate("INSERT INTO test (id) VALUES (1)", 1);
				
				assert false : "HSQLDB does not support executeUpdate(String, int)";
			}
			catch (SQLException e)
			{
				assert true;
			}

			try
			{
				this.statement.executeUpdate("INSERT INTO test (id) VALUES (1)", new int[] { 1 });
				
				assert false : "HSQLDB does not support executeUpdate(String, int[])";
			}
			catch (SQLException e)
			{
				assert true;
			}

			try
			{
				this.statement.executeUpdate("INSERT INTO test (id) VALUES (1)", new String[] { "id" });
				
				assert false : "HSQLDB does not support executeUpdate(String, String[])";
			}
			catch (SQLException e)
			{
				assert true;
			}
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetConnection()
	{
		java.sql.Connection connection = this.statement.getConnection();
		
		assert Connection.class.isInstance(connection);
	}

	/**
	 * @testng.test
	 */
	public void testGetFetchDirection()
	{
		try
		{
			int fetchDirection = this.statement.getFetchDirection();
			
			assert fetchDirection == java.sql.ResultSet.FETCH_FORWARD;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetFetchSize()
	{
		try
		{
			int fetchSize = this.statement.getFetchSize();
			
			assert fetchSize == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetGeneratedKeys()
	{
		try
		{
			this.statement.getGeneratedKeys();
			
			assert false : "HSQLDB does not support getGeneratedKeys()";
		}
		catch (SQLException e)
		{
			assert true;
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetMaxFieldSize()
	{
		try
		{
			int size = this.statement.getMaxFieldSize();
			
			assert size == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetMaxRows()
	{
		try
		{
			int rows = this.statement.getMaxRows();
			
			assert rows == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetMoreResults()
	{
		try
		{
			this.statement.execute("SELECT count(*) FROM test; SELECT count(*) FROM test;");

			java.sql.ResultSet resultSet = this.statement.getResultSet();
			
			boolean next = resultSet.next();
			
			assert next;
			
			int count = resultSet.getInt(1);
			
			assert count == 0;
			
			boolean more = this.statement.getMoreResults();
			
			assert more : "Expected more results";

			resultSet = this.statement.getResultSet();
			
			next = resultSet.next();
			
			assert next;
			
			count = resultSet.getInt(1);
			
			assert count == 0;
			
			more = this.statement.getMoreResults();
			
			assert !more : "Expected no more results";
			
			try
			{
				this.statement.getMoreResults(1);
				
				assert false : "HSQLDB does not support getMoreResults(int)";
			}
			catch (SQLException e)
			{
				assert true;
			}
		}
		catch (SQLException e)
		{
			e.getMessage();
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetQueryTimeout()
	{
		try
		{
			int timeout = this.statement.getQueryTimeout();
			
			assert timeout == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetResultSet()
	{
		try
		{
			this.statement.execute("SELECT count(*) FROM test");
			
			java.sql.ResultSet resultSet = this.statement.getResultSet();
			
			assert !ResultSet.class.isInstance(resultSet);
			
			boolean next = resultSet.next();
			
			assert next;
			
			int count = resultSet.getInt(1);
			
			assert count == 0;
			
			next = resultSet.next();
			
			assert !next;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetResultSetConcurrency()
	{
		try
		{
			int concurrency = this.statement.getResultSetConcurrency();
			
			assert concurrency == java.sql.ResultSet.CONCUR_READ_ONLY;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetResultSetHoldability()
	{
		try
		{
			int holdability = this.statement.getResultSetHoldability();
			
			assert holdability == java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetResultSetType()
	{
		try
		{
			int type = this.statement.getResultSetType();
			
			assert type == java.sql.ResultSet.TYPE_FORWARD_ONLY;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetUpdateCount()
	{
		try
		{
			this.statement.execute("INSERT INTO test (id) VALUES (1)");
			
			int count = this.statement.getUpdateCount();
			
			assert count == 1;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetWarnings()
	{
		try
		{
			SQLWarning warnings = this.statement.getWarnings();
			
			assert warnings == null;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetCursorName()
	{
		try
		{
			this.statement.setCursorName("test");
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetEscapeProcessing()
	{
		try
		{
			this.statement.setEscapeProcessing(false);
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetFetchDirection()
	{
		try
		{
			this.statement.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD);
			
			int direction1 = this.statement1.getFetchDirection();
			int direction2 = this.statement2.getFetchDirection();
			
			assert direction1 == java.sql.ResultSet.FETCH_FORWARD;
			assert direction2 == java.sql.ResultSet.FETCH_FORWARD;
			
			try
			{
				this.statement.setFetchDirection(java.sql.ResultSet.FETCH_REVERSE);
				
				assert false : "HSQLDB does not support FETCH_REVERSE";
			}
			catch (java.sql.SQLException e)
			{
				assert true;
			}
			
			try
			{
				this.statement.setFetchDirection(java.sql.ResultSet.FETCH_UNKNOWN);
				
				assert false : "HSQLDB does not support FETCH_UNKNOWN";
			}
			catch (java.sql.SQLException e)
			{
				assert true;
			}
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetFetchSize()
	{
		try
		{
			this.statement.setFetchSize(1);
			
			int size1 = this.statement1.getFetchSize();
			int size2 = this.statement2.getFetchSize();
			
			assert size1 == 0;
			assert size2 == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetMaxFieldSize()
	{
		try
		{
			this.statement.setMaxFieldSize(100);
			
			int size1 = this.statement1.getMaxFieldSize();
			int size2 = this.statement2.getMaxFieldSize();
			
			assert size1 == 0;
			assert size2 == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetMaxRows()
	{
		try
		{
			this.statement.setMaxRows(100);
			
			int rows1 = this.statement1.getMaxRows();
			int rows2 = this.statement2.getMaxRows();
			
			assert rows1 == 100;
			assert rows2 == 100;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetQueryTimeout()
	{
		try
		{
			this.statement.setQueryTimeout(100);
			
			int timeout1 = this.statement1.getQueryTimeout();
			int timeout2 = this.statement2.getQueryTimeout();
			
			assert timeout1 == 0;
			assert timeout2 == 0;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}
}
