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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.prefs.Preferences;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.local.LocalDatabaseCluster;

public class TestConnection
{
	public static void storedProcedure()
	{
		// Do nothing
	}
	
	private Connection connection;
	private java.sql.Connection connection1;
	private java.sql.Connection connection2;
	
	/**
	 * @testng.configuration beforeTestMethod = "true"
	 */
	public void setUp() throws Exception
	{
		Preferences.userNodeForPackage(LocalDatabaseCluster.class).remove("cluster");
		
		Class.forName("net.sf.hajdbc.sql.Driver");

		this.connection = (Connection) DriverManager.getConnection("jdbc:ha-jdbc:cluster", "sa", "");
		
		DatabaseCluster databaseCluster = DatabaseClusterFactory.getInstance().getDatabaseCluster("cluster");
		Database database1 = databaseCluster.getDatabase("database1");
		Database database2 = databaseCluster.getDatabase("database2");
		
		this.connection1 = (java.sql.Connection) this.connection.getObject(database1);
		this.connection2 = (java.sql.Connection) this.connection.getObject(database2);
	}

	/**
	 * @testng.configuration afterTestMethod = "true"
	 */
	public void tearDown() throws Exception
	{
		if (!this.connection.isClosed())
		{
			this.connection.close();
		}
	}

	private void createTable() throws SQLException
	{
		java.sql.Statement statement = this.connection.createStatement();
		statement.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)");
		statement.close();
	}
	
	private int countRows(java.sql.Connection connection) throws SQLException
	{
		Statement statement = this.connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM test");
		
		resultSet.next();
		
		int rows = resultSet.getInt(1);
		
		resultSet.close();
		statement.close();
		
		return rows;
	}
	
	/**
	 * @testng.test
	 */
	public void testClearWarnings()
	{
		try
		{
			this.connection.clearWarnings();
		}
		catch (SQLException e)
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
			boolean closed1 = this.connection1.isClosed();
			boolean closed2 = this.connection2.isClosed();

			assert !closed1;
			assert !closed2;
			
			this.connection.close();
			
			closed1 = this.connection1.isClosed();
			closed2 = this.connection2.isClosed();
			
			assert closed1;
			assert closed2;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testCommit()
	{
		try
		{
			this.createTable();
			
			this.connection.setAutoCommit(false);
			
			java.sql.Statement statement = this.connection.createStatement();
			
			statement.executeUpdate("INSERT INTO test (id) VALUES (1)");
			statement.close();
			
			this.connection.commit();
			
			int count1 = this.countRows(this.connection1);
			int count2 = this.countRows(this.connection2);
			
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
	public void testCreateStatement()
	{
		try
		{
			java.sql.Statement statement = this.connection.createStatement();
			int type = statement.getResultSetType();
			int concurrency = statement.getResultSetConcurrency();
			int holdability = statement.getResultSetHoldability();
			
			assert Statement.class.isInstance(statement);
			assert type == ResultSet.TYPE_FORWARD_ONLY;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
			
			statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			type = statement.getResultSetType();
			concurrency = statement.getResultSetConcurrency();
			holdability = statement.getResultSetHoldability();
			
			assert Statement.class.isInstance(statement);
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
			
			statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			type = statement.getResultSetType();
			concurrency = statement.getResultSetConcurrency();
			holdability = statement.getResultSetHoldability();
			
			assert Statement.class.isInstance(statement);
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetAutoCommit()
	{
		try
		{
			boolean autoCommit = this.connection.getAutoCommit();
			
			assert autoCommit;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetCatalog()
	{
		try
		{
			String catalog = this.connection.getCatalog();
			
			assert catalog == null;
		}
		catch (SQLException e)
		{
			assert false;
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetHoldability()
	{
		try
		{
			int holdability = this.connection.getHoldability();
			
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT : holdability;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetMetaData()
	{
		try
		{
			DatabaseMetaData metaData = this.connection.getMetaData();
			
			assert metaData != null;
		}
		catch (SQLException e)
		{
			assert false;
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetTransactionIsolation()
	{
		try
		{
			int isolation = this.connection.getTransactionIsolation();
			
			assert isolation == java.sql.Connection.TRANSACTION_READ_UNCOMMITTED : isolation;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetTypeMap()
	{
		try
		{
			this.connection.getTypeMap();
			
			assert false : "getTypeMap() is not supported in HSQL";
		}
		catch (SQLException e)
		{
			assert true;
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetWarnings()
	{
		try
		{
			SQLWarning warning = this.connection.getWarnings();
			
			assert warning == null;
		}
		catch (SQLException e)
		{
			assert false;
		}
	}

	/**
	 * @testng.test
	 */
	public void testIsClosed()
	{
		try
		{
			boolean closed = this.connection.isClosed();
			
			assert !closed;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testIsReadOnly()
	{
		try
		{
			boolean readOnly = this.connection.isReadOnly();
			
			assert !readOnly;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testNativeSQL()
	{
		try
		{
			String sql = "CALL NOW()";
			String nativeSQL = this.connection.nativeSQL(sql);
			
			assert nativeSQL.equals(sql);
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testPrepareCall()
	{
		try
		{
			java.sql.Statement stmt = this.connection.createStatement();
			stmt.execute("GRANT ALL ON CLASS \"" + this.getClass().getName() + "\" TO PUBLIC");
			stmt.close();

			String sql = "call storedProcedure";
			java.sql.CallableStatement statement = this.connection.prepareCall(sql);
			int type = statement.getResultSetType();
			int concurrency = statement.getResultSetConcurrency();
			int holdability = statement.getResultSetHoldability();
			
			assert CallableStatement.class.isInstance(statement);
			assert type == ResultSet.TYPE_FORWARD_ONLY;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
			
			statement = this.connection.prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			type = statement.getResultSetType();
			concurrency = statement.getResultSetConcurrency();
			holdability = statement.getResultSetHoldability();
			
			assert CallableStatement.class.isInstance(statement);
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
			
			statement = this.connection.prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			type = statement.getResultSetType();
			concurrency = statement.getResultSetConcurrency();
			holdability = statement.getResultSetHoldability();
			
			assert CallableStatement.class.isInstance(statement);
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testPrepareStatement()
	{
		try
		{
			String sql = "CALL NOW()";
			java.sql.PreparedStatement statement = this.connection.prepareStatement(sql);
			int type = statement.getResultSetType();
			int concurrency = statement.getResultSetConcurrency();
			int holdability = statement.getResultSetHoldability();
			
			assert PreparedStatement.class.isInstance(statement);
			assert type == ResultSet.TYPE_FORWARD_ONLY;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
			
			statement = this.connection.prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			type = statement.getResultSetType();
			concurrency = statement.getResultSetConcurrency();
			holdability = statement.getResultSetHoldability();
			
			assert PreparedStatement.class.isInstance(statement);
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
			
			statement = this.connection.prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			type = statement.getResultSetType();
			concurrency = statement.getResultSetConcurrency();
			holdability = statement.getResultSetHoldability();
			
			assert PreparedStatement.class.isInstance(statement);
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE;
			assert concurrency == ResultSet.CONCUR_READ_ONLY;
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testReleaseSavepoint()
	{
		try
		{
			this.createTable();
			
			this.connection.setAutoCommit(false);
			
			java.sql.Statement statement = this.connection.createStatement();
			
			statement.executeUpdate("INSERT INTO test (id) VALUES (1)");

			java.sql.Savepoint savepoint = this.connection.setSavepoint("savepoint1");
			
			assert Savepoint.class.isInstance(savepoint);
			
			statement.executeUpdate("INSERT INTO test (id) VALUES (2)");

			statement.close();
			
			this.connection.releaseSavepoint(savepoint);
			
			this.connection.commit();
			
			int count1 = this.countRows(this.connection1);
			int count2 = this.countRows(this.connection2);
			
			assert count1 == 2 : count1;
			assert count2 == 2 : count2;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testRollback()
	{
		try
		{
			this.createTable();
			
			this.connection.setAutoCommit(false);
			
			java.sql.Statement statement = this.connection.createStatement();
			
			statement.executeUpdate("INSERT INTO test (id) VALUES (1)");
			statement.close();
			
			this.connection.rollback();
			
			int count1 = this.countRows(this.connection1);
			int count2 = this.countRows(this.connection2);
			
			assert count1 == 0 : count1;
			assert count2 == 0 : count2;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testRollbackSavepoint()
	{
		try
		{
			this.createTable();
			
			this.connection.setAutoCommit(false);
			
			java.sql.Statement statement = this.connection.createStatement();
			
			statement.executeUpdate("INSERT INTO test (id) VALUES (1)");

			java.sql.Savepoint savepoint = this.connection.setSavepoint("savepoint1");
			
			statement.executeUpdate("INSERT INTO test (id) VALUES (2)");

			statement.close();
			
			this.connection.rollback(savepoint);
			
			this.connection.commit();
			
			int count1 = this.countRows(this.connection1);
			int count2 = this.countRows(this.connection2);
			
			assert count1 == 1;
			assert count2 == 1;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetAutoCommit()
	{
		try
		{
			boolean autoCommit1 = this.connection1.getAutoCommit();
			boolean autoCommit2 = this.connection2.getAutoCommit();
			
			assert autoCommit1;
			assert autoCommit2;

			this.connection.setAutoCommit(false);
			
			autoCommit1 = this.connection1.getAutoCommit();
			autoCommit2 = this.connection2.getAutoCommit();
			
			assert !autoCommit1;
			assert !autoCommit2;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetCatalog()
	{
		try
		{
			this.connection.setCatalog("catalog");
		}
		catch (SQLException e)
		{
			assert false;
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetHoldability()
	{
		try
		{
			this.connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			
			assert false : "CLOSE_CURSORS_AT_COMMIT holdability is not supported in HSQL";
		}
		catch (SQLException e)
		{
			assert true;
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetReadOnly()
	{
		try
		{
			boolean readOnly1 = this.connection1.isReadOnly();
			boolean readOnly2 = this.connection2.isReadOnly();

			assert !readOnly1;
			assert !readOnly2;
			
			this.connection.setReadOnly(true);
			
			readOnly1 = this.connection1.isReadOnly();
			readOnly2 = this.connection2.isReadOnly();

			assert readOnly1;
			assert readOnly2;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetSavepoint()
	{
		try
		{
			this.connection.setAutoCommit(false);

			try
			{
				this.connection.setSavepoint();
				
				assert false : "setSavepoint() is not supported in HSQL";
			}
			catch (SQLException e)
			{
				assert true;
			}
			
			java.sql.Savepoint savepoint = this.connection.setSavepoint("savepoint1");
			
			assert Savepoint.class.isInstance(savepoint);
		}
		catch (SQLException ex)
		{
			assert false : ex.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testSetTransactionIsolation()
	{
		try
		{
			this.connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_NONE);
			
			assert false : "TRANSACTION_NONE is not supported in HSQL";
		}
		catch (SQLException e)
		{
			assert true;
		}
	}
	
	/**
	 * @testng.test
	 */
	public void testSetTypeMap()
	{
		try
		{
			this.connection.setTypeMap(null);
			
			assert false : "setTypeMap() is not supported in HSQL";
		}
		catch (SQLException e)
		{
			assert true;
		}
	}
}
