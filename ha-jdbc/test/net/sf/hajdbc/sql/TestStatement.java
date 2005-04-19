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

import java.sql.Connection;
import java.sql.DriverManager;

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

		Connection connection = DriverManager.getConnection("jdbc:ha-jdbc:cluster", "sa", "");
		
		
		
		this.statement = (Statement) connection.createStatement();
		
		DatabaseCluster databaseCluster = DatabaseClusterFactory.getInstance().getDatabaseCluster("cluster");
		Database database1 = databaseCluster.getDatabase("database1");
		Database database2 = databaseCluster.getDatabase("database2");
		
		this.statement1 = (java.sql.Statement) this.statement.getObject(database1);
		this.statement2 = (java.sql.Statement) this.statement.getObject(database2);
	}

	public void testAddBatch()
	{
	}

	public void testCancel()
	{
	}

	public void testClearBatch()
	{
	}

	public void testClearWarnings()
	{
	}

	public void testClose()
	{
	}

	/*
	 * Class under test for boolean execute(String)
	 */
	public void testExecuteString()
	{
	}

	/*
	 * Class under test for boolean execute(String, int)
	 */
	public void testExecuteStringint()
	{
	}

	/*
	 * Class under test for boolean execute(String, int[])
	 */
	public void testExecuteStringintArray()
	{
	}

	/*
	 * Class under test for boolean execute(String, String[])
	 */
	public void testExecuteStringStringArray()
	{
	}

	public void testExecuteBatch()
	{
	}

	public void testExecuteQuery()
	{
	}

	/*
	 * Class under test for int executeUpdate(String)
	 */
	public void testExecuteUpdateString()
	{
	}

	/*
	 * Class under test for int executeUpdate(String, int)
	 */
	public void testExecuteUpdateStringint()
	{
	}

	/*
	 * Class under test for int executeUpdate(String, int[])
	 */
	public void testExecuteUpdateStringintArray()
	{
	}

	/*
	 * Class under test for int executeUpdate(String, String[])
	 */
	public void testExecuteUpdateStringStringArray()
	{
	}

	public void testGetConnection()
	{
	}

	public void testGetFetchDirection()
	{
	}

	public void testGetFetchSize()
	{
	}

	public void testGetGeneratedKeys()
	{
	}

	public void testGetMaxFieldSize()
	{
	}

	public void testGetMaxRows()
	{
	}

	/*
	 * Class under test for boolean getMoreResults()
	 */
	public void testGetMoreResults()
	{
	}

	/*
	 * Class under test for boolean getMoreResults(int)
	 */
	public void testGetMoreResultsint()
	{
	}

	public void testGetQueryTimeout()
	{
	}

	public void testGetResultSet()
	{
	}

	public void testGetResultSetConcurrency()
	{
	}

	public void testGetResultSetHoldability()
	{
	}

	public void testGetResultSetType()
	{
	}

	public void testGetUpdateCount()
	{
	}

	public void testGetWarnings()
	{
	}

	public void testSetCursorName()
	{
	}

	public void testSetEscapeProcessing()
	{
	}

	public void testSetFetchDirection()
	{
	}

	public void testSetFetchSize()
	{
	}

	public void testSetMaxFieldSize()
	{
	}

	public void testSetMaxRows()
	{
	}

	public void testSetQueryTimeout()
	{
	}

}
