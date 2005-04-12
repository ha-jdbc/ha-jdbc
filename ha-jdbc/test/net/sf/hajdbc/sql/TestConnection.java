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

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SynchronizationStrategy;

public class TestConnection
{
	private Connection connection;
	
	protected void setUp() throws Exception
	{
		
		Database database = new Database()
		{
			public String getId()
			{
				return null;
			}

			public java.sql.Connection connect(Object connectionFactory) throws SQLException
			{
				return null;
			}

			public Object createConnectionFactory() throws SQLException
			{
				return null;
			}

			public Integer getWeight()
			{
				return null;
			}
			
		};
		
		ConnectionFactory factory = new ConnectionFactory(null, null);
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return new MockConnection();
			}
		};
		
		this.connection = new Connection(factory, operation);
	}

	public void testConnection()
	{
	}

	public void testGetHoldability()
	{
	}

	public void testGetTransactionIsolation()
	{
	}

	public void testClearWarnings()
	{
	}

	public void testClose()
	{
	}

	public void testCommit()
	{
	}

	public void testRollback()
	{
	}

	public void testGetAutoCommit()
	{
	}

	public void testIsClosed()
	{
	}

	public void testIsReadOnly()
	{
	}

	public void testSetHoldability()
	{
	}

	public void testSetTransactionIsolation()
	{
	}

	public void testSetAutoCommit()
	{
	}

	public void testSetReadOnly()
	{
	}

	public void testGetCatalog()
	{
	}

	public void testSetCatalog()
	{
	}

	public void testGetMetaData()
	{
	}

	public void testGetWarnings()
	{
	}

	public void testSetSavepoint()
	{
	}

	public void testReleaseSavepoint()
	{
	}

	public void testRollbackSavepoint()
	{
	}

	public void testCreateStatement()
	{
	}

	public void testCreateStatementintint()
	{
	}

	public void testCreateStatementintintint()
	{
	}

	public void testGetTypeMap()
	{
	}

	public void testSetTypeMap()
	{
	}

	public void testNativeSQL()
	{
	}

	public void testPrepareCallString()
	{
	}

	public void testPrepareCallStringintint()
	{
	}

	public void testPrepareCallStringintintint()
	{
	}

	public void testPrepareStatementString()
	{
	}

	public void testPrepareStatementStringint()
	{
	}

	public void testPrepareStatementStringintint()
	{
	}

	public void testPrepareStatementStringintintint()
	{
	}

	public void testPrepareStatementStringintArray()
	{
	}

	public void testPrepareStatementStringStringArray()
	{
	}

	public void testSetSavepointString()
	{
	}
}
