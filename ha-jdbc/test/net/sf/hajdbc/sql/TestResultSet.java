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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.AbstractTestCase;

import org.easymock.MockControl;

public class TestResultSet extends AbstractTestCase
{
	private MockControl databaseClusterControl = this.createControl(DatabaseCluster.class);
	private DatabaseCluster databaseCluster = (DatabaseCluster) this.databaseClusterControl.getMock();
	
	private MockControl sqlResultSetControl = this.createControl(java.sql.ResultSet.class);
	private java.sql.ResultSet sqlResultSet = (java.sql.ResultSet) this.sqlResultSetControl.getMock();
	
	private MockControl databaseControl = this.createControl(Database.class);
	private Database database = (Database) this.databaseControl.getMock();
	
	private MockControl balancerControl = this.createControl(Balancer.class);
	private Balancer balancer = (Balancer) this.balancerControl.getMock();
	
	private MockControl fileSupportControl = this.createControl(FileSupport.class);
	private FileSupport fileSupport = (FileSupport) this.fileSupportControl.getMock();
	
	private Statement statement;
	private ResultSet resultSet;
	private Database[] databases = new Database[] { this.database };
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 6);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 6);
		
		this.replay();
		
		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster, Collections.singletonMap(this.database, new Object()));
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return MockControl.createControl(java.sql.Connection.class).getMock();
			}
		};
		
		Connection connection = new Connection(connectionFactory, operation, this.fileSupport);
		
		ConnectionOperation connectionOperation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return MockControl.createControl(java.sql.Statement.class).getMock();
			}
		};
		
		this.statement = new Statement(connection, connectionOperation);
		
		StatementOperation statementOperation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return sqlResultSet;
			}
		};
		
		this.resultSet = new ResultSet(this.statement, statementOperation);
		
		this.verify();
		this.reset();
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.absolute(int)'
	 */
	public void testAbsolute()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.absolute(10);
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean valid = this.resultSet.absolute(10);
			
			verify();
			
			assertTrue(valid);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.afterLast()'
	 */
	public void testAfterLast()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.afterLast();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.afterLast();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.beforeFirst()'
	 */
	public void testBeforeFirst()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.beforeFirst();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.beforeFirst();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.cancelRowUpdates()'
	 */
	public void testCancelRowUpdates()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.cancelRowUpdates();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.cancelRowUpdates();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.clearWarnings()'
	 */
	public void testClearWarnings()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.clearWarnings();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.clearWarnings();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.close()'
	 */
	public void testClose()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.close();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.close();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.deleteRow()'
	 */
	public void testDeleteRow()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.deleteRow();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.deleteRow();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.findColumn(String)'
	 */
	public void testFindColumn()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.findColumn("test");
			this.sqlResultSetControl.setReturnValue(1);
			
			replay();
			
			int column = this.resultSet.findColumn("test");
			
			verify();
			
			assertEquals(1, column);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.first()'
	 */
	public void testFirst()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.first();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean valid = this.resultSet.first();
			
			verify();
			
			assertTrue(valid);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getArray(int)'
	 */
	public void testGetArrayInt()
	{
		Array array = (Array) this.createMock(Array.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getArray(1);
			this.sqlResultSetControl.setReturnValue(array);
			
			replay();
			
			Array value = this.resultSet.getArray(1);
			
			verify();
			
			assertSame(array, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getArray(String)'
	 */
	public void testGetArrayString()
	{
		Array array = (Array) this.createMock(Array.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getArray("column");
			this.sqlResultSetControl.setReturnValue(array);
			
			replay();
			
			Array value = this.resultSet.getArray("column");
			
			verify();
			
			assertSame(array, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getAsciiStream(int)'
	 */
	public void testGetAsciiStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getAsciiStream(1);
			this.sqlResultSetControl.setReturnValue(inputStream);
			
			replay();
			
			InputStream value = this.resultSet.getAsciiStream(1);
			
			verify();
			
			assertSame(inputStream, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getAsciiStream(String)'
	 */
	public void testGetAsciiStreamString()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getAsciiStream("column");
			this.sqlResultSetControl.setReturnValue(inputStream);
			
			replay();
			
			InputStream value = this.resultSet.getAsciiStream("column");
			
			verify();
			
			assertSame(inputStream, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBigDecimal(int)'
	 */
	public void testGetBigDecimalInt()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBigDecimal(1);
			this.sqlResultSetControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.resultSet.getBigDecimal(1);
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBigDecimal(int, int)'
	 * @deprecated
	 */
	public void testGetBigDecimalIntInt()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBigDecimal(1, 0);
			this.sqlResultSetControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.resultSet.getBigDecimal(1, 0);
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBigDecimal(String)'
	 */
	public void testGetBigDecimalString()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBigDecimal("column");
			this.sqlResultSetControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.resultSet.getBigDecimal("column");
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBigDecimal(String, int)'
	 * @deprecated
	 */
	public void testGetBigDecimalStringInt()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBigDecimal("column", 0);
			this.sqlResultSetControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.resultSet.getBigDecimal("column", 0);
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBinaryStream(int)'
	 */
	public void testGetBinaryStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBinaryStream(1);
			this.sqlResultSetControl.setReturnValue(inputStream);
			
			replay();
			
			InputStream value = this.resultSet.getBinaryStream(1);
			
			verify();
			
			assertSame(inputStream, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBinaryStream(String)'
	 */
	public void testGetBinaryStreamString()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBinaryStream("column");
			this.sqlResultSetControl.setReturnValue(inputStream);
			
			replay();
			
			InputStream value = this.resultSet.getBinaryStream("column");
			
			verify();
			
			assertSame(inputStream, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBlob(int)'
	 */
	public void testGetBlobInt()
	{
		Blob blob = (Blob) this.createMock(Blob.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBlob(1);
			this.sqlResultSetControl.setReturnValue(blob);
			
			replay();
			
			Blob value = this.resultSet.getBlob(1);
			
			verify();
			
			assertSame(blob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBlob(String)'
	 */
	public void testGetBlobString()
	{
		Blob blob = (Blob) this.createMock(Blob.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBlob("column");
			this.sqlResultSetControl.setReturnValue(blob);
			
			replay();
			
			Blob value = this.resultSet.getBlob("column");
			
			verify();
			
			assertSame(blob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBoolean(int)'
	 */
	public void testGetBooleanInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBoolean(1);
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean value = this.resultSet.getBoolean(1);
			
			verify();
			
			assertTrue(true);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBoolean(String)'
	 */
	public void testGetBooleanString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getBoolean("column");
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean value = this.resultSet.getBoolean("column");
			
			verify();
			
			assertTrue(true);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getByte(int)'
	 */
	public void testGetByteInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getByte(1);
			this.sqlResultSetControl.setReturnValue(1);
			
			replay();
			
			byte value = this.resultSet.getByte(1);
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getByte(String)'
	 */
	public void testGetByteString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getByte("column");
			this.sqlResultSetControl.setReturnValue(1);
			
			replay();
			
			byte value = this.resultSet.getByte("column");
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBytes(int)'
	 */
	public void testGetBytesInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		byte[] bytes = new byte[] { 1 };
		
		try
		{
			this.sqlResultSet.getBytes(1);
			this.sqlResultSetControl.setReturnValue(bytes);
			
			replay();
			
			byte[] value = this.resultSet.getBytes(1);
			
			verify();
			
			assertSame(bytes, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getBytes(String)'
	 */
	public void testGetBytesString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		byte[] bytes = new byte[] { 1 };
		
		try
		{
			this.sqlResultSet.getBytes("column");
			this.sqlResultSetControl.setReturnValue(bytes);
			
			replay();
			
			byte[] value = this.resultSet.getBytes("column");
			
			verify();
			
			assertSame(bytes, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getCharacterStream(int)'
	 */
	public void testGetCharacterStreamInt()
	{
		Reader reader = new StringReader("");
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getCharacterStream(1);
			this.sqlResultSetControl.setReturnValue(reader);
			
			replay();
			
			Reader value = this.resultSet.getCharacterStream(1);
			
			verify();
			
			assertSame(reader, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getCharacterStream(String)'
	 */
	public void testGetCharacterStreamString()
	{
		Reader reader = new StringReader("");
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getCharacterStream("column");
			this.sqlResultSetControl.setReturnValue(reader);
			
			replay();
			
			Reader value = this.resultSet.getCharacterStream("column");
			
			verify();
			
			assertSame(reader, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getClob(int)'
	 */
	public void testGetClobInt()
	{
		Clob clob = (Clob) this.createMock(Clob.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getClob(1);
			this.sqlResultSetControl.setReturnValue(clob);
			
			replay();
			
			Clob value = this.resultSet.getClob(1);
			
			verify();
			
			assertSame(clob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getClob(String)'
	 */
	public void testGetClobString()
	{
		Clob clob = (Clob) this.createMock(Clob.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getClob("column");
			this.sqlResultSetControl.setReturnValue(clob);
			
			replay();
			
			Clob value = this.resultSet.getClob("column");
			
			verify();
			
			assertSame(clob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getConcurrency()'
	 */
	public void testGetConcurrency()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getConcurrency();
			this.sqlResultSetControl.setReturnValue(ResultSet.CONCUR_UPDATABLE);
			
			replay();
			
			int concurrency = this.resultSet.getConcurrency();
			
			verify();
			
			assertEquals(ResultSet.CONCUR_UPDATABLE, concurrency);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getCursorName()'
	 */
	public void testGetCursorName()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getCursorName();
			this.sqlResultSetControl.setReturnValue("test");
			
			replay();
			
			String cursor = this.resultSet.getCursorName();
			
			verify();
			
			assertEquals("test", cursor);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getDate(int)'
	 */
	public void testGetDateInt()
	{
		Date date = new Date(System.currentTimeMillis());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getDate(1);
			this.sqlResultSetControl.setReturnValue(date);
			
			replay();
			
			Date value = this.resultSet.getDate(1);
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getDate(int, Calendar)'
	 */
	public void testGetDateIntCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getDate(1, calendar);
			this.sqlResultSetControl.setReturnValue(date);
			
			replay();
			
			Date value = this.resultSet.getDate(1, calendar);
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getDate(String)'
	 */
	public void testGetDateString()
	{
		Date date = new Date(System.currentTimeMillis());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getDate("column");
			this.sqlResultSetControl.setReturnValue(date);
			
			replay();
			
			Date value = this.resultSet.getDate("column");
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getDate(String, Calendar)'
	 */
	public void testGetDateStringCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getDate("column", calendar);
			this.sqlResultSetControl.setReturnValue(date);
			
			replay();
			
			Date value = this.resultSet.getDate("column", calendar);
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getDouble(int)'
	 */
	public void testGetDoubleInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getDouble(1);
			this.sqlResultSetControl.setReturnValue(1.0);
			
			replay();
			
			double value = this.resultSet.getDouble(1);
			
			verify();
			
			assertEquals(1.0, value, 0.0);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getDouble(String)'
	 */
	public void testGetDoubleString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getDouble("column");
			this.sqlResultSetControl.setReturnValue(1.0);
			
			replay();
			
			double value = this.resultSet.getDouble("column");
			
			verify();
			
			assertEquals(1.0, value, 0.0);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getFetchDirection()'
	 */
	public void testGetFetchDirection()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getFetchDirection();
			this.sqlResultSetControl.setReturnValue(ResultSet.FETCH_REVERSE);
			
			replay();
			
			int direction = this.resultSet.getFetchDirection();
			
			verify();
			
			assertEquals(ResultSet.FETCH_REVERSE, direction);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getFetchSize()'
	 */
	public void testGetFetchSize()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getFetchSize();
			this.sqlResultSetControl.setReturnValue(100);
			
			replay();
			
			int size = this.resultSet.getFetchSize();
			
			verify();
			
			assertEquals(100, size);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getFloat(int)'
	 */
	public void testGetFloatInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getFloat(1);
			this.sqlResultSetControl.setReturnValue(1.0f);
			
			replay();
			
			float value = this.resultSet.getFloat(1);
			
			verify();
			
			assertEquals(1.0f, value, 0.0f);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getFloat(String)'
	 */
	public void testGetFloatString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getFloat("column");
			this.sqlResultSetControl.setReturnValue(1.0f);
			
			replay();
			
			float value = this.resultSet.getFloat("column");
			
			verify();
			
			assertEquals(1.0f, value, 0.0f);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getInt(int)'
	 */
	public void testGetIntInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getInt(1);
			this.sqlResultSetControl.setReturnValue(1);
			
			replay();
			
			int value = this.resultSet.getInt(1);
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getInt(String)'
	 */
	public void testGetIntString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getInt("column");
			this.sqlResultSetControl.setReturnValue(1);
			
			replay();
			
			int value = this.resultSet.getInt("column");
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getLong(int)'
	 */
	public void testGetLongInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getLong(1);
			this.sqlResultSetControl.setReturnValue(100L);
			
			replay();
			
			long value = this.resultSet.getLong(1);
			
			verify();
			
			assertEquals(100L, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getLong(String)'
	 */
	public void testGetLongString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getLong("column");
			this.sqlResultSetControl.setReturnValue(100L);
			
			replay();
			
			long value = this.resultSet.getLong("column");
			
			verify();
			
			assertEquals(100L, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getMetaData()'
	 */
	public void testGetMetaData()
	{
		ResultSetMetaData resultSetMetaData = (ResultSetMetaData) this.createMock(ResultSetMetaData.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.next();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlResultSet.getMetaData();
			this.sqlResultSetControl.setReturnValue(resultSetMetaData);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			replay();
			
			ResultSetMetaData metaData = this.resultSet.getMetaData();
			
			verify();
			
			assertSame(resultSetMetaData, metaData);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getObject(int)'
	 */
	public void testGetObjectInt()
	{
		Object object = new Object();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getObject(1);
			this.sqlResultSetControl.setReturnValue(object);
			
			replay();
			
			Object value = this.resultSet.getObject(1);
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getObject(int, Map)'
	 */
	public void testGetObjectIntMap()
	{
		Object object = new Object();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getObject(1, Collections.EMPTY_MAP);
			this.sqlResultSetControl.setReturnValue(object);
			
			replay();
			
			Object value = this.resultSet.getObject(1, Collections.EMPTY_MAP);
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getObject(String)'
	 */
	public void testGetObjectString()
	{
		Object object = new Object();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getObject("column");
			this.sqlResultSetControl.setReturnValue(object);
			
			replay();
			
			Object value = this.resultSet.getObject("column");
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getObject(String, Map)'
	 */
	public void testGetObjectStringMap()
	{
		Object object = new Object();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getObject("column", Collections.EMPTY_MAP);
			this.sqlResultSetControl.setReturnValue(object);
			
			replay();
			
			Object value = this.resultSet.getObject("column", Collections.EMPTY_MAP);
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getRef(int)'
	 */
	public void testGetRefInt()
	{
		Ref ref = (Ref) this.createMock(Ref.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getRef(1);
			this.sqlResultSetControl.setReturnValue(ref);
			
			replay();
			
			Ref value = this.resultSet.getRef(1);
			
			verify();
			
			assertSame(ref, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getRef(String)'
	 */
	public void testGetRefString()
	{
		Ref ref = (Ref) this.createMock(Ref.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getRef("column");
			this.sqlResultSetControl.setReturnValue(ref);
			
			replay();
			
			Ref value = this.resultSet.getRef("column");
			
			verify();
			
			assertSame(ref, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getRow()'
	 */
	public void testGetRow()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getRow();
			this.sqlResultSetControl.setReturnValue(10);
			
			replay();
			
			int row = this.resultSet.getRow();
			
			verify();
			
			assertEquals(10, row);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getShort(int)'
	 */
	public void testGetShortInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getShort(1);
			this.sqlResultSetControl.setReturnValue((short) 1);
			
			replay();
			
			short value = this.resultSet.getShort(1);
			
			verify();
			
			assertEquals((short) 1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getShort(String)'
	 */
	public void testGetShortString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getShort("column");
			this.sqlResultSetControl.setReturnValue((short) 1);
			
			replay();
			
			short value = this.resultSet.getShort("column");
			
			verify();
			
			assertEquals((short) 1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getStatement()'
	 */
	public void testGetStatement()
	{
		java.sql.Statement statement = this.resultSet.getStatement();
		
		assertSame(this.statement, statement);
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getString(int)'
	 */
	public void testGetStringInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getString(1);
			this.sqlResultSetControl.setReturnValue("test");
			
			replay();
			
			String value = this.resultSet.getString(1);
			
			verify();
			
			assertEquals("test", value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getString(String)'
	 */
	public void testGetStringString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getString("column");
			this.sqlResultSetControl.setReturnValue("test");
			
			replay();
			
			String value = this.resultSet.getString("column");
			
			verify();
			
			assertEquals("test", value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTime(int)'
	 */
	public void testGetTimeInt()
	{
		Time time = new Time(System.currentTimeMillis());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTime(1);
			this.sqlResultSetControl.setReturnValue(time);
			
			replay();
			
			Time value = this.resultSet.getTime(1);
			
			verify();
			
			assertEquals(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTime(int, Calendar)'
	 */
	public void testGetTimeIntCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTime(1, calendar);
			this.sqlResultSetControl.setReturnValue(time);
			
			replay();
			
			Time value = this.resultSet.getTime(1, calendar);
			
			verify();
			
			assertEquals(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTime(String)'
	 */
	public void testGetTimeString()
	{
		Time time = new Time(System.currentTimeMillis());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTime("column");
			this.sqlResultSetControl.setReturnValue(time);
			
			replay();
			
			Time value = this.resultSet.getTime("column");
			
			verify();
			
			assertEquals(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTime(String, Calendar)'
	 */
	public void testGetTimeStringCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTime("column", calendar);
			this.sqlResultSetControl.setReturnValue(time);
			
			replay();
			
			Time value = this.resultSet.getTime("column", calendar);
			
			verify();
			
			assertEquals(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTimestamp(int)'
	 */
	public void testGetTimestampInt()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTimestamp(1);
			this.sqlResultSetControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.resultSet.getTimestamp(1);
			
			verify();
			
			assertEquals(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTimestamp(int, Calendar)'
	 */
	public void testGetTimestampIntCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTimestamp(1, calendar);
			this.sqlResultSetControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.resultSet.getTimestamp(1, calendar);
			
			verify();
			
			assertEquals(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTimestamp(String)'
	 */
	public void testGetTimestampString()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTimestamp("column");
			this.sqlResultSetControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.resultSet.getTimestamp("column");
			
			verify();
			
			assertEquals(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getTimestamp(String, Calendar)'
	 */
	public void testGetTimestampStringCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getTimestamp("column", calendar);
			this.sqlResultSetControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.resultSet.getTimestamp("column", calendar);
			
			verify();
			
			assertEquals(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getType()'
	 */
	public void testGetType()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_SCROLL_INSENSITIVE);
			
			replay();
			
			int type = this.resultSet.getType();
			
			verify();
			
			assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, type);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getUnicodeStream(int)'
	 * @deprecated
	 */
	public void testGetUnicodeStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getUnicodeStream(1);
			this.sqlResultSetControl.setReturnValue(inputStream);
			
			replay();
			
			InputStream value = this.resultSet.getUnicodeStream(1);
			
			verify();
			
			assertSame(inputStream, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getUnicodeStream(String)'
	 * @deprecated
	 */
	public void testGetUnicodeStreamString()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getUnicodeStream("column");
			this.sqlResultSetControl.setReturnValue(inputStream);
			
			replay();
			
			InputStream value = this.resultSet.getUnicodeStream("column");
			
			verify();
			
			assertSame(inputStream, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getURL(int)'
	 */
	public void testGetURLInt()
	{
		try
		{
			URL url = new URL("http://www.google.com");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getURL(1);
			this.sqlResultSetControl.setReturnValue(url);
			
			replay();
			
			URL value = this.resultSet.getURL(1);
			
			verify();
			
			assertSame(url, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
		catch (MalformedURLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getURL(String)'
	 */
	public void testGetURLString()
	{
		try
		{
			URL url = new URL("http://www.google.com");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getURL("column");
			this.sqlResultSetControl.setReturnValue(url);
			
			replay();
			
			URL value = this.resultSet.getURL("column");
			
			verify();
			
			assertSame(url, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
		catch (MalformedURLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.getWarnings()'
	 */
	public void testGetWarnings()
	{
		try
		{
			SQLWarning warnings = new SQLWarning();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getWarnings();
			this.sqlResultSetControl.setReturnValue(warnings);
			
			replay();
			
			SQLWarning value = this.resultSet.getWarnings();
			
			verify();
			
			assertSame(warnings, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.insertRow()'
	 */
	public void testInsertRow()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.insertRow();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.insertRow();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.isAfterLast()'
	 */
	public void testIsAfterLast()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.isAfterLast();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean afterLast = this.resultSet.isAfterLast();
			
			verify();
			
			assertTrue(afterLast);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.isBeforeFirst()'
	 */
	public void testIsBeforeFirst()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.isBeforeFirst();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean beforeFirst = this.resultSet.isBeforeFirst();
			
			verify();
			
			assertTrue(beforeFirst);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.isFirst()'
	 */
	public void testIsFirst()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database, 2);
		
		try
		{
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.isFirst();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean first = this.resultSet.isFirst();
			
			verify();
			
			assertTrue(first);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.isFirst()'
	 */
	public void testScrollSensitiveIsFirst()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_SCROLL_SENSITIVE);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);
			
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlResultSet.isFirst();
			this.sqlResultSetControl.setReturnValue(true);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			replay();
			
			boolean first = this.resultSet.isFirst();
			
			verify();
			
			assertTrue(first);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.isLast()'
	 */
	public void testIsLast()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database, 2);
		
		try
		{
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.isLast();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean last = this.resultSet.isLast();
			
			verify();
			
			assertTrue(last);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.isLast()'
	 */
	public void testScrollSensitiveIsLast()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_SCROLL_SENSITIVE);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);
			
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlResultSet.isLast();
			this.sqlResultSetControl.setReturnValue(true);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			replay();
			
			boolean last = this.resultSet.isLast();
			
			verify();
			
			assertTrue(last);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.last()'
	 */
	public void testLast()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.last();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean valid = this.resultSet.last();
			
			verify();
			
			assertTrue(valid);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.moveToCurrentRow()'
	 */
	public void testMoveToCurrentRow()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.moveToCurrentRow();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.moveToCurrentRow();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.moveToInsertRow()'
	 */
	public void testMoveToInsertRow()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.moveToInsertRow();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.moveToInsertRow();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.next()'
	 */
	public void testNext()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.next();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean valid = this.resultSet.next();
			
			verify();
			
			assertTrue(valid);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.previous()'
	 */
	public void testPrevious()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.getType();
			this.sqlResultSetControl.setReturnValue(ResultSet.TYPE_FORWARD_ONLY);
			
			this.sqlResultSet.previous();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean valid = this.resultSet.previous();
			
			verify();
			
			assertTrue(valid);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.refreshRow()'
	 */
	public void testRefreshRow()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.refreshRow();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.refreshRow();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.relative(int)'
	 */
	public void testRelative()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.relative(10);
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean valid = this.resultSet.relative(10);
			
			verify();
			
			assertTrue(valid);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.rowDeleted()'
	 */
	public void testRowDeleted()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.rowDeleted();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean deleted = this.resultSet.rowDeleted();
			
			verify();
			
			assertTrue(deleted);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.rowInserted()'
	 */
	public void testRowInserted()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.rowUpdated();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean updated = this.resultSet.rowUpdated();
			
			verify();
			
			assertTrue(updated);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.rowUpdated()'
	 */
	public void testRowUpdated()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlResultSet.rowInserted();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean inserted = this.resultSet.rowInserted();
			
			verify();
			
			assertTrue(inserted);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.setFetchDirection(int)'
	 */
	public void testSetFetchDirection()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.setFetchSize(int)'
	 */
	public void testSetFetchSize()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlResultSet.setFetchSize(100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.setFetchSize(100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateArray(int, Array)'
	 */
	public void testUpdateArrayIntArray()
	{
		try
		{
			Array array = (Array) this.createMock(Array.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateArray(1, array);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateArray(1, array);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateArray(String, Array)'
	 */
	public void testUpdateArrayStringArray()
	{
		try
		{
			Array array = (Array) this.createMock(Array.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateArray("column", array);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateArray("column", array);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateAsciiStream(int, InputStream, int)'
	 */
	public void testUpdateAsciiStreamIntInputStreamInt()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.sqlResultSet.updateAsciiStream(1, inputStream, 100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateAsciiStream(1, inputStream, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateAsciiStream(String, InputStream, int)'
	 */
	public void testUpdateAsciiStreamStringInputStreamInt()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.sqlResultSet.updateAsciiStream("column", inputStream, 100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateAsciiStream("column", inputStream, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBigDecimal(int, BigDecimal)'
	 */
	public void testUpdateBigDecimalIntBigDecimal()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.0);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBigDecimal(1, decimal);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBigDecimal(1, decimal);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBigDecimal(String, BigDecimal)'
	 */
	public void testUpdateBigDecimalStringBigDecimal()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.0);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBigDecimal("column", decimal);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBigDecimal("column", decimal);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBinaryStream(int, InputStream, int)'
	 */
	public void testUpdateBinaryStreamIntInputStreamInt()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.sqlResultSet.updateBinaryStream(1, inputStream, 100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBinaryStream(1, inputStream, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBinaryStream(String, InputStream, int)'
	 */
	public void testUpdateBinaryStreamStringInputStreamInt()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.sqlResultSet.updateBinaryStream("column", inputStream, 100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBinaryStream("column", inputStream, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBlob(int, Blob)'
	 */
	public void testUpdateBlobIntBlob()
	{
		try
		{
			Blob blob = (Blob) this.createMock(Blob.class);

			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBlob(1, blob);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBlob(1, blob);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBlob(String, Blob)'
	 */
	public void testUpdateBlobStringBlob()
	{
		try
		{
			Blob blob = (Blob) this.createMock(Blob.class);

			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBlob("column", blob);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBlob("column", blob);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBoolean(int, boolean)'
	 */
	public void testUpdateBooleanIntBoolean()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBoolean(1, true);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBoolean(1, true);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBoolean(String, boolean)'
	 */
	public void testUpdateBooleanStringBoolean()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBoolean("column", true);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBoolean("column", true);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateByte(int, byte)'
	 */
	public void testUpdateByteIntByte()
	{
		try
		{
			byte value = 1;
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateByte(1, value);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateByte(1, value);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateByte(String, byte)'
	 */
	public void testUpdateByteStringByte()
	{
		try
		{
			byte value = 1;
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateByte("column", value);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateByte("column", value);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBytes(int, byte[])'
	 */
	public void testUpdateBytesIntByteArray()
	{
		try
		{
			byte[] value = new byte[] { 1 };
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBytes(1, value);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBytes(1, value);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateBytes(String, byte[])'
	 */
	public void testUpdateBytesStringByteArray()
	{
		try
		{
			byte[] value = new byte[] { 1 };
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateBytes("column", value);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateBytes("column", value);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateCharacterStream(int, Reader, int)'
	 */
	public void testUpdateCharacterStreamIntReaderInt()
	{
		try
		{
			Reader reader = new StringReader("test");
			File file = new File("");
			
			this.fileSupport.createFile(reader);
			this.fileSupportControl.setReturnValue(file);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.getReader(file);
			this.fileSupportControl.setReturnValue(reader);
			
			this.sqlResultSet.updateCharacterStream(1, reader, 100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateCharacterStream(1, reader, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateCharacterStream(String, Reader, int)'
	 */
	public void testUpdateCharacterStreamStringReaderInt()
	{
		try
		{
			Reader reader = new StringReader("test");
			File file = new File("");
			
			this.fileSupport.createFile(reader);
			this.fileSupportControl.setReturnValue(file);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.getReader(file);
			this.fileSupportControl.setReturnValue(reader);
			
			this.sqlResultSet.updateCharacterStream("column", reader, 100);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateCharacterStream("column", reader, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateClob(int, Clob)'
	 */
	public void testUpdateClobIntClob()
	{
		try
		{
			Clob clob = (Clob) this.createMock(Clob.class);

			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateClob(1, clob);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateClob(1, clob);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateClob(String, Clob)'
	 */
	public void testUpdateClobStringClob()
	{
		try
		{
			Clob clob = (Clob) this.createMock(Clob.class);

			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateClob("column", clob);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateClob("column", clob);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateDate(int, Date)'
	 */
	public void testUpdateDateIntDate()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());

			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateDate(1, date);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateDate(1, date);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateDate(String, Date)'
	 */
	public void testUpdateDateStringDate()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());

			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateDate("column", date);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateDate("column", date);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateDouble(int, double)'
	 */
	public void testUpdateDoubleIntDouble()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateDouble(1, 1.1);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateDouble(1, 1.1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateDouble(String, double)'
	 */
	public void testUpdateDoubleStringDouble()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateDouble("column", 1.1);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateDouble("column", 1.1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateFloat(int, float)'
	 */
	public void testUpdateFloatIntFloat()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateFloat(1, 1.1f);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateFloat(1, 1.1f);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateFloat(String, float)'
	 */
	public void testUpdateFloatStringFloat()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateFloat("column", 1.1f);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateFloat("column", 1.1f);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateInt(int, int)'
	 */
	public void testUpdateIntIntInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateInt(1, 1);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateInt(1, 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateInt(String, int)'
	 */
	public void testUpdateIntStringInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateInt("column", 1);;
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateInt("column", 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateLong(int, long)'
	 */
	public void testUpdateLongIntLong()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateLong(1, 1);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateLong(1, 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateLong(String, long)'
	 */
	public void testUpdateLongStringLong()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateLong("column", 1);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateLong("column", 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateNull(int)'
	 */
	public void testUpdateNullInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateNull(1);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateNull(1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateNull(String)'
	 */
	public void testUpdateNullString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateNull("column");
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateNull("column");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateObject(int, Object)'
	 */
	public void testUpdateObjectIntObject()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateObject(1, object);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateObject(1, object);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateObject(int, Object, int)'
	 */
	public void testUpdateObjectIntObjectInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateObject(1, object, Types.ARRAY);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateObject(1, object, Types.ARRAY);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateObject(String, Object)'
	 */
	public void testUpdateObjectStringObject()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateObject("column", object);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateObject("column", object);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateObject(String, Object, int)'
	 */
	public void testUpdateObjectStringObjectInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateObject("column", object, Types.ARRAY);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateObject("column", object, Types.ARRAY);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateRef(int, Ref)'
	 */
	public void testUpdateRefIntRef()
	{
		try
		{
			Ref ref = (Ref) this.createMock(Ref.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateObject(1, ref);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateObject(1, ref);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateRef(String, Ref)'
	 */
	public void testUpdateRefStringRef()
	{
		try
		{
			Ref ref = (Ref) this.createMock(Ref.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateObject("column", ref);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateObject("column", ref);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateRow()'
	 */
	public void testUpdateRow()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateRow();
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateRow();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateShort(int, short)'
	 */
	public void testUpdateShortIntShort()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateShort(1, (short) 10);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateShort(1, (short) 10);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateShort(String, short)'
	 */
	public void testUpdateShortStringShort()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateShort("column", (short) 10);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateShort("column", (short) 10);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateString(int, String)'
	 */
	public void testUpdateStringIntString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateString(1, "test");
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateString(1, "test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateString(String, String)'
	 */
	public void testUpdateStringStringString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateString("column", "test");
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateString("column", "test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateTime(int, Time)'
	 */
	public void testUpdateTimeIntTime()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateTime(1, time);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateTime(1, time);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateTime(String, Time)'
	 */
	public void testUpdateTimeStringTime()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateTime("column", time);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateTime("column", time);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateTimestamp(int, Timestamp)'
	 */
	public void testUpdateTimestampIntTimestamp()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateTimestamp(1, timestamp);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateTimestamp(1, timestamp);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.updateTimestamp(String, Timestamp)'
	 */
	public void testUpdateTimestampStringTimestamp()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResultSet.updateTimestamp("column", timestamp);
			this.sqlResultSetControl.setVoidCallable();
			
			replay();
			
			this.resultSet.updateTimestamp("column", timestamp);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.ResultSet.wasNull()'
	 */
	public void testWasNull()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResultSet.wasNull();
			this.sqlResultSetControl.setReturnValue(true);
			
			replay();
			
			boolean wasNull = this.resultSet.wasNull();
			
			verify();
			
			assertTrue(wasNull);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
}
