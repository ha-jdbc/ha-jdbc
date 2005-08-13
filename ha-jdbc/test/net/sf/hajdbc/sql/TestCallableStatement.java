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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;

public class TestCallableStatement extends TestPreparedStatement
{
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#createStatement(net.sf.hajdbc.sql.Connection, net.sf.hajdbc.sql.ConnectionOperation)
	 */
	protected Statement createStatement(Connection connection, ConnectionOperation operation) throws SQLException
	{
		return new CallableStatement(connection, operation, "");
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#getSQLStatementClass()
	 */
	protected Class getSQLStatementClass()
	{
		return java.sql.CallableStatement.class;
	}

	private CallableStatement getStatement()
	{
		return (CallableStatement) this.statement;
	}

	private java.sql.CallableStatement getSQLStatement()
	{
		return (java.sql.CallableStatement) this.sqlStatement;
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getArray(int)'
	 */
	public void testGetArrayInt()
	{
		try
		{
			Array array = (Array) this.createMock(Array.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getArray(1);
			this.sqlStatementControl.setReturnValue(array);
			
			replay();
			
			Array value = this.getStatement().getArray(1);
			
			verify();
			
			assertSame(array, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getArray(String)'
	 */
	public void testGetArrayString()
	{
		try
		{
			Array array = (Array) this.createMock(Array.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getArray("column");
			this.sqlStatementControl.setReturnValue(array);
			
			replay();
			
			Array value = this.getStatement().getArray("column");
			
			verify();
			
			assertSame(array, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBigDecimal(int)'
	 */
	public void testGetBigDecimalInt()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.1);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBigDecimal(1);
			this.sqlStatementControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.getStatement().getBigDecimal(1);
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBigDecimal(int, int)'
	 */
	public void testGetBigDecimalIntInt()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.1);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBigDecimal(1, 10);
			this.sqlStatementControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.getStatement().getBigDecimal(1, 10);
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBigDecimal(String)'
	 */
	public void testGetBigDecimalString()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.1);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBigDecimal("column");
			this.sqlStatementControl.setReturnValue(decimal);
			
			replay();
			
			BigDecimal value = this.getStatement().getBigDecimal("column");
			
			verify();
			
			assertSame(decimal, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBlob(int)'
	 */
	public void testGetBlobInt()
	{
		try
		{
			Blob blob = (Blob) this.createMock(Blob.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBlob(1);
			this.sqlStatementControl.setReturnValue(blob);
			
			replay();
			
			Blob value = this.getStatement().getBlob(1);
			
			verify();
			
			assertSame(blob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBlob(String)'
	 */
	public void testGetBlobString()
	{
		try
		{
			Blob blob = (Blob) this.createMock(Blob.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBlob("column");
			this.sqlStatementControl.setReturnValue(blob);
			
			replay();
			
			Blob value = this.getStatement().getBlob("column");
			
			verify();
			
			assertSame(blob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBoolean(int)'
	 */
	public void testGetBooleanInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBoolean(1);
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean value = this.getStatement().getBoolean(1);
			
			verify();
			
			assertTrue(value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBoolean(String)'
	 */
	public void testGetBooleanString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBoolean("column");
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean value = this.getStatement().getBoolean("column");
			
			verify();
			
			assertTrue(value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getByte(int)'
	 */
	public void testGetByteInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getByte(1);
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			byte value = this.getStatement().getByte(1);
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getByte(String)'
	 */
	public void testGetByteString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getByte("column");
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			byte value = this.getStatement().getByte("column");
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBytes(int)'
	 */
	public void testGetBytesInt()
	{
		try
		{
			byte[] bytes = new byte[] { 1 };
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBytes(1);
			this.sqlStatementControl.setReturnValue(bytes);
			
			replay();
			
			byte[] value = this.getStatement().getBytes(1);
			
			verify();
			
			assertSame(bytes, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getBytes(String)'
	 */
	public void testGetBytesString()
	{
		try
		{
			byte[] bytes = new byte[] { 1 };
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getBytes("column");
			this.sqlStatementControl.setReturnValue(bytes);
			
			replay();
			
			byte[] value = this.getStatement().getBytes("column");
			
			verify();
			
			assertSame(bytes, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getClob(int)'
	 */
	public void testGetClobInt()
	{
		try
		{
			Clob clob = (Clob) this.createMock(Clob.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getClob(1);
			this.sqlStatementControl.setReturnValue(clob);
			
			replay();
			
			Clob value = this.getStatement().getClob(1);
			
			verify();
			
			assertSame(clob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getClob(String)'
	 */
	public void testGetClobString()
	{
		try
		{
			Clob clob = (Clob) this.createMock(Clob.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getClob("column");
			this.sqlStatementControl.setReturnValue(clob);
			
			replay();
			
			Clob value = this.getStatement().getClob("column");
			
			verify();
			
			assertSame(clob, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getDate(int)'
	 */
	public void testGetDateInt()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getDate(1);
			this.sqlStatementControl.setReturnValue(date);
			
			replay();
			
			Date value = this.getStatement().getDate(1);
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getDate(int, Calendar)'
	 */
	public void testGetDateIntCalendar()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getDate(1, calendar);
			this.sqlStatementControl.setReturnValue(date);
			
			replay();
			
			Date value = this.getStatement().getDate(1, calendar);
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getDate(String)'
	 */
	public void testGetDateString()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getDate("column");
			this.sqlStatementControl.setReturnValue(date);
			
			replay();
			
			Date value = this.getStatement().getDate("column");
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getDate(String, Calendar)'
	 */
	public void testGetDateStringCalendar()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getDate("column", calendar);
			this.sqlStatementControl.setReturnValue(date);
			
			replay();
			
			Date value = this.getStatement().getDate("column", calendar);
			
			verify();
			
			assertSame(date, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getDouble(int)'
	 */
	public void testGetDoubleInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getDouble(1);
			this.sqlStatementControl.setReturnValue(1.1);
			
			replay();
			
			double value = this.getStatement().getDouble(1);
			
			verify();
			
			assertEquals(1.1, value, 0);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getDouble(String)'
	 */
	public void testGetDoubleString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getDouble("column");
			this.sqlStatementControl.setReturnValue(1.1);
			
			replay();
			
			double value = this.getStatement().getDouble("column");
			
			verify();
			
			assertEquals(1.1, value, 0);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getFloat(int)'
	 */
	public void testGetFloatInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getFloat(1);
			this.sqlStatementControl.setReturnValue(1.1f);
			
			replay();
			
			float value = this.getStatement().getFloat(1);
			
			verify();
			
			assertEquals(1.1f, value, 0);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getFloat(String)'
	 */
	public void testGetFloatString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getFloat("column");
			this.sqlStatementControl.setReturnValue(1.1f);
			
			replay();
			
			float value = this.getStatement().getFloat("column");
			
			verify();
			
			assertEquals(1.1f, value, 0);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getInt(int)'
	 */
	public void testGetIntInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getInt(1);
			this.sqlStatementControl.setReturnValue(10);
			
			replay();
			
			int value = this.getStatement().getInt(1);
			
			verify();
			
			assertEquals(10, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getInt(String)'
	 */
	public void testGetIntString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getInt("column");
			this.sqlStatementControl.setReturnValue(10);
			
			replay();
			
			int value = this.getStatement().getInt("column");
			
			verify();
			
			assertEquals(10, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getLong(int)'
	 */
	public void testGetLongInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getLong(1);
			this.sqlStatementControl.setReturnValue(10);
			
			replay();
			
			long value = this.getStatement().getLong(1);
			
			verify();
			
			assertEquals(10, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getLong(String)'
	 */
	public void testGetLongString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getLong("column");
			this.sqlStatementControl.setReturnValue(10);
			
			replay();
			
			long value = this.getStatement().getLong("column");
			
			verify();
			
			assertEquals(10, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getObject(int)'
	 */
	public void testGetObjectInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getObject(1);
			this.sqlStatementControl.setReturnValue(object);
			
			replay();
			
			Object value = this.getStatement().getObject(1);
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getObject(int, Map)'
	 */
	public void testGetObjectIntMap()
	{
		try
		{
			Object object = new Object();
			Map typeMap = Collections.EMPTY_MAP;
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getObject(1, typeMap);
			this.sqlStatementControl.setReturnValue(object);
			
			replay();
			
			Object value = this.getStatement().getObject(1, typeMap);
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getObject(String)'
	 */
	public void testGetObjectString()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getObject("column");
			this.sqlStatementControl.setReturnValue(object);
			
			replay();
			
			Object value = this.getStatement().getObject("column");
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getObject(String, Map)'
	 */
	public void testGetObjectStringMap()
	{
		try
		{
			Object object = new Object();
			Map typeMap = Collections.EMPTY_MAP;
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getObject("column", typeMap);
			this.sqlStatementControl.setReturnValue(object);
			
			replay();
			
			Object value = this.getStatement().getObject("column", typeMap);
			
			verify();
			
			assertSame(object, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getRef(int)'
	 */
	public void testGetRefInt()
	{
		try
		{
			Ref ref = (Ref) this.createMock(Ref.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getRef(1);
			this.sqlStatementControl.setReturnValue(ref);
			
			replay();
			
			Ref value = this.getStatement().getRef(1);
			
			verify();
			
			assertSame(ref, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getRef(String)'
	 */
	public void testGetRefString()
	{
		try
		{
			Ref ref = (Ref) this.createMock(Ref.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getRef("column");
			this.sqlStatementControl.setReturnValue(ref);
			
			replay();
			
			Ref value = this.getStatement().getRef("column");
			
			verify();
			
			assertSame(ref, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getShort(int)'
	 */
	public void testGetShortInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getShort(1);
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			short value = this.getStatement().getShort(1);
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getShort(String)'
	 */
	public void testGetShortString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getShort("column");
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			short value = this.getStatement().getShort("column");
			
			verify();
			
			assertEquals(1, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getString(int)'
	 */
	public void testGetStringInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getString(1);
			this.sqlStatementControl.setReturnValue("test");
			
			replay();
			
			String value = this.getStatement().getString(1);
			
			verify();
			
			assertEquals("test", value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getString(String)'
	 */
	public void testGetStringString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getString("column");
			this.sqlStatementControl.setReturnValue("test");
			
			replay();
			
			String value = this.getStatement().getString("column");
			
			verify();
			
			assertEquals("test", value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTime(int)'
	 */
	public void testGetTimeInt()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTime(1);
			this.sqlStatementControl.setReturnValue(time);
			
			replay();
			
			Time value = this.getStatement().getTime(1);
			
			verify();
			
			assertSame(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTime(int, Calendar)'
	 */
	public void testGetTimeIntCalendar()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTime(1, calendar);
			this.sqlStatementControl.setReturnValue(time);
			
			replay();
			
			Time value = this.getStatement().getTime(1, calendar);
			
			verify();
			
			assertSame(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTime(String)'
	 */
	public void testGetTimeString()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTime("column");
			this.sqlStatementControl.setReturnValue(time);
			
			replay();
			
			Time value = this.getStatement().getTime("column");
			
			verify();
			
			assertSame(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTime(String, Calendar)'
	 */
	public void testGetTimeStringCalendar()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTime("column", calendar);
			this.sqlStatementControl.setReturnValue(time);
			
			replay();
			
			Time value = this.getStatement().getTime("column", calendar);
			
			verify();
			
			assertSame(time, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTimestamp(int)'
	 */
	public void testGetTimestampInt()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTimestamp(1);
			this.sqlStatementControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.getStatement().getTimestamp(1);
			
			verify();
			
			assertSame(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTimestamp(int, Calendar)'
	 */
	public void testGetTimestampIntCalendar()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTimestamp(1, calendar);
			this.sqlStatementControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.getStatement().getTimestamp(1, calendar);
			
			verify();
			
			assertSame(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTimestamp(String)'
	 */
	public void testGetTimestampString()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTimestamp("column");
			this.sqlStatementControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.getStatement().getTimestamp("column");
			
			verify();
			
			assertSame(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getTimestamp(String, Calendar)'
	 */
	public void testGetTimestampStringCalendar()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getTimestamp("column", calendar);
			this.sqlStatementControl.setReturnValue(timestamp);
			
			replay();
			
			Timestamp value = this.getStatement().getTimestamp("column", calendar);
			
			verify();
			
			assertSame(timestamp, value);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getURL(int)'
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
			
			this.getSQLStatement().getURL(1);
			this.sqlStatementControl.setReturnValue(url);
			
			replay();
			
			URL value = this.getStatement().getURL(1);
			
			verify();
			
			assertSame(url, value);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.getURL(String)'
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
			
			this.getSQLStatement().getURL("column");
			this.sqlStatementControl.setReturnValue(url);
			
			replay();
			
			URL value = this.getStatement().getURL("column");
			
			verify();
			
			assertSame(url, value);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.registerOutParameter(int, int)'
	 */
	public void testRegisterOutParameterIntInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().registerOutParameter(1, Types.ARRAY);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().registerOutParameter(1, Types.ARRAY);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.registerOutParameter(int, int, int)'
	 */
	public void testRegisterOutParameterIntIntInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().registerOutParameter(1, Types.NUMERIC, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().registerOutParameter(1, Types.NUMERIC, 10);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.registerOutParameter(int, int, String)'
	 */
	public void testRegisterOutParameterIntIntString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().registerOutParameter(1, Types.JAVA_OBJECT, "test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().registerOutParameter(1, Types.JAVA_OBJECT, "test");
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.registerOutParameter(String, int)'
	 */
	public void testRegisterOutParameterStringInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().registerOutParameter("param", Types.ARRAY);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().registerOutParameter("param", Types.ARRAY);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.registerOutParameter(String, int, int)'
	 */
	public void testRegisterOutParameterStringIntInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().registerOutParameter("param", Types.NUMERIC, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().registerOutParameter("param", Types.NUMERIC, 10);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.registerOutParameter(String, int, String)'
	 */
	public void testRegisterOutParameterStringIntString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().registerOutParameter("param", Types.JAVA_OBJECT, "test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().registerOutParameter("param", Types.JAVA_OBJECT, "test");
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setAsciiStream(String, InputStream, int)'
	 */
	public void testSetAsciiStreamStringInputStreamInt()
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
			
			this.getSQLStatement().setAsciiStream("param", inputStream, 100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setAsciiStream("param", inputStream, 100);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setBigDecimal(String, BigDecimal)'
	 */
	public void testSetBigDecimalStringBigDecimal()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.1);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBigDecimal("param", decimal);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBigDecimal("param", decimal);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setBinaryStream(String, InputStream, int)'
	 */
	public void testSetBinaryStreamStringInputStreamInt()
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
			
			this.getSQLStatement().setBinaryStream("param", inputStream, 100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBinaryStream("param", inputStream, 100);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setBoolean(String, boolean)'
	 */
	public void testSetBooleanStringBoolean()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBoolean("param", true);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBoolean("param", true);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setByte(String, byte)'
	 */
	public void testSetByteStringByte()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setByte("param", (byte) 1);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setByte("param", (byte) 1);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setBytes(String, byte[])'
	 */
	public void testSetBytesStringByteArray()
	{
		try
		{
			byte[] bytes = new byte[] { 1 };
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBytes("param", bytes);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBytes("param", bytes);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setCharacterStream(String, Reader, int)'
	 */
	public void testSetCharacterStreamStringReaderInt()
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
			
			this.getSQLStatement().setCharacterStream("param", reader, 100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setCharacterStream("param", reader, 100);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setDate(String, Date)'
	 */
	public void testSetDateStringDate()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setDate("param", date);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setDate("param", date);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setDate(String, Date, Calendar)'
	 */
	public void testSetDateStringDateCalendar()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setDate("param", date, calendar);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setDate("param", date, calendar);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setDouble(String, double)'
	 */
	public void testSetDoubleStringDouble()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setDouble("param", 1.1);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setDouble("param", 1.1);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setFloat(String, float)'
	 */
	public void testSetFloatStringFloat()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setFloat("param", 1.1f);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setFloat("param", 1.1f);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setInt(String, int)'
	 */
	public void testSetIntStringInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setInt("param", 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setInt("param", 10);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setLong(String, long)'
	 */
	public void testSetLongStringLong()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setLong("param", 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setLong("param", 10);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setNull(String, int)'
	 */
	public void testSetNullStringInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setNull("param", Types.ARRAY);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setNull("param", Types.ARRAY);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setNull(String, int, String)'
	 */
	public void testSetNullStringIntString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setNull("param", Types.JAVA_OBJECT, "test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setNull("param", Types.JAVA_OBJECT, "test");
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setObject(String, Object)'
	 */
	public void testSetObjectStringObject()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setObject("param", object);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setObject("param", object);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setObject(String, Object, int)'
	 */
	public void testSetObjectStringObjectInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setObject("param", object, Types.ARRAY);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setObject("param", object, Types.ARRAY);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setObject(String, Object, int, int)'
	 */
	public void testSetObjectStringObjectIntInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setObject("param", object, Types.NUMERIC, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setObject("param", object, Types.NUMERIC, 10);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setShort(String, short)'
	 */
	public void testSetShortStringShort()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setShort("param", (short) 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setShort("param", (short) 10);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setString(String, String)'
	 */
	public void testSetStringStringString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setString("param", "test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setString("param", "test");
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setTime(String, Time)'
	 */
	public void testSetTimeStringTime()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTime("param", time);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTime("param", time);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setTime(String, Time, Calendar)'
	 */
	public void testSetTimeStringTimeCalendar()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTime("param", time, calendar);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTime("param", time, calendar);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setTimestamp(String, Timestamp)'
	 */
	public void testSetTimestampStringTimestamp()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTimestamp("param", timestamp);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTimestamp("param", timestamp);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setTimestamp(String, Timestamp, Calendar)'
	 */
	public void testSetTimestampStringTimestampCalendar()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTimestamp("param", timestamp, calendar);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTimestamp("param", timestamp, calendar);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.setURL(String, URL)'
	 */
	public void testSetURLStringURL()
	{
		try
		{
			URL url = new URL("http://www.google.com");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setURL("param", url);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setURL("param", url);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.CallableStatement.wasNull()'
	 */
	public void testWasNull()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().wasNull();
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean wasNull = this.getStatement().wasNull();
			
			verify();
			
			assertTrue(wasNull);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}
}
