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

import org.easymock.EasyMock;
import org.testng.annotations.Test;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Operation;

/**
 * Unit test for {@link CallableStatement}.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
public class TestCallableStatement extends TestPreparedStatement
{
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#createStatement(net.sf.hajdbc.sql.Connection)
	 */
	protected Statement createStatement(Connection connection) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.CallableStatement> operation = new Operation<java.sql.Connection, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return TestCallableStatement.this.getSQLStatement();
			}
		};
		
		return new CallableStatement(connection, operation, "");
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TestPreparedStatement#getStatementClass()
	 */
	protected Class< ? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.CallableStatement.class;
	}

	private CallableStatement getStatement()
	{
		return CallableStatement.class.cast(this.statement);
	}

	protected java.sql.CallableStatement getSQLStatement()
	{
		return java.sql.CallableStatement.class.cast(this.sqlStatement);
	}
	
	/**
	 * Test method for {@link CallableStatement#getArray(int)}
	 */
	public void testGetArrayInt()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getArray(1)).andReturn(array);
			
			this.control.replay();
			
			Array value = this.getStatement().getArray(1);
			
			this.control.verify();
			
			assert value == array;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getArray(String)}
	 */
	public void testGetArrayString()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getArray("column")).andReturn(array);
			
			this.control.replay();
			
			Array value = this.getStatement().getArray("column");
			
			this.control.verify();
			
			assert value == array;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBigDecimal(int)}
	 */
	public void testGetBigDecimalInt()
	{
		BigDecimal decimal = new BigDecimal(1.1);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBigDecimal(1)).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.getStatement().getBigDecimal(1);
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBigDecimal(int, int)}
	 * @deprecated
	 */
	public void testGetBigDecimalIntInt()
	{
		BigDecimal decimal = new BigDecimal(1.1);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBigDecimal(1, 10)).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.getStatement().getBigDecimal(1, 10);
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBigDecimal(String)}
	 */
	public void testGetBigDecimalString()
	{
		BigDecimal decimal = new BigDecimal(1.1);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBigDecimal("column")).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.getStatement().getBigDecimal("column");
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBlob(int)}
	 */
	public void testGetBlobInt()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBlob(1)).andReturn(blob);
			
			this.control.replay();
			
			Blob value = this.getStatement().getBlob(1);
			
			this.control.verify();
			
			assert value == blob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBlob(String)}
	 */
	public void testGetBlobString()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBlob("column")).andReturn(blob);
			
			this.control.replay();
			
			Blob value = this.getStatement().getBlob("column");
			
			this.control.verify();
			
			assert value == blob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBoolean(int)}
	 */
	public void testGetBooleanInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBoolean(1)).andReturn(true);
			
			this.control.replay();
			
			boolean value = this.getStatement().getBoolean(1);
			
			this.control.verify();
			
			assert value;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBoolean(String)}
	 */
	public void testGetBooleanString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBoolean("column")).andReturn(true);
			
			this.control.replay();
			
			boolean value = this.getStatement().getBoolean("column");
			
			this.control.verify();
			
			assert value;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getByte(int)}
	 */
	public void testGetByteInt()
	{
		byte b = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getByte(1)).andReturn(b);
			
			this.control.replay();
			
			byte value = this.getStatement().getByte(1);
			
			this.control.verify();
			
			assert value == b;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getByte(String)}
	 */
	public void testGetByteString()
	{
		byte b = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getByte("column")).andReturn(b);
			
			this.control.replay();
			
			byte value = this.getStatement().getByte("column");
			
			this.control.verify();
			
			assert value == b;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBytes(int)}
	 */
	public void testGetBytesInt()
	{
		byte[] bytes = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBytes(1)).andReturn(bytes);
			
			this.control.replay();
			
			byte[] value = this.getStatement().getBytes(1);
			
			this.control.verify();
			
			assert value == bytes;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getBytes(String)}
	 */
	public void testGetBytesString()
	{
		byte[] bytes = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getBytes("column")).andReturn(bytes);
			
			this.control.replay();
			
			byte[] value = this.getStatement().getBytes("column");
			
			this.control.verify();
			
			assert value == bytes;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getClob(int)}
	 */
	public void testGetClobInt()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getClob(1)).andReturn(clob);
			
			this.control.replay();
			
			Clob value = this.getStatement().getClob(1);
			
			this.control.verify();
			
			assert value == clob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getClob(String)}
	 */
	public void testGetClobString()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getClob("column")).andReturn(clob);
			
			this.control.replay();
			
			Clob value = this.getStatement().getClob("column");
			
			this.control.verify();
			
			assert value == clob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getDate(int)}
	 */
	public void testGetDateInt()
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getDate(1)).andReturn(date);
			
			this.control.replay();
			
			Date value = this.getStatement().getDate(1);
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getDate(int, Calendar)}
	 */
	public void testGetDateIntCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getDate(1, calendar)).andReturn(date);
			
			this.control.replay();
			
			Date value = this.getStatement().getDate(1, calendar);
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getDate(String)}
	 */
	public void testGetDateString()
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getDate("column")).andReturn(date);
			
			this.control.replay();
			
			Date value = this.getStatement().getDate("column");
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getDate(String, Calendar)}
	 */
	public void testGetDateStringCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getDate("column", calendar)).andReturn(date);
			
			this.control.replay();
			
			Date value = this.getStatement().getDate("column", calendar);
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getDouble(int)}
	 */
	public void testGetDoubleInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getDouble(1)).andReturn(1.1);
			
			this.control.replay();
			
			double value = this.getStatement().getDouble(1);
			
			this.control.verify();
			
			assert value == 1.1;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getDouble(String)}
	 */
	public void testGetDoubleString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getDouble("column")).andReturn(1.1);
			
			this.control.replay();
			
			double value = this.getStatement().getDouble("column");
			
			this.control.verify();
			
			assert value == 1.1;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getFloat(int)}
	 */
	public void testGetFloatInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getFloat(1)).andReturn(1.1f);
			
			this.control.replay();
			
			float value = this.getStatement().getFloat(1);
			
			this.control.verify();
			
			assert value == 1.1f;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getFloat(String)}
	 */
	public void testGetFloatString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getFloat("column")).andReturn(1.1f);
			
			this.control.replay();
			
			float value = this.getStatement().getFloat("column");
			
			this.control.verify();
			
			assert value == 1.1f;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getInt(int)}
	 */
	public void testGetIntInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getInt(1)).andReturn(10);
			
			this.control.replay();
			
			int value = this.getStatement().getInt(1);
			
			this.control.verify();
			
			assert value == 10;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getInt(String)}
	 */
	public void testGetIntString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getInt("column")).andReturn(10);
			
			this.control.replay();
			
			int value = this.getStatement().getInt("column");
			
			this.control.verify();
			
			assert value == 10;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getLong(int)}
	 */
	public void testGetLongInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getLong(1)).andReturn(10L);
			
			this.control.replay();
			
			long value = this.getStatement().getLong(1);
			
			this.control.verify();
			
			assert value == 10L;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getLong(String)}
	 */
	public void testGetLongString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getLong("column")).andReturn(10L);
			
			this.control.replay();
			
			long value = this.getStatement().getLong("column");
			
			this.control.verify();
			
			assert value == 10L;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getObject(int)}
	 */
	public void testGetObjectInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getObject(1)).andReturn(object);
			
			this.control.replay();
			
			Object value = this.getStatement().getObject(1);
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getObject(int, Map)}
	 */
	public void testGetObjectIntMap()
	{
		Object object = new Object();
		Map typeMap = Collections.EMPTY_MAP;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getObject(1, typeMap)).andReturn(object);
			
			this.control.replay();
			
			Object value = this.getStatement().getObject(1, typeMap);
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getObject(String)}
	 */
	public void testGetObjectString()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getObject("column")).andReturn(object);
			
			this.control.replay();
			
			Object value = this.getStatement().getObject("column");
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getObject(String, Map)}
	 */
	public void testGetObjectStringMap()
	{
		Object object = new Object();
		Map typeMap = Collections.EMPTY_MAP;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getObject("column", typeMap)).andReturn(object);
			
			this.control.replay();
			
			Object value = this.getStatement().getObject("column", typeMap);
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getRef(int)}
	 */
	public void testGetRefInt()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getRef(1)).andReturn(ref);
			
			this.control.replay();
			
			Ref value = this.getStatement().getRef(1);
			
			this.control.verify();
			
			assert value == ref;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getRef(String)}
	 */
	public void testGetRefString()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getRef("column")).andReturn(ref);
			
			this.control.replay();
			
			Ref value = this.getStatement().getRef("column");
			
			this.control.verify();
			
			assert value == ref;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getShort(int)}
	 */
	public void testGetShortInt()
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getShort(1)).andReturn(s);
			
			this.control.replay();
			
			short value = this.getStatement().getShort(1);
			
			this.control.verify();
			
			assert value == s;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getShort(String)}
	 */
	public void testGetShortString()
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getShort("column")).andReturn(s);
			
			this.control.replay();
			
			short value = this.getStatement().getShort("column");
			
			this.control.verify();
			
			assert value == s;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getString(int)}
	 */
	public void testGetStringInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getString(1)).andReturn("test");
			
			this.control.replay();
			
			String value = this.getStatement().getString(1);
			
			this.control.verify();
			
			assert value == "test";
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getString(String)}
	 */
	public void testGetStringString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getString("column")).andReturn("test");
			
			this.control.replay();
			
			String value = this.getStatement().getString("column");
			
			this.control.verify();
			
			assert value == "test";
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTime(int)}
	 */
	public void testGetTimeInt()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTime(1)).andReturn(time);
			
			this.control.replay();
			
			Time value = this.getStatement().getTime(1);
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTime(int, Calendar)}
	 */
	public void testGetTimeIntCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTime(1, calendar)).andReturn(time);
			
			this.control.replay();
			
			Time value = this.getStatement().getTime(1, calendar);
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTime(String)}
	 */
	public void testGetTimeString()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTime("column")).andReturn(time);
			
			this.control.replay();
			
			Time value = this.getStatement().getTime("column");
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTime(String, Calendar)}
	 */
	public void testGetTimeStringCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTime("column", calendar)).andReturn(time);
			
			this.control.replay();
			
			Time value = this.getStatement().getTime("column", calendar);
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTimestamp(int)}
	 */
	public void testGetTimestampInt()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTimestamp(1)).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.getStatement().getTimestamp(1);
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTimestamp(int, Calendar)}
	 */
	public void testGetTimestampIntCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTimestamp(1, calendar)).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.getStatement().getTimestamp(1, calendar);
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTimestamp(String)}
	 */
	public void testGetTimestampString()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTimestamp("column")).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.getStatement().getTimestamp("column");
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getTimestamp(String, Calendar)}
	 */
	public void testGetTimestampStringCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getTimestamp("column", calendar)).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.getStatement().getTimestamp("column", calendar);
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getURL(int)}
	 */
	public void testGetURLInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			URL url = new URL("http://www.google.com");
			
			EasyMock.expect(this.getSQLStatement().getURL(1)).andReturn(url);
			
			this.control.replay();
			
			URL value = this.getStatement().getURL(1);
			
			this.control.verify();
			
			assert value == url;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#getURL(String)}
	 */
	public void testGetURLString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			URL url = new URL("http://www.google.com");
			
			EasyMock.expect(this.getSQLStatement().getURL("column")).andReturn(url);
			
			this.control.replay();
			
			URL value = this.getStatement().getURL("column");
			
			this.control.verify();
			
			assert value == url;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#registerOutParameter(int, int)}
	 */
	public void testRegisterOutParameterIntInt()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().registerOutParameter(1, Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().registerOutParameter(1, Types.ARRAY);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#registerOutParameter(int, int, int)}
	 */
	public void testRegisterOutParameterIntIntInt()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().registerOutParameter(1, Types.NUMERIC, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().registerOutParameter(1, Types.NUMERIC, 10);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#registerOutParameter(int, int, String)}
	 */
	public void testRegisterOutParameterIntIntString()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().registerOutParameter(1, Types.JAVA_OBJECT, "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().registerOutParameter(1, Types.JAVA_OBJECT, "test");
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#registerOutParameter(String, int)}
	 */
	public void testRegisterOutParameterStringInt()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().registerOutParameter("param", Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().registerOutParameter("param", Types.ARRAY);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#registerOutParameter(String, int, int)}
	 */
	public void testRegisterOutParameterStringIntInt()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().registerOutParameter("param", Types.NUMERIC, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().registerOutParameter("param", Types.NUMERIC, 10);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#registerOutParameter(String, int, String)}
	 */
	public void testRegisterOutParameterStringIntString()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().registerOutParameter("param", Types.JAVA_OBJECT, "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().registerOutParameter("param", Types.JAVA_OBJECT, "test");
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setAsciiStream(String, InputStream, int)}
	 */
	public void testSetAsciiStreamStringInputStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
			EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.getSQLStatement().setAsciiStream("param", inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setAsciiStream("param", inputStream, 100);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setBigDecimal(String, BigDecimal)}
	 */
	public void testSetBigDecimalStringBigDecimal()
	{
		BigDecimal decimal = new BigDecimal(1.1);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setBigDecimal("param", decimal);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBigDecimal("param", decimal);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setBinaryStream(String, InputStream, int)}
	 */
	public void testSetBinaryStreamStringInputStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
			EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.getSQLStatement().setBinaryStream("param", inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBinaryStream("param", inputStream, 100);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setBoolean(String, boolean)}
	 */
	public void testSetBooleanStringBoolean()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setBoolean("param", true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBoolean("param", true);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setByte(String, byte)}
	 */
	public void testSetByteStringByte()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setByte("param", (byte) 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setByte("param", (byte) 1);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setBytes(String, byte[])}
	 */
	public void testSetBytesStringByteArray()
	{
		byte[] bytes = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setBytes("param", bytes);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBytes("param", bytes);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setCharacterStream(String, Reader, int)}
	 */
	public void testSetCharacterStreamStringReaderInt()
	{
		Reader reader = new StringReader("test");
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(reader)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
			EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
			
			this.getSQLStatement().setCharacterStream("param", reader, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setCharacterStream("param", reader, 100);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setDate(String, Date)}
	 */
	public void testSetDateStringDate()
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setDate("param", date);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setDate("param", date);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setDate(String, Date, Calendar)}
	 */
	public void testSetDateStringDateCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setDate("param", date, calendar);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setDate("param", date, calendar);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setDouble(String, double)}
	 */
	public void testSetDoubleStringDouble()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setDouble("param", 1.1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setDouble("param", 1.1);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setFloat(String, float)}
	 */
	public void testSetFloatStringFloat()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setFloat("param", 1.1f);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setFloat("param", 1.1f);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setInt(String, int)}
	 */
	public void testSetIntStringInt()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setInt("param", 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setInt("param", 10);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setLong(String, long)}
	 */
	public void testSetLongStringLong()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setLong("param", 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setLong("param", 10);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setNull(String, int)}
	 */
	public void testSetNullStringInt()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setNull("param", Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setNull("param", Types.ARRAY);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setNull(String, int, String)}
	 */
	public void testSetNullStringIntString()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setNull("param", Types.JAVA_OBJECT, "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setNull("param", Types.JAVA_OBJECT, "test");
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setObject(String, Object)}
	 */
	public void testSetObjectStringObject()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setObject("param", object);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setObject("param", object);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setObject(String, Object, int)}
	 */
	public void testSetObjectStringObjectInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setObject("param", object, Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setObject("param", object, Types.ARRAY);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setObject(String, Object, int, int)}
	 */
	public void testSetObjectStringObjectIntInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setObject("param", object, Types.NUMERIC, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setObject("param", object, Types.NUMERIC, 10);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setShort(String, short)}
	 */
	public void testSetShortStringShort()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setShort("param", (short) 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setShort("param", (short) 10);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setString(String, String)}
	 */
	public void testSetStringStringString()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setString("param", "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setString("param", "test");
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setTime(String, Time)}
	 */
	public void testSetTimeStringTime()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTime("param", time);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTime("param", time);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setTime(String, Time, Calendar)}
	 */
	public void testSetTimeStringTimeCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTime("param", time, calendar);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTime("param", time, calendar);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setTimestamp(String, Timestamp)}
	 */
	public void testSetTimestampStringTimestamp()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTimestamp("param", timestamp);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTimestamp("param", timestamp);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setTimestamp(String, Timestamp, Calendar)}
	 */
	public void testSetTimestampStringTimestampCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTimestamp("param", timestamp, calendar);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTimestamp("param", timestamp, calendar);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#setURL(String, URL)}
	 */
	public void testSetURLStringURL()
	{
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			URL url = new URL("http://www.google.com");
			
			this.getSQLStatement().setURL("param", url);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setURL("param", url);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link CallableStatement#wasNull()}
	 */
	public void testWasNull()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().wasNull()).andReturn(true);
			
			this.control.replay();
			
			boolean wasNull = this.getStatement().wasNull();
			
			this.control.verify();
			
			assert wasNull;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
}
