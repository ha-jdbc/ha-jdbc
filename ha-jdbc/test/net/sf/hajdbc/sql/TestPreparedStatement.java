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
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

import net.sf.hajdbc.SQLObject;

public class TestPreparedStatement extends TestStatement
{
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#createStatement(net.sf.hajdbc.sql.Connection, net.sf.hajdbc.sql.ConnectionOperation)
	 */
	protected Statement createStatement(Connection connection, ConnectionOperation operation) throws SQLException
	{
		return new PreparedStatement(connection, operation, "");
	}
	
	protected Class getSQLStatementClass()
	{
		return java.sql.PreparedStatement.class;
	}

	private PreparedStatement getStatement()
	{
		return (PreparedStatement) this.statement;
	}

	private java.sql.PreparedStatement getSQLStatement()
	{
		return (java.sql.PreparedStatement) this.sqlStatement;
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.addBatch()'
	 */
	public void testAddBatch()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().addBatch("test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().addBatch("test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.clearParameters()'
	 */
	public void testClearParameters()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().clearParameters();
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().clearParameters();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.execute()'
	 */
	public void testExecute()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().execute();
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean success = this.getStatement().execute();
			
			verify();
			
			assertTrue(success);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.executeQuery()'
	 */
	public void testExecuteQuery()
	{
		try
		{
			ResultSet resultSet = (ResultSet) this.createMock(ResultSet.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getResultSetConcurrency();
			this.sqlStatementControl.setReturnValue(ResultSet.CONCUR_READ_ONLY);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);
			
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.getSQLStatement().executeQuery();
			this.sqlStatementControl.setReturnValue(resultSet);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			replay();
			
			ResultSet rs = this.getStatement().executeQuery();
			
			verify();
			
			assertSame(resultSet, rs);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.executeQuery()'
	 */
	public void testUpdatableExecuteQuery()
	{
		try
		{
			ResultSet resultSet = (ResultSet) this.createMock(ResultSet.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 3);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.getSQLStatement().getResultSetConcurrency();
			this.sqlStatementControl.setReturnValue(ResultSet.CONCUR_UPDATABLE);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().executeQuery();
			this.sqlStatementControl.setReturnValue(resultSet);
			
			replay();
			
			ResultSet rs = this.getStatement().executeQuery();
			
			verify();
			
			assertNotNull(rs);
			assertTrue(SQLObject.class.isInstance(rs));			
			assertSame(resultSet, ((SQLObject) rs).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.executeUpdate()'
	 */
	public void testExecuteUpdate()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().executeUpdate();
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			int result = this.getStatement().executeUpdate();
			
			verify();
			
			assertEquals(1, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.getMetaData()'
	 */
	public void testGetMetaData()
	{
		try
		{
			ResultSetMetaData metaData = (ResultSetMetaData) this.createMock(ResultSetMetaData.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);
			
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.getSQLStatement().getMetaData();
			this.sqlStatementControl.setReturnValue(metaData);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			replay();
			
			ResultSetMetaData result = this.getStatement().getMetaData();
			
			verify();
			
			assertSame(metaData, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.getParameterMetaData()'
	 */
	public void testGetParameterMetaData()
	{
		try
		{
			ParameterMetaData metaData = (ParameterMetaData) this.createMock(ParameterMetaData.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);
			
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.getSQLStatement().getParameterMetaData();
			this.sqlStatementControl.setReturnValue(metaData);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			replay();
			
			ParameterMetaData result = this.getStatement().getParameterMetaData();
			
			verify();
			
			assertSame(metaData, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setArray(int, Array)'
	 */
	public void testSetArray()
	{
		try
		{
			Array array = (Array) this.createMock(Array.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setArray(1, array);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setArray(1, array);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setAsciiStream(int, InputStream, int)'
	 */
	public void testSetAsciiStream()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.getSQLStatement().setAsciiStream(1, inputStream, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setAsciiStream(1, inputStream, 10);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setBigDecimal(int, BigDecimal)'
	 */
	public void testSetBigDecimal()
	{
		try
		{
			BigDecimal decimal = new BigDecimal(1.0);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBigDecimal(1, decimal);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBigDecimal(1, decimal);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setBinaryStream(int, InputStream, int)'
	 */
	public void testSetBinaryStream()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.getSQLStatement().setBinaryStream(1, inputStream, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBinaryStream(1, inputStream, 10);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setBlob(int, Blob)'
	 */
	public void testSetBlob()
	{
		try
		{
			Blob blob = (Blob) this.createMock(Blob.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBlob(1, blob);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBlob(1, blob);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setBoolean(int, boolean)'
	 */
	public void testSetBoolean()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBoolean(1, true);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBoolean(1, true);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setByte(int, byte)'
	 */
	public void testSetByte()
	{
		try
		{
			byte value = 1;
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setByte(1, value);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setByte(1, value);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setBytes(int, byte[])'
	 */
	public void testSetBytes()
	{
		try
		{
			byte[] bytes = new byte[] { 1 };
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setBytes(1, bytes);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setBytes(1, bytes);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setCharacterStream(int, Reader, int)'
	 */
	public void testSetCharacterStream()
	{
		try
		{
			Reader reader = new StringReader("test");
			File file = new File("");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.createFile(reader);
			this.fileSupportControl.setReturnValue(file);
			
			this.fileSupport.getReader(file);
			this.fileSupportControl.setReturnValue(reader);
			
			this.getSQLStatement().setCharacterStream(1, reader, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setCharacterStream(1, reader, 10);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setClob(int, Clob)'
	 */
	public void testSetClob()
	{
		try
		{
			Clob clob = (Clob) this.createMock(Clob.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setClob(1, clob);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setClob(1, clob);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setDate(int, Date)'
	 */
	public void testSetDateIntDate()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setDate(1, date);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setDate(1, date);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setDate(int, Date, Calendar)'
	 */
	public void testSetDateIntDateCalendar()
	{
		try
		{
			Date date = new Date(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setDate(1, date, calendar);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setDate(1, date, calendar);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setDouble(int, double)'
	 */
	public void testSetDouble()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setDouble(1, 1.0);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setDouble(1, 1.0);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setFloat(int, float)'
	 */
	public void testSetFloat()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setFloat(1, 1.0f);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setFloat(1, 1.0f);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setInt(int, int)'
	 */
	public void testSetInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setInt(1, 1);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setInt(1, 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setLong(int, long)'
	 */
	public void testSetLong()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setLong(1, 1);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setLong(1, 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setNull(int, int)'
	 */
	public void testSetNullIntInt()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setNull(1, Types.ARRAY);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setNull(1, Types.ARRAY);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setNull(int, int, String)'
	 */
	public void testSetNullIntIntString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setNull(1, Types.JAVA_OBJECT, "test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setNull(1, Types.JAVA_OBJECT, "test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setObject(int, Object)'
	 */
	public void testSetObjectIntObject()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setObject(1, object);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setObject(1, object);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setObject(int, Object, int)'
	 */
	public void testSetObjectIntObjectInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setObject(1, object, Types.ARRAY);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setObject(1, object, Types.ARRAY);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setObject(int, Object, int, int)'
	 */
	public void testSetObjectIntObjectIntInt()
	{
		try
		{
			Object object = new Object();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setObject(1, object, Types.NUMERIC, 10);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setObject(1, object, Types.NUMERIC, 10);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setRef(int, Ref)'
	 */
	public void testSetRef()
	{
		try
		{
			Ref ref = (Ref) this.createMock(Ref.class);
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setRef(1, ref);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setRef(1, ref);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setShort(int, short)'
	 */
	public void testSetShort()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setShort(1, (short) 1);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setShort(1, (short) 1);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setString(int, String)'
	 */
	public void testSetString()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setString(1, "test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setString(1, "test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setTime(int, Time)'
	 */
	public void testSetTimeIntTime()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTime(1, time);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTime(1, time);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setTime(int, Time, Calendar)'
	 */
	public void testSetTimeIntTimeCalendar()
	{
		try
		{
			Time time = new Time(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTime(1, time, calendar);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTime(1, time, calendar);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setTimestamp(int, Timestamp)'
	 */
	public void testSetTimestampIntTimestamp()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTimestamp(1, timestamp);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTimestamp(1, timestamp);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setTimestamp(int, Timestamp, Calendar)'
	 */
	public void testSetTimestampIntTimestampCalendar()
	{
		try
		{
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			Calendar calendar = Calendar.getInstance();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setTimestamp(1, timestamp, calendar);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setTimestamp(1, timestamp, calendar);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setUnicodeStream(int, InputStream, int)'
	 * @deprecated
	 */
	public void testSetUnicodeStream()
	{
		try
		{
			InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
			File file = new File("");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.fileSupport.createFile(inputStream);
			this.fileSupportControl.setReturnValue(file);
			
			this.fileSupport.getInputStream(file);
			this.fileSupportControl.setReturnValue(inputStream);
			
			this.getSQLStatement().setUnicodeStream(1, inputStream, 100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setUnicodeStream(1, inputStream, 100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.PreparedStatement.setURL(int, URL)'
	 */
	public void testSetURL()
	{
		try
		{
			URL url = new URL("http://www.google.com");
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.getSQLStatement().setURL(1, url);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.getStatement().setURL(1, url);
			
			verify();
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}
}
