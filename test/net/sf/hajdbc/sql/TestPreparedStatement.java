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
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.regex.Pattern;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * Unit test for {@link PreparedStatement}
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
public class TestPreparedStatement extends TestStatement
{
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#createStatement(net.sf.hajdbc.sql.Connection)
	 */
	protected Statement createStatement(Connection connection) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return TestPreparedStatement.this.getSQLStatement();
			}
		};
		
		return new PreparedStatement(connection, operation, "");
	}
	
	private PreparedStatement getStatement()
	{
		return PreparedStatement.class.cast(this.statement);
	}

	protected java.sql.PreparedStatement getSQLStatement()
	{
		return java.sql.PreparedStatement.class.cast(this.sqlStatement);
	}

	protected Class<? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.PreparedStatement.class;
	}
	
	/**
	 * Test method for {@link PreparedStatement#addBatch()}
	 */
	public void testAddBatch()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().addBatch("test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().addBatch("test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#clearParameters()}
	 */
	public void testClearParameters()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().clearParameters();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().clearParameters();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#execute()}
	 */
	public void testExecute()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().execute()).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean success = this.getStatement().execute();
			
			this.control.verify();
			
			assert success;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#executeQuery()}
	 */
	public void testExecuteQuery()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);

			EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
			EasyMock.expect(this.dialect.getSelectForUpdatePattern()).andReturn(Pattern.compile("XXX"));
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);
			
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.getSQLStatement().executeQuery()).andReturn(resultSet);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			ResultSet rs = this.getStatement().executeQuery();
			
			this.control.verify();
			
			assert rs == resultSet;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#executeQuery()}
	 */
	public void testUpdatableExecuteQuery()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.getSQLStatement().executeQuery()).andReturn(resultSet);
			
			this.lock.unlock();
			
			this.control.replay();
			
			ResultSet rs = this.getStatement().executeQuery();
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.ResultSet.class.cast(rs).getObject(this.database) == resultSet;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#executeUpdate()}
	 */
	public void testExecuteUpdate()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().executeUpdate()).andReturn(1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			int result = this.getStatement().executeUpdate();
			
			this.control.verify();
			
			assert result == 1 : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#getMetaData()}
	 */
	public void testGetMetaData()
	{
		ResultSetMetaData metaData = EasyMock.createMock(ResultSetMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getMetaData()).andReturn(metaData);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			ResultSetMetaData result = this.getStatement().getMetaData();
			
			this.control.verify();
			
			assert result == metaData;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#getParameterMetaData()}
	 */
	public void testGetParameterMetaData()
	{
		ParameterMetaData metaData = EasyMock.createMock(ParameterMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		try
		{
			EasyMock.expect(this.getSQLStatement().getParameterMetaData()).andReturn(metaData);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			ParameterMetaData result = this.getStatement().getParameterMetaData();
			
			this.control.verify();
			
			assert result == metaData;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setArray(int, Array)}
	 */
	public void testSetArray()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setArray(1, array);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setArray(1, array);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setAsciiStream(int, InputStream, int)}
	 */
	public void testSetAsciiStream()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.getSQLStatement().setAsciiStream(1, inputStream, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setAsciiStream(1, inputStream, 10);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setBigDecimal(int, BigDecimal)}
	 */
	public void testSetBigDecimal()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setBigDecimal(1, decimal);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBigDecimal(1, decimal);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setBinaryStream(int, InputStream, int)}
	 */
	public void testSetBinaryStream()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.getSQLStatement().setBinaryStream(1, inputStream, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBinaryStream(1, inputStream, 10);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setBlob(int, Blob)}
	 */
	public void testSetBlob()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		File file = new File("");
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(blob)).andReturn(file);
			EasyMock.expect(this.fileSupport.getBlob(file)).andReturn(blob);
			
			this.getSQLStatement().setBlob(1, blob);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBlob(1, blob);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setBoolean(int, boolean)}
	 */
	public void testSetBoolean()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setBoolean(1, true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBoolean(1, true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setByte(int, byte)}
	 */
	public void testSetByte()
	{
		byte value = 1;
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setByte(1, value);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setByte(1, value);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setBytes(int, byte[])}
	 */
	public void testSetBytes()
	{
		byte[] bytes = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setBytes(1, bytes);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setBytes(1, bytes);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setCharacterStream(int, Reader, int)}
	 */
	public void testSetCharacterStream()
	{
		Reader reader = new StringReader("test");
		File file = new File("");
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(reader)).andReturn(file);
			EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
			
			this.getSQLStatement().setCharacterStream(1, reader, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setCharacterStream(1, reader, 10);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setClob(int, Clob)}
	 */
	public void testSetClob()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		File file = new File("");
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(clob)).andReturn(file);
			EasyMock.expect(this.fileSupport.getClob(file)).andReturn(clob);
			
			this.getSQLStatement().setClob(1, clob);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setClob(1, clob);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setDate(int, Date)}
	 */
	public void testSetDateIntDate()
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setDate(1, date);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setDate(1, date);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setDate(int, Date, Calendar)}
	 */
	public void testSetDateIntDateCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setDate(1, date, calendar);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setDate(1, date, calendar);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setDouble(int, double)}
	 */
	public void testSetDouble()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setDouble(1, 1.0);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setDouble(1, 1.0);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setFloat(int, float)}
	 */
	public void testSetFloat()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setFloat(1, 1.0f);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setFloat(1, 1.0f);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setInt(int, int)}
	 */
	public void testSetInt()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setInt(1, 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setInt(1, 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setLong(int, long)}
	 */
	public void testSetLong()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setLong(1, 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setLong(1, 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setNull(int, int)}
	 */
	public void testSetNullIntInt()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setNull(1, Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setNull(1, Types.ARRAY);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setNull(int, int, String)}
	 */
	public void testSetNullIntIntString()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setNull(1, Types.JAVA_OBJECT, "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setNull(1, Types.JAVA_OBJECT, "test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setObject(int, Object)}
	 */
	public void testSetObjectIntObject()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setObject(1, object);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setObject(1, object);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setObject(int, Object, int)}
	 */
	public void testSetObjectIntObjectInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setObject(1, object, Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setObject(1, object, Types.ARRAY);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setObject(int, Object, int, int)}
	 */
	public void testSetObjectIntObjectIntInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setObject(1, object, Types.NUMERIC, 10);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setObject(1, object, Types.NUMERIC, 10);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setRef(int, Ref)}
	 */
	public void testSetRef()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setRef(1, ref);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setRef(1, ref);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setShort(int, short)}
	 */
	public void testSetShort()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setShort(1, (short) 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setShort(1, (short) 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setString(int, String)}
	 */
	public void testSetString()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setString(1, "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setString(1, "test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setTime(int, Time)}
	 */
	public void testSetTimeIntTime()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTime(1, time);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTime(1, time);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setTime(int, Time, Calendar)}
	 */
	public void testSetTimeIntTimeCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTime(1, time, calendar);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTime(1, time, calendar);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setTimestamp(int, Timestamp)}
	 */
	public void testSetTimestampIntTimestamp()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTimestamp(1, timestamp);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTimestamp(1, timestamp);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setTimestamp(int, Timestamp, Calendar)}
	 */
	public void testSetTimestampIntTimestampCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.getSQLStatement().setTimestamp(1, timestamp, calendar);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setTimestamp(1, timestamp, calendar);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setUnicodeStream(int, InputStream, int)}
	 * @deprecated
	 */
	public void testSetUnicodeStream()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.getSQLStatement().setUnicodeStream(1, inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setUnicodeStream(1, inputStream, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link PreparedStatement#setURL(int, URL)}
	 */
	public void testSetURL()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			URL url = new URL("http://www.google.com");
			
			this.getSQLStatement().setURL(1, url);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.getStatement().setURL(1, url);
			
			this.control.verify();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
}
