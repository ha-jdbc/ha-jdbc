package net.sf.ha.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class ResultSetProxy extends AbstractProxy implements ResultSet
{
	protected StatementProxy statement;
	
	public ResultSetProxy(StatementProxy statement, Map resultSetMap)
	{
		super(resultSetMap);
		
		this.statement = statement;
	}
	
	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	public int getConcurrency() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getConcurrency());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getFetchDirection());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	public int getFetchSize() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getFetchSize());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getRow()
	 */
	public int getRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getRow());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	public int getType() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getType());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#afterLast()
	 */
	public void afterLast() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.afterLast();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#beforeFirst()
	 */
	public void beforeFirst() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.beforeFirst();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#cancelRowUpdates()
	 */
	public void cancelRowUpdates() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.cancelRowUpdates();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.clearWarnings();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#close()
	 */
	public void close() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.close();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#deleteRow()
	 */
	public void deleteRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.deleteRow();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#insertRow()
	 */
	public void insertRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.insertRow();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#moveToCurrentRow()
	 */
	public void moveToCurrentRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.moveToCurrentRow();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#moveToInsertRow()
	 */
	public void moveToInsertRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.moveToInsertRow();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#refreshRow()
	 */
	public void refreshRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.refreshRow();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateRow()
	 */
	public void updateRow() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateRow();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#first()
	 */
	public boolean first() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.first());
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	public boolean isAfterLast() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.isAfterLast());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	public boolean isBeforeFirst() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.isBeforeFirst());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#isFirst()
	 */
	public boolean isFirst() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.isFirst());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#isLast()
	 */
	public boolean isLast() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.isLast());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#last()
	 */
	public boolean last() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.last());
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#next()
	 */
	public boolean next() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.next());
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#previous()
	 */
	public boolean previous() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.previous());
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#rowDeleted()
	 */
	public boolean rowDeleted() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.rowDeleted());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#rowInserted()
	 */
	public boolean rowInserted() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.rowInserted());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#rowUpdated()
	 */
	public boolean rowUpdated() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.rowUpdated());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#wasNull()
	 */
	public boolean wasNull() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.wasNull());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#getByte(int)
	 */
	public byte getByte(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Byte(resultSet.getByte(index));
			}
		};
		
		return ((Byte) this.executeRead(operation)).byteValue();
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	public double getDouble(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Double(resultSet.getDouble(index));
			}
		};
		
		return ((Double) this.executeRead(operation)).doubleValue();
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	public float getFloat(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Float(resultSet.getFloat(index));
			}
		};
		
		return ((Float) this.executeRead(operation)).floatValue();
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	public int getInt(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getInt(index));
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	public long getLong(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Long(resultSet.getLong(index));
			}
		};
		
		return ((Long) this.executeRead(operation)).longValue();
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	public short getShort(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Short(resultSet.getShort(index));
			}
		};
		
		return ((Short) this.executeRead(operation)).shortValue();
	}

	/**
	 * @see java.sql.ResultSet#setFetchDirection(int)
	 */
	public void setFetchDirection(final int direction) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.setFetchDirection(direction);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	public void setFetchSize(final int size) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.setFetchSize(size);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateNull(int)
	 */
	public void updateNull(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateNull(index);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#absolute(int)
	 */
	public boolean absolute(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.absolute(index));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	public boolean getBoolean(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.getBoolean(index));
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#relative(int)
	 */
	public boolean relative(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.relative(index));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#getBytes(int)
	 */
	public byte[] getBytes(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBytes(index);
			}
		};
		
		return (byte[]) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateByte(int, byte)
	 */
	public void updateByte(final int index, final byte value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateByte(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(int, double)
	 */
	public void updateDouble(final int index, final double value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateDouble(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(int, float)
	 */
	public void updateFloat(final int index, final float value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateFloat(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateInt(int, int)
	 */
	public void updateInt(final int index, final int value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateInt(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateLong(int, long)
	 */
	public void updateLong(final int index, final long value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateLong(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateShort(int, short)
	 */
	public void updateShort(final int index, final short value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateShort(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
	 */
	public void updateBoolean(final int index, final boolean value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBoolean(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(int, byte[])
	 */
	public void updateBytes(final int index, final byte[] value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBytes(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(int)
	 */
	public InputStream getAsciiStream(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getAsciiStream(index);
			}
		};
		
		return (InputStream) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(int)
	 */
	public InputStream getBinaryStream(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBinaryStream(index);
			}
		};
		
		return (InputStream) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 * @deprecated
	 */
	public InputStream getUnicodeStream(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getUnicodeStream(index);
			}
		};
		
		return (InputStream) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
	 */
	public void updateAsciiStream(final int index, final InputStream inputStream, final int length) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateAsciiStream(index, inputStream, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
	 */
	public void updateBinaryStream(final int index, final InputStream inputStream, final int length) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBinaryStream(index, inputStream, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(int)
	 */
	public Reader getCharacterStream(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getCharacterStream(index);
			}
		};
		
		return (Reader) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
	 */
	public void updateCharacterStream(final int index, final Reader reader, final int length) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateCharacterStream(index, reader, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(int)
	 */
	public Object getObject(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(index);
			}
		};
		
		return this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
	 */
	public void updateObject(final int index, final Object value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
	 */
	public void updateObject(final int index, final Object value, final int sqlType) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(index, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getCursorName()
	 */
	public String getCursorName() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getCursorName();
			}
		};
		
		return (String) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getString(int)
	 */
	public String getString(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getString(index);
			}
		};
		
		return (String) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
	 */
	public void updateString(final int index, final String value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateString(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	public byte getByte(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Byte(resultSet.getByte(name));
			}
		};
		
		return ((Byte) this.executeRead(operation)).byteValue();
	}

	/**
	 * @see java.sql.ResultSet#getDouble(java.lang.String)
	 */
	public double getDouble(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Double(resultSet.getDouble(name));
			}
		};
		
		return ((Double) this.executeRead(operation)).doubleValue();
	}

	/**
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	public float getFloat(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Float(resultSet.getFloat(name));
			}
		};
		
		return ((Float) this.executeRead(operation)).floatValue();
	}

	/**
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	public int findColumn(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.findColumn(name));
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	public int getInt(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Integer(resultSet.getInt(name));
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	public long getLong(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Long(resultSet.getLong(name));
			}
		};
		
		return ((Long) this.executeRead(operation)).longValue();
	}

	/**
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	public short getShort(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Short(resultSet.getShort(name));
			}
		};
		
		return ((Short) this.executeRead(operation)).shortValue();
	}

	/**
	 * @see java.sql.ResultSet#updateNull(java.lang.String)
	 */
	public void updateNull(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateNull(name);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return new Boolean(resultSet.getBoolean(name));
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see java.sql.ResultSet#getBytes(java.lang.String)
	 */
	public byte[] getBytes(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBytes(name);
			}
		};
		
		return (byte[]) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
	 */
	public void updateByte(final String name, final byte value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateByte(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
	 */
	public void updateDouble(final String name, final double value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateDouble(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
	 */
	public void updateFloat(final String name, final float value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateFloat(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
	 */
	public void updateInt(final String name, final int value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateInt(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
	 */
	public void updateLong(final String name, final long value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateLong(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
	 */
	public void updateShort(final String name, final short value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateShort(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
	 */
	public void updateBoolean(final String name, final boolean value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBoolean(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
	 */
	public void updateBytes(final String name, final byte[] value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBytes(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(index);
			}
		};
		
		return (BigDecimal) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(final int index, final int scale) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(index, scale);
			}
		};
		
		return (BigDecimal) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
	 */
	public void updateBigDecimal(final int index, final BigDecimal value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBigDecimal(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getURL(int)
	 */
	public URL getURL(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getURL(index);
			}
		};
		
		return (URL) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getArray(int)
	 */
	public Array getArray(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getArray(index);
			}
		};
		
		return (Array) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
	 */
	public void updateArray(final int index, final Array value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateArray(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBlob(int)
	 */
	public Blob getBlob(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBlob(index);
			}
		};
		
		return (Blob) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
	 */
	public void updateBlob(final int index, final Blob value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBlob(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getClob(int)
	 */
	public Clob getClob(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getClob(index);
			}
		};
		
		return (Clob) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
	 */
	public void updateClob(final int index, final Clob value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateClob(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(int)
	 */
	public Date getDate(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(index);
			}
		};
		
		return (Date) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
	 */
	public void updateDate(final int index, final Date value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateDate(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getRef(int)
	 */
	public Ref getRef(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getRef(index);
			}
		};
		
		return (Ref) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
	 */
	public void updateRef(final int index, final Ref value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateRef(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getMetaData();
			}
		};
		
		return (ResultSetMetaData) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getWarnings();
			}
		};
		
		return (SQLWarning) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getStatement()
	 */
	public Statement getStatement()
	{
		return this.statement;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int)
	 */
	public Time getTime(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(index);
			}
		};
		
		return (Time) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
	 */
	public void updateTime(final int index, final Time value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateTime(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	public Timestamp getTimestamp(final int index) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(index);
			}
		};
		
		return (Timestamp) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
	 */
	public void updateTimestamp(final int index, final Timestamp value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateTimestamp(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
	 */
	public InputStream getAsciiStream(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getAsciiStream(name);
			}
		};
		
		return (InputStream) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
	 */
	public InputStream getBinaryStream(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBinaryStream(name);
			}
		};
		
		return (InputStream) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
	 * @deprecated
	 */
	public InputStream getUnicodeStream(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getUnicodeStream(name);
			}
		};
		
		return (InputStream) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	public void updateAsciiStream(final String name, final InputStream inputStream, final int length) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateAsciiStream(name, inputStream, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	public void updateBinaryStream(final String name, final InputStream inputStream, final int length) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBinaryStream(name, inputStream, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
	 */
	public Reader getCharacterStream(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getCharacterStream(name);
			}
		};
		
		return (Reader) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	public void updateCharacterStream(final String name, final Reader reader, final int length) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateCharacterStream(name, reader, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	public Object getObject(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(name);
			}
		};
		
		return this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
	 */
	public void updateObject(final String name, final Object value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
	 */
	public void updateObject(final String name, final Object value, final int sqlType) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(name, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
	 */
	public Object getObject(final int index, final Map typeMap) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(index, typeMap);
			}
		};
		
		return this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	public String getString(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getString(name);
			}
		};
		
		return (String) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
	 */
	public void updateString(final String name, final String value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateString(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	public BigDecimal getBigDecimal(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(name);
			}
		};
		
		return (BigDecimal) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(final String name, final int scale) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(name, scale);
			}
		};
		
		return (BigDecimal) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	public void updateBigDecimal(final String name, final BigDecimal value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBigDecimal(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getURL(java.lang.String)
	 */
	public URL getURL(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getURL(name);
			}
		};
		
		return (URL) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getArray(java.lang.String)
	 */
	public Array getArray(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getArray(name);
			}
		};
		
		return (Array) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
	 */
	public void updateArray(final String name, final Array value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateArray(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBlob(java.lang.String)
	 */
	public Blob getBlob(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getBlob(name);
			}
		};
		
		return (Blob) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
	 */
	public void updateBlob(final String name, final Blob value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateBlob(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getClob(java.lang.String)
	 */
	public Clob getClob(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getClob(name);
			}
		};
		
		return (Clob) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
	 */
	public void updateClob(final String name, final Clob value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateClob(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	public Date getDate(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(name);
			}
		};
		
		return (Date) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
	 */
	public void updateDate(final String name, final Date value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateDate(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	public Date getDate(final int index, final Calendar calendar) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(index, calendar);
			}
		};
		
		return (Date) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getRef(java.lang.String)
	 */
	public Ref getRef(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getRef(name);
			}
		};
		
		return (Ref) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
	 */
	public void updateRef(final String name, final Ref value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateRef(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	public Time getTime(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(name);
			}
		};
		
		return (Time) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
	 */
	public void updateTime(final String name, final Time value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateTime(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	public Time getTime(final int index, final Calendar calendar) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(index, calendar);
			}
		};
		
		return (Time) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	public Timestamp getTimestamp(final String name) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(name);
			}
		};
		
		return (Timestamp) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	public void updateTimestamp(final String name, final Timestamp value) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				resultSet.updateTimestamp(name, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(final int index, final Calendar calendar) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(index, calendar);
			}
		};
		
		return (Timestamp) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
	 */
	public Object getObject(final String name, final Map typeMap) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(name, typeMap);
			}
		};
		
		return this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	public Date getDate(final String name, final Calendar calendar) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(name, calendar);
			}
		};
		
		return (Date) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	public Time getTime(final String name, final Calendar calendar) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(name, calendar);
			}
		};
		
		return (Time) this.executeRead(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	public Timestamp getTimestamp(final String name, final Calendar calendar) throws SQLException
	{
		ResultSetOperation operation = new ResultSetOperation()
		{
			public Object execute(ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(name, calendar);
			}
		};
		
		return (Timestamp) this.executeRead(operation);
	}
	
	protected abstract static class ResultSetOperation implements Operation
	{
		public abstract Object execute(ResultSet resultSet) throws SQLException;

		/**
		 * @see net.sf.hajdbc.AbstractProxy.Operation#execute(net.sf.hajdbc.ConnectionInfo, java.lang.Object)
		 */
		public Object execute(Database database, Object object) throws SQLException
		{
			return this.execute((ResultSet) object);
		}
	}

	/**
	 * @see net.sf.hajdbc.AbstractProxy#getDatabaseManager()
	 */
	protected DatabaseManager getDatabaseManager()
	{
		return this.statement.getDatabaseManager();
	}
	
	
	/**
	 * @see net.sf.hajdbc.AbstractProxy#drop(net.sf.hajdbc.ConnectionInfo)
	 */
	public void deactivate(Database database)
	{
		this.statement.deactivate(database);
		super.deactivate(database);
	}
}
