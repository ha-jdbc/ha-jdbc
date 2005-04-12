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
 * @author  Paul Ferraro
 * @since   1.0
 */
public class MockResultSet implements ResultSet
{

	/**
	 * @see java.sql.ResultSet#next()
	 */
	public boolean next() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#close()
	 */
	public void close() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#wasNull()
	 */
	public boolean wasNull() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#getString(int)
	 */
	public String getString(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	public boolean getBoolean(int arg0) throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#getByte(int)
	 */
	public byte getByte(int arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	public short getShort(int arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	public int getInt(int arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	public long getLong(int arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	public float getFloat(int arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	public double getDouble(int arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 */
	public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBytes(int)
	 */
	public byte[] getBytes(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getDate(int)
	 */
	public Date getDate(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int)
	 */
	public Time getTime(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	public Timestamp getTimestamp(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(int)
	 */
	public InputStream getAsciiStream(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 */
	public InputStream getUnicodeStream(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(int)
	 */
	public InputStream getBinaryStream(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	public String getString(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(String arg0) throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	public byte getByte(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	public short getShort(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	public int getInt(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	public long getLong(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	public float getFloat(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getDouble(java.lang.String)
	 */
	public double getDouble(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
	 */
	public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBytes(java.lang.String)
	 */
	public byte[] getBytes(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	public Date getDate(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	public Time getTime(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	public Timestamp getTimestamp(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
	 */
	public InputStream getAsciiStream(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
	 */
	public InputStream getUnicodeStream(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
	 */
	public InputStream getBinaryStream(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#getCursorName()
	 */
	public String getCursorName() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getObject(int)
	 */
	public Object getObject(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	public Object getObject(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	public int findColumn(String arg0) throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(int)
	 */
	public Reader getCharacterStream(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
	 */
	public Reader getCharacterStream(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	public BigDecimal getBigDecimal(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	public boolean isBeforeFirst() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	public boolean isAfterLast() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#isFirst()
	 */
	public boolean isFirst() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#isLast()
	 */
	public boolean isLast() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#beforeFirst()
	 */
	public void beforeFirst() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#afterLast()
	 */
	public void afterLast() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#first()
	 */
	public boolean first() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#last()
	 */
	public boolean last() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#getRow()
	 */
	public int getRow() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#absolute(int)
	 */
	public boolean absolute(int arg0) throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#relative(int)
	 */
	public boolean relative(int arg0) throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#previous()
	 */
	public boolean previous() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#setFetchDirection(int)
	 */
	public void setFetchDirection(int arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	public void setFetchSize(int arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	public int getFetchSize() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	public int getType() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	public int getConcurrency() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.ResultSet#rowUpdated()
	 */
	public boolean rowUpdated() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#rowInserted()
	 */
	public boolean rowInserted() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#rowDeleted()
	 */
	public boolean rowDeleted() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.ResultSet#updateNull(int)
	 */
	public void updateNull(int arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
	 */
	public void updateBoolean(int arg0, boolean arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateByte(int, byte)
	 */
	public void updateByte(int arg0, byte arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateShort(int, short)
	 */
	public void updateShort(int arg0, short arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateInt(int, int)
	 */
	public void updateInt(int arg0, int arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateLong(int, long)
	 */
	public void updateLong(int arg0, long arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(int, float)
	 */
	public void updateFloat(int arg0, float arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(int, double)
	 */
	public void updateDouble(int arg0, double arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
	 */
	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
	 */
	public void updateString(int arg0, String arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(int, byte[])
	 */
	public void updateBytes(int arg0, byte[] arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
	 */
	public void updateDate(int arg0, Date arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
	 */
	public void updateTime(int arg0, Time arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
	 */
	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
	 */
	public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
	 */
	public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
	 */
	public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
	 */
	public void updateObject(int arg0, Object arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
	 */
	public void updateObject(int arg0, Object arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateNull(java.lang.String)
	 */
	public void updateNull(String arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
	 */
	public void updateBoolean(String arg0, boolean arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
	 */
	public void updateByte(String arg0, byte arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
	 */
	public void updateShort(String arg0, short arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
	 */
	public void updateInt(String arg0, int arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
	 */
	public void updateLong(String arg0, long arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
	 */
	public void updateFloat(String arg0, float arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
	 */
	public void updateDouble(String arg0, double arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
	 */
	public void updateString(String arg0, String arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
	 */
	public void updateBytes(String arg0, byte[] arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
	 */
	public void updateDate(String arg0, Date arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
	 */
	public void updateTime(String arg0, Time arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
	 */
	public void updateObject(String arg0, Object arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
	 */
	public void updateObject(String arg0, Object arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#insertRow()
	 */
	public void insertRow() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateRow()
	 */
	public void updateRow() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#deleteRow()
	 */
	public void deleteRow() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#refreshRow()
	 */
	public void refreshRow() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#cancelRowUpdates()
	 */
	public void cancelRowUpdates() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#moveToInsertRow()
	 */
	public void moveToInsertRow() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#moveToCurrentRow()
	 */
	public void moveToCurrentRow() throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#getStatement()
	 */
	public Statement getStatement() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
	 */
	public Object getObject(int arg0, Map arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getRef(int)
	 */
	public Ref getRef(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBlob(int)
	 */
	public Blob getBlob(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getClob(int)
	 */
	public Clob getClob(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getArray(int)
	 */
	public Array getArray(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
	 */
	public Object getObject(String arg0, Map arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getRef(java.lang.String)
	 */
	public Ref getRef(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getBlob(java.lang.String)
	 */
	public Blob getBlob(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getClob(java.lang.String)
	 */
	public Clob getClob(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getArray(java.lang.String)
	 */
	public Array getArray(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	public Date getDate(int arg0, Calendar arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	public Date getDate(String arg0, Calendar arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	public Time getTime(int arg0, Calendar arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	public Time getTime(String arg0, Calendar arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getURL(int)
	 */
	public URL getURL(int arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#getURL(java.lang.String)
	 */
	public URL getURL(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
	 */
	public void updateRef(int arg0, Ref arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
	 */
	public void updateRef(String arg0, Ref arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
	 */
	public void updateBlob(int arg0, Blob arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
	 */
	public void updateBlob(String arg0, Blob arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
	 */
	public void updateClob(int arg0, Clob arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
	 */
	public void updateClob(String arg0, Clob arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
	 */
	public void updateArray(int arg0, Array arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
	 */
	public void updateArray(String arg0, Array arg1) throws SQLException
	{
	}

}
