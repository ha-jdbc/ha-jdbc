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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class MockPreparedStatement extends MockStatement implements PreparedStatement
{

	/**
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	public ResultSet executeQuery() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	public int executeUpdate() throws SQLException
	{
		return 0;
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(int arg0, int arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(int arg0, boolean arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(int arg0, byte arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(int arg0, short arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(int arg0, int arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(int arg0, long arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(int arg0, float arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(int arg0, double arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(int arg0, String arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	public void setBytes(int arg0, byte[] arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	public void setDate(int arg0, Date arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	public void setTime(int arg0, Time arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(int arg0, Timestamp arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
	 */
	public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
	 */
	public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
	 */
	public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters() throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
	 */
	public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	public void setObject(int arg0, Object arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(int arg0, Object arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#execute()
	 */
	public boolean execute() throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	public void addBatch() throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
	 */
	public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(int arg0, Ref arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(int arg0, Blob arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(int arg0, Clob arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(int arg0, Array arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)
	 */
	public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)
	 */
	public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
	 */
	public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(int arg0, int arg1, String arg2) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(int arg0, URL arg1) throws SQLException
	{
	}

	/**
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		return null;
	}

}
