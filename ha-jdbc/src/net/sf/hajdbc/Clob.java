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
package net.sf.hajdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class Clob implements java.sql.Clob
{
	private java.sql.Clob clob;
	
	/**
	 * Constructs a new Clob.
	 * @param clob a character large object
	 */
	public Clob(java.sql.Clob clob)
	{
		this.clob = clob;
	}

	/**
	 * @see java.sql.Clob#length()
	 */
	public long length() throws SQLException
	{
		return this.clob.length();
	}

	/**
	 * @see java.sql.Clob#truncate(long)
	 */
	public void truncate(long length) throws SQLException
	{
		this.clob.truncate(length);
	}

	/**
	 * @see java.sql.Clob#getAsciiStream()
	 */
	public InputStream getAsciiStream() throws SQLException
	{
		return this.clob.getAsciiStream();
	}

	/**
	 * @see java.sql.Clob#setAsciiStream(long)
	 */
	public OutputStream setAsciiStream(long position) throws SQLException
	{
		return this.clob.setAsciiStream(position);
	}

	/**
	 * @see java.sql.Clob#getCharacterStream()
	 */
	public Reader getCharacterStream() throws SQLException
	{
		return this.clob.getCharacterStream();
	}

	/**
	 * @see java.sql.Clob#setCharacterStream(long)
	 */
	public Writer setCharacterStream(long position) throws SQLException
	{
		return this.clob.setCharacterStream(position);
	}

	/**
	 * @see java.sql.Clob#getSubString(long, int)
	 */
	public String getSubString(long position, int length) throws SQLException
	{
		return this.clob.getSubString(position, length);
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String)
	 */
	public int setString(long position, String string) throws SQLException
	{
		return this.clob.setString(position, string);
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String, int, int)
	 */
	public int setString(long position, String string, int offset, int length) throws SQLException
	{
		return this.clob.setString(position, string, offset, length);
	}

	/**
	 * @see java.sql.Clob#position(java.lang.String, long)
	 */
	public long position(String pattern, long start) throws SQLException
	{
		return this.clob.position(pattern, start);
	}

	/**
	 * @see java.sql.Clob#position(java.sql.Clob, long)
	 */
	public long position(java.sql.Clob pattern, long start) throws SQLException
	{
		return this.clob.position(pattern, start);
	}
}
