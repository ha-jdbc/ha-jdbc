/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;

/**
 * @author Paul Ferraro
 *
 */
public class MockClob implements NClob
{

	/**
	 * @see java.sql.Clob#free()
	 */
	public void free()
	{
	}

	/**
	 * @see java.sql.Clob#getAsciiStream()
	 */
	public InputStream getAsciiStream()
	{
		return null;
	}

	/**
	 * @see java.sql.Clob#getCharacterStream()
	 */
	public Reader getCharacterStream()
	{
		return new StringReader("");
	}

	/**
	 * @see java.sql.Clob#getCharacterStream(long, long)
	 */
	public Reader getCharacterStream(long arg0, long arg1)
	{
		return new StringReader("");
	}

	/**
	 * @see java.sql.Clob#getSubString(long, int)
	 */
	public String getSubString(long arg0, int arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Clob#length()
	 */
	public long length()
	{
		return 0;
	}

	/**
	 * @see java.sql.Clob#position(java.lang.String, long)
	 */
	public long position(String arg0, long arg1)
	{
		return 0;
	}

	/**
	 * @see java.sql.Clob#position(java.sql.Clob, long)
	 */
	public long position(Clob arg0, long arg1)
	{
		return 0;
	}

	/**
	 * @see java.sql.Clob#setAsciiStream(long)
	 */
	public OutputStream setAsciiStream(long arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Clob#setCharacterStream(long)
	 */
	public Writer setCharacterStream(long arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String)
	 */
	public int setString(long arg0, String arg1)
	{
		return 0;
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String, int, int)
	 */
	public int setString(long arg0, String arg1, int arg2, int arg3)
	{
		return 0;
	}

	/**
	 * @see java.sql.Clob#truncate(long)
	 */
	public void truncate(long arg0)
	{
	}
}
