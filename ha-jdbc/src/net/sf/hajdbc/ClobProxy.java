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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ClobProxy implements Clob
{
	private Clob clob;
	
	/**
	 * Constructs a new ClobProxy.
	 * 
	 */
	public ClobProxy(Clob clob)
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
	public long position(Clob pattern, long start) throws SQLException
	{
		return this.clob.position(pattern, start);
	}
	
	public static File createFile(Reader reader) throws SQLException
	{
		File file = null;
		
		try
		{
			file = File.createTempFile("ha-jdbc", "clob");
			
			Writer writer = new BufferedWriter(new FileWriter(file), 8192);
			char[] chunk = new char[8192];
			int byteCount = reader.read(chunk);
			
			while (byteCount >= 0)
			{
				writer.write(chunk, 0, byteCount);
				byteCount = reader.read(chunk);
			}
			
			writer.flush();
			writer.close();
			
			return file;
		}
		catch (IOException e)
		{
			if (file != null)
			{
				file.delete();
			}
			
			throw new SQLException(e.getMessage());
		}
	}
}
