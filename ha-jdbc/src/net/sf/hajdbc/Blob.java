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
import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class Blob implements java.sql.Blob
{
	private java.sql.Blob blob;
	
	/**
	 * Constructs a new Blob.
	 * @param blob a binary large object
	 */
	public Blob(java.sql.Blob blob)
	{
		this.blob = blob;
	}

	/**
	 * @see java.sql.Blob#length()
	 */
	public long length() throws SQLException
	{
		return this.blob.length();
	}

	/**
	 * @see java.sql.Blob#truncate(long)
	 */
	public void truncate(long length) throws SQLException
	{
		this.truncate(length);
	}

	/**
	 * @see java.sql.Blob#getBytes(long, int)
	 */
	public byte[] getBytes(long position, int length) throws SQLException
	{
		return this.blob.getBytes(position, length);
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[])
	 */
	public int setBytes(long position, byte[] bytes) throws SQLException
	{
		return this.blob.setBytes(position, bytes);
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[], int, int)
	 */
	public int setBytes(long position, byte[] bytes, int offset, int length) throws SQLException
	{
		return this.setBytes(position, bytes, offset, length);
	}

	/**
	 * @see java.sql.Blob#position(byte[], long)
	 */
	public long position(byte[] pattern, long start) throws SQLException
	{
		return this.blob.position(pattern, start);
	}

	/**
	 * @see java.sql.Blob#getBinaryStream()
	 */
	public InputStream getBinaryStream() throws SQLException
	{
		return this.blob.getBinaryStream();
	}

	/**
	 * @see java.sql.Blob#setBinaryStream(long)
	 */
	public OutputStream setBinaryStream(long position) throws SQLException
	{
		return this.blob.setBinaryStream(position);
	}

	/**
	 * @see java.sql.Blob#position(java.sql.Blob, long)
	 */
	public long position(java.sql.Blob pattern, long start) throws SQLException
	{
		return this.blob.position(pattern, start);
	}
}
