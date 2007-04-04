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
import java.sql.Blob;

/**
 * @author Paul Ferraro
 *
 */
public class MockBlob implements Blob
{
	/**
	 * @see java.sql.Blob#free()
	 */
	public void free()
	{
	}

	/**
	 * @see java.sql.Blob#getBinaryStream()
	 */
	public InputStream getBinaryStream()
	{
		return null;
	}

	/**
	 * @see java.sql.Blob#getBinaryStream(long, long)
	 */
	public InputStream getBinaryStream(long arg0, long arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Blob#getBytes(long, int)
	 */
	public byte[] getBytes(long arg0, int arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Blob#length()
	 */
	public long length()
	{
		return 0;
	}

	/**
	 * @see java.sql.Blob#position(byte[], long)
	 */
	public long position(byte[] arg0, long arg1)
	{
		return 0;
	}

	/**
	 * @see java.sql.Blob#position(java.sql.Blob, long)
	 */
	public long position(Blob arg0, long arg1)
	{
		return 0;
	}

	/**
	 * @see java.sql.Blob#setBinaryStream(long)
	 */
	public OutputStream setBinaryStream(long arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[])
	 */
	public int setBytes(long arg0, byte[] arg1)
	{
		return 0;
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[], int, int)
	 */
	public int setBytes(long arg0, byte[] arg1, int arg2, int arg3)
	{
		return 0;
	}

	/**
	 * @see java.sql.Blob#truncate(long)
	 */
	public void truncate(long arg0)
	{
	}
}
