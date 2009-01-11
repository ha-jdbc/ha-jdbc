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
package net.sf.hajdbc.cache;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;


/**
 * DatabaseMetaDataCache implementation that lazily caches data when requested.
 * Used when a compromise between memory usage and performance is desired.
 * Caches DatabaseProperties using a soft reference to prevent <code>OutOfMemoryError</code>s.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public class LazyDatabaseMetaDataCache implements DatabaseMetaDataCache
{
	private volatile Reference<LazyDatabaseProperties> propertiesRef = new SoftReference<LazyDatabaseProperties>(null);
	private final Dialect dialect;
	
	public LazyDatabaseMetaDataCache(Dialect dialect)
	{
		this.dialect = dialect;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#flush()
	 */
	@Override
	public void flush()
	{
		this.propertiesRef.clear();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getDatabaseProperties(java.sql.Connection)
	 */
	@Override
	public synchronized DatabaseProperties getDatabaseProperties(Connection connection) throws SQLException
	{
		LazyDatabaseProperties properties = this.propertiesRef.get();
		
		if (properties == null)
		{
			properties = new LazyDatabaseProperties(connection.getMetaData(), this.dialect);
		
			this.propertiesRef = new SoftReference<LazyDatabaseProperties>(properties);
		}
		else
		{
			properties.setConnection(connection);
		}
		
		return properties;
	}
}
