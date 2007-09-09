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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.DatabaseProperties;


/**
 * DatabaseMetaDataCache implementation that lazily caches data when requested.
 * Used when a compromise between memory usage and performance is desired.
 * Caches DatabaseProperties using a soft reference to prevent <code>OutOfMemoryError</code>s.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public class LazyDatabaseMetaDataCache extends AbstractDatabaseMetaDataCache
{
	private Reference<DatabaseProperties> propertiesRef = new SoftReference<DatabaseProperties>(null);
	
	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#flush(DatabaseMetaData)
	 */
	@Override
	public synchronized void flush(DatabaseMetaData metaData)
	{
		this.propertiesRef.clear();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getDatabaseProperties(DatabaseMetaData)
	 */
	@Override
	public synchronized DatabaseProperties getDatabaseProperties(DatabaseMetaData metaData) throws SQLException
	{
		LazyDatabaseProperties.setDatabaseMetaData(metaData);
		
		DatabaseProperties properties = this.propertiesRef.get();
		
		if (properties == null)
		{
			properties = new LazyDatabaseProperties(this.dialect);
		
			this.propertiesRef = new SoftReference<DatabaseProperties>(properties);
		}
		
		return properties;
	}
}
