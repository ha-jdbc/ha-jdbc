/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import net.sf.hajdbc.DatabaseProperties;


/**
 * DatabaseMetaDataCache that lazily caches data when requested, but only for a single thread.
 * To be used when memory usage is more of a concern than performance.
 * 
 * @author Paul Ferraro
 * @since 1.2
 */
public class ThreadLocalDatabaseMetaDataCache extends LazyDatabaseMetaDataCache
{
	private static ThreadLocal<DatabaseProperties> threadLocal = new ThreadLocal<DatabaseProperties>();

	/**
	 * @see net.sf.hajdbc.cache.AbstractLazyDatabaseMetaDataCache#getDatabaseProperties()
	 */
	@Override
	protected DatabaseProperties getDatabaseProperties()
	{
		return threadLocal.get();
	}

	/**
	 * @see net.sf.hajdbc.cache.AbstractLazyDatabaseMetaDataCache#setDatabaseProperties(net.sf.hajdbc.DatabaseProperties)
	 */
	@Override
	protected void setDatabaseProperties(DatabaseProperties properties)
	{
		threadLocal.set(properties);
	}
}
