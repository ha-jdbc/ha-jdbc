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

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;

/**
 * DatabaseMetaDataCache implementation that eagerly caches data when first flushed.
 * To be used when performance more of a concern than memory usage.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public class EagerDatabaseMetaDataCache<D> extends AbstractDatabaseMetaDataCache
{
	private volatile DatabaseProperties properties;
	private final Balancer<D> balancer;
	
	public EagerDatabaseMetaDataCache(Dialect dialect, Balancer<D> balancer)
	{
		super(dialect);

		this.balancer = balancer;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#flush()
	 */
	@Override
	public void flush() throws SQLException
	{
		Database<D> database = this.balancer.next();
		
		this.setDatabaseProperties(database.connect(database.createConnectionFactory()));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getDatabaseProperties(java.sql.Connection)
	 */
	@Override
	public synchronized DatabaseProperties getDatabaseProperties(Connection connection) throws SQLException
	{
		if (this.properties == null)
		{
			this.setDatabaseProperties(connection);
		}
		
		return this.properties;
	}
	
	private synchronized void setDatabaseProperties(Connection connection) throws SQLException
	{
		this.properties = new EagerDatabaseProperties(connection.getMetaData(), this.factory, this.dialect);
	}
}