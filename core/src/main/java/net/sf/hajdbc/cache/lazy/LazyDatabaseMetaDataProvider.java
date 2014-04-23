/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.cache.lazy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.cache.DatabaseMetaDataProvider;

/**
 * @author Paul Ferraro
 *
 */
public class LazyDatabaseMetaDataProvider implements DatabaseMetaDataProvider
{
	private final ThreadLocal<Connection> threadLocal = new ThreadLocal<>();
	
	public LazyDatabaseMetaDataProvider(DatabaseMetaData metaData) throws SQLException
	{
		this.setConnection(metaData.getConnection());
	}
	
	public void setConnection(Connection connection)
	{
		this.threadLocal.set(connection);
	}
	
	/**
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataProvider#getDatabaseMetaData()
	 */
	@Override
	public DatabaseMetaData getDatabaseMetaData() throws SQLException
	{
		return this.threadLocal.get().getMetaData();
	}
}
