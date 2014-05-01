/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.sql.CommonDataSourceDatabaseBuilder;

public class ConnectionPoolDataSourceDatabaseBuilder extends CommonDataSourceDatabaseBuilder<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	public ConnectionPoolDataSourceDatabaseBuilder(String id)
	{
		super(id, ConnectionPoolDataSource.class);
	}

	@Override
	public ConnectionPoolDataSourceDatabase build() throws SQLException
	{
		return new ConnectionPoolDataSourceDatabase(this.id, this.getDataSource(), this.credentials, this.weight, this.locality);
	}
}
