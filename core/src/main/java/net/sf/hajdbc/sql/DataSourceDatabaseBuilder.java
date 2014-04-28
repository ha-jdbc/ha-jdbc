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
package net.sf.hajdbc.sql;

import java.sql.SQLException;

import javax.sql.DataSource;

public class DataSourceDatabaseBuilder extends CommonDataSourceDatabaseBuilder<DataSource, DataSourceDatabase>
{
	public DataSourceDatabaseBuilder(String id)
	{
		super(id, DataSource.class);
	}

	@Override
	public DataSourceDatabase build() throws SQLException
	{
		return new DataSourceDatabase(this.id, this.getDataSource(), this.credentials, this.weight, this.locality);
	}
}
