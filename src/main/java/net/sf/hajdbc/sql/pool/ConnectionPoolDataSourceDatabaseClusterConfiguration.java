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
package net.sf.hajdbc.sql.pool;

import javax.sql.ConnectionPoolDataSource;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.sql.AbstractDatabaseClusterConfiguration;

/**
 * @author Paul Ferraro
 */
@XmlRootElement(name = "ha-jdbc")
@XmlType(name = "databaseClusterConfiguration")
public class ConnectionPoolDataSourceDatabaseClusterConfiguration extends AbstractDatabaseClusterConfiguration<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	private static final long serialVersionUID = 6137340553506862286L;
	
	@XmlElement(name = "cluster", required = true)
	private ConnectionPoolDataSourceNestedConfiguration configuration = new ConnectionPoolDataSourceNestedConfiguration();

	@Override
	protected AbstractDatabaseClusterConfiguration.NestedConfiguration<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> getNestedConfiguration()
	{
		return this.configuration;
	}

	@XmlType(name = "nestedConfiguration")
	static class ConnectionPoolDataSourceNestedConfiguration extends AbstractDatabaseClusterConfiguration.NestedConfiguration<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
	{
		private static final long serialVersionUID = 3139842065941633881L;

		@XmlElement(name = "database")
		private ConnectionPoolDataSourceDatabase[] getDatabases()
		{
			return this.getDatabaseMap().values().toArray(new ConnectionPoolDataSourceDatabase[this.getDatabaseMap().size()]);
		}
		
		@SuppressWarnings("unused")
		private void setDatabases(ConnectionPoolDataSourceDatabase[] databases)
		{
			for (ConnectionPoolDataSourceDatabase database: databases)
			{
				this.getDatabaseMap().put(database.getId(), database);
			}
		}

		@Override
		public ConnectionPoolDataSourceDatabase createDatabase(String id)
		{
			ConnectionPoolDataSourceDatabase database = new ConnectionPoolDataSourceDatabase();
			database.setId(id);
			return database;
		}
	}
}
