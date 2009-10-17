/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.sql.AbstractDatabaseClusterConfiguration;

/**
 * @author Paul Ferraro
 */
@XmlRootElement(name = "ha-jdbc")
@XmlType(name = "databaseClusterConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConnectionPoolDataSourceDatabaseClusterConfiguration extends AbstractDatabaseClusterConfiguration<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	@XmlElement(name = "cluster", required = true)
	private ConnectionPoolDataSourceNestedConfiguration configuration;

	@Override
	protected AbstractDatabaseClusterConfiguration.NestedConfiguration<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> getNestedConfiguration()
	{
		return this.configuration;
	}

	@XmlType(name = "nestedDatabaseClusterConfiguration")
	static class ConnectionPoolDataSourceNestedConfiguration extends AbstractDatabaseClusterConfiguration.NestedConfiguration<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
	{
		@SuppressWarnings("unused")
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
	}
}
