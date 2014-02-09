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
package net.sf.hajdbc.sql;

import java.sql.Driver;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author paul
 *
 */
@XmlRootElement(name = "ha-jdbc")
@XmlType(name = "databaseClusterConfiguration")
public class DriverDatabaseClusterConfiguration extends AbstractDatabaseClusterConfiguration<Driver, DriverDatabase>
{
	private static final long serialVersionUID = -1244943642980298325L;
	
	@XmlElement(name = "cluster", required = true)
	private DriverNestedConfiguration configuration = new DriverNestedConfiguration();

	@Override
	protected AbstractDatabaseClusterConfiguration.NestedConfiguration<Driver, DriverDatabase> getNestedConfiguration()
	{
		return this.configuration;
	}
	
	@XmlType(name = "nestedConfiguration")
	static class DriverNestedConfiguration extends AbstractDatabaseClusterConfiguration.NestedConfiguration<Driver, DriverDatabase>
	{
		private static final long serialVersionUID = 6162775670140013194L;

		@XmlElement(name = "database")
		private DriverDatabase[] getDatabases()
		{
			return this.getDatabaseMap().values().toArray(new DriverDatabase[this.getDatabaseMap().size()]);
		}
		
		@SuppressWarnings("unused")
		private void setDatabases(DriverDatabase[] databases)
		{
			for (DriverDatabase database: databases)
			{
				this.getDatabaseMap().put(database.getId(), database);
			}
		}

		@Override
		public DriverDatabase createDatabase(String id)
		{
			DriverDatabase database = new DriverDatabase();
			database.setId(id);
			return database;
		}
	}
}
