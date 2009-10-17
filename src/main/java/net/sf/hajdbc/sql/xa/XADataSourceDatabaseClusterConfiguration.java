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
package net.sf.hajdbc.sql.xa;

import javax.sql.XADataSource;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import net.sf.hajdbc.sql.AbstractDatabaseClusterConfiguration;

@XmlRootElement(name = "ha-jdbc")
@XmlAccessorType(XmlAccessType.FIELD)
public class XADataSourceDatabaseClusterConfiguration extends AbstractDatabaseClusterConfiguration<XADataSource, XADataSourceDatabase>
{
	@XmlElement(name = "cluster", required = true)
	private XADataSourceNestedConfiguration configuration;

	@Override
	protected AbstractDatabaseClusterConfiguration.NestedConfiguration<XADataSource, XADataSourceDatabase> getNestedConfiguration()
	{
		return this.configuration;
	}

	static class XADataSourceNestedConfiguration extends AbstractDatabaseClusterConfiguration.NestedConfiguration<XADataSource, XADataSourceDatabase>
	{
		@SuppressWarnings("unused")
		@XmlElement(name = "database")
		private XADataSourceDatabase[] getDatabases()
		{
			return this.getDatabaseMap().values().toArray(new XADataSourceDatabase[this.getDatabaseMap().size()]);
		}
		
		@SuppressWarnings("unused")
		private void setDatabases(XADataSourceDatabase[] databases)
		{
			for (XADataSourceDatabase database: databases)
			{
				this.getDatabaseMap().put(database.getId(), database);
			}
		}
	}
}
