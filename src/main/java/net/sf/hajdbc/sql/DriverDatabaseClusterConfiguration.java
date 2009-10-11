/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.Driver;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author paul
 *
 */
@XmlRootElement(name = "ha-jdbc")
@XmlAccessorType(XmlAccessType.FIELD)
public class DriverDatabaseClusterConfiguration extends AbstractDatabaseClusterConfiguration<Driver, DriverDatabase>
{
	@XmlElement(name = "cluster", required = true)
	private DriverNestedConfiguration configuration;

	@Override
	protected AbstractDatabaseClusterConfiguration.NestedConfiguration<Driver, DriverDatabase> getNestedConfiguration()
	{
		return this.configuration;
	}
	
	static class DriverNestedConfiguration extends AbstractDatabaseClusterConfiguration.NestedConfiguration<Driver, DriverDatabase>
	{
		@SuppressWarnings("unused")
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
	}
}
