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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.sql.AbstractDatabaseClusterConfiguration;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;
import net.sf.hajdbc.tx.UUIDTransactionIdentifierFactory;

@XmlRootElement(name = "ha-jdbc")
@XmlType(name = "databaseClusterConfiguration")
public class XADataSourceDatabaseClusterConfiguration extends AbstractDatabaseClusterConfiguration<XADataSource, XADataSourceDatabase>
{
	private static final long serialVersionUID = 6548016448539963613L;
	
	@XmlElement(name = "cluster", required = true)
	private XADataSourceNestedConfiguration configuration = new XADataSourceNestedConfiguration();

	@Override
	protected AbstractDatabaseClusterConfiguration.NestedConfiguration<XADataSource, XADataSourceDatabase> getNestedConfiguration()
	{
		return this.configuration;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractDatabaseClusterConfiguration#getTransactionIdentifierFactory()
	 */
	@Override
	public TransactionIdentifierFactory<? extends Object> getTransactionIdentifierFactory()
	{
		return new UUIDTransactionIdentifierFactory();
	}

	@XmlType(name = "nestedConfiguration")
	static class XADataSourceNestedConfiguration extends AbstractDatabaseClusterConfiguration.NestedConfiguration<XADataSource, XADataSourceDatabase>
	{
		private static final long serialVersionUID = 8096563929212126538L;

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
