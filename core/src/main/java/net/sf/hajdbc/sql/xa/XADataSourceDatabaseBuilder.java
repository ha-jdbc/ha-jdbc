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
package net.sf.hajdbc.sql.xa;

import java.sql.SQLException;

import javax.sql.XADataSource;

import net.sf.hajdbc.sql.CommonDataSourceDatabaseBuilder;

public class XADataSourceDatabaseBuilder extends CommonDataSourceDatabaseBuilder<XADataSource, XADataSourceDatabase>
{
	private volatile boolean force2PC = false;
	
	public XADataSourceDatabaseBuilder(String id)
	{
		super(id, XADataSource.class);
	}

	public XADataSourceDatabaseBuilder force2PC(boolean force2PC)
	{
		this.force2PC = force2PC;
		return this;
	}

	@Override
	public XADataSourceDatabase build() throws SQLException
	{
		return new XADataSourceDatabase(this.id, this.getDataSource(), this.credentials, this.weight, this.locality, this.force2PC);
	}
}
