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

import java.lang.reflect.InvocationHandler;

import javax.sql.XADataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.CommonDataSourceFactory;

/**
 * @author Paul Ferraro
 */
public class XADataSourceFactory extends CommonDataSourceFactory<XADataSource, XADataSourceDatabase>
{
	/**
	 * Constructs a new factory for creating an <code>XADataSource</code>.
	 */
	public XADataSourceFactory()
	{
		super(XADataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.CommonDataSourceFactory#createInvocationHandler(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public InvocationHandler createInvocationHandler(DatabaseCluster<XADataSource, XADataSourceDatabase> cluster)
	{
		return new XADataSourceInvocationHandler(cluster);
	}
}
