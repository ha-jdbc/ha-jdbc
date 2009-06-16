/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.sql.SQLException;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public abstract class AbstractRootInvocationHandler<D> extends AbstractInvocationHandler<D, D>
{
	/**
	 * Constructs a new AbstractRootInvocationHandler.
	 * @param databaseCluster
	 * @param proxyClass
	 */
	protected AbstractRootInvocationHandler(DatabaseCluster<D> databaseCluster, Class<D> proxyClass)
	{
		super(databaseCluster, proxyClass, new TreeMap<Database<D>, D>());
		
		for (Database<D> database: databaseCluster.getBalancer().all())
		{
			this.getObject(database);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	protected void close(Database<D> database, D object)
	{
		// Nothing to close
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#createObject(net.sf.hajdbc.Database)
	 */
	@Override
	protected D createObject(Database<D> database) throws SQLException
	{
		return database.createConnectionFactory();
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getRoot()
	 */
	@Override
	public SQLProxy<D, ?> getRoot()
	{
		return this;
	}
}
