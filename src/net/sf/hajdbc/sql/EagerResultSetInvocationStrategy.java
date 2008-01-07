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

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class EagerResultSetInvocationStrategy<D, S extends Statement> extends TransactionalDatabaseWriteInvocationStrategy<D, S, ResultSet>
{
	private S statement;
	private FileSupport fileSupport;
	
	/**
	 * @param lockList
	 */
	public EagerResultSetInvocationStrategy(S statement, FileSupport fileSupport, Set<String> identifierSet)
	{
		super(identifierSet);
		
		this.statement = statement;
		this.fileSupport = fileSupport;
	}

	/**
	 * @param lockList
	 */
	public EagerResultSetInvocationStrategy(S statement, FileSupport fileSupport)
	{
		super();
		
		this.statement = statement;
		this.fileSupport = fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public ResultSet invoke(SQLProxy<D, S> proxy, Invoker<D, S, ResultSet> invoker) throws Exception
	{
		return ProxyFactory.createProxy(ResultSet.class, new ResultSetInvocationHandler<D, S>(this.statement, proxy, invoker, this.invokeAll(proxy, invoker), this.fileSupport));
	}
}
