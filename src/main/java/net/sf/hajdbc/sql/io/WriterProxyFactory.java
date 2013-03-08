/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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
package net.sf.hajdbc.sql.io;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.sql.ProxyFactory;

/**
 * 
 * @author Paul Ferraro
 */
public class WriterProxyFactory<Z, D extends Database<Z>, P> extends OutputProxyFactory<Z, D, P, Writer>
{
	public WriterProxyFactory(P parentProxy, ProxyFactory<Z, D, P, SQLException> parent, Invoker<Z, D, P, Writer, SQLException> invoker, Map<D, Writer> writers)
	{
		super(parentProxy, parent, invoker, writers);
	}
	
	@Override
	public void close(D database, Writer writer)
	{
		try
		{
			writer.close();
		}
		catch (IOException e)
		{
			this.logger.log(Level.INFO, e);
		}
	}

	@Override
	public Writer createProxy()
	{
		return new WriterProxy<Z, D, P>(this);
	}
}
