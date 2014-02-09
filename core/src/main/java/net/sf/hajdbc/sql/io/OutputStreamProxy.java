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
import java.io.OutputStream;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.Invoker;

/**
 * Static proxy for OutputStreams
 * @author Paul Ferraro
 * @param <Z> connection source
 * @param <D> database
 * @param <P> parent type
 */
public class OutputStreamProxy<Z, D extends Database<Z>, P> extends OutputStream
{
	private final OutputStreamProxyFactory<Z, D, P> factory;
	
	public OutputStreamProxy(OutputStreamProxyFactory<Z, D, P> factory)
	{
		this.factory = factory;
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException
	{
		OutputStreamInvoker<Z, D> invoker = new OutputStreamInvoker<Z, D>()
		{
			@Override
			public Void invoke(D database, OutputStream output) throws IOException
			{
				output.write(b, off, len);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void write(final byte[] b) throws IOException
	{
		OutputStreamInvoker<Z, D> invoker = new OutputStreamInvoker<Z, D>()
		{
			@Override
			public Void invoke(D database, OutputStream output) throws IOException
			{
				output.write(b);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void write(final int b) throws IOException
	{
		OutputStreamInvoker<Z, D> invoker = new OutputStreamInvoker<Z, D>()
		{
			@Override
			public Void invoke(D database, OutputStream output) throws IOException
			{
				output.write(b);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void flush() throws IOException
	{
		OutputStreamInvoker<Z, D> invoker = new OutputStreamInvoker<Z, D>()
		{
			@Override
			public Void invoke(D database, OutputStream output) throws IOException
			{
				output.flush();
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void close() throws IOException
	{
		OutputStreamInvoker<Z, D> invoker = new OutputStreamInvoker<Z, D>()
		{
			@Override
			public Void invoke(D database, OutputStream output) throws IOException
			{
				output.close();
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.remove();
	}

	private interface OutputStreamInvoker<Z, D extends Database<Z>> extends Invoker<Z, D, OutputStream, Void, IOException>
	{
	}
}
