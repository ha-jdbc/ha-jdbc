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

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.Invoker;

/**
 * Static proxy for writers.
 * @author Paul Ferraro
 * @param <Z> connection source
 * @param <D> database
 * @param <P> parent type
 */
public class WriterProxy<Z, D extends Database<Z>, P> extends Writer
{
	private final WriterProxyFactory<Z, D, P> factory;
	
	public WriterProxy(WriterProxyFactory<Z, D, P> factory)
	{
		this.factory = factory;
	}
	
	@Override
	public void write(final int c) throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.write(c);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void write(final char[] cbuf) throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.write(cbuf);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void write(final String str) throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.write(str);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void write(final String str, final int off, final int len) throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.write(str, off, len);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public Writer append(final CharSequence csq) throws IOException
	{
		WriterInvoker<Z, D, Writer> invoker = new WriterInvoker<Z, D, Writer>()
		{
			@Override
			public Writer invoke(D database, Writer writer) throws IOException
			{
				return writer.append(csq);
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
		
		return this;
	}

	@Override
	public Writer append(final CharSequence csq, final int start, final int end) throws IOException
	{
		WriterInvoker<Z, D, Writer> invoker = new WriterInvoker<Z, D, Writer>()
		{
			@Override
			public Writer invoke(D database, Writer writer) throws IOException
			{
				return writer.append(csq, start, end);
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
		
		return this;
	}

	@Override
	public Writer append(final char c) throws IOException
	{
		WriterInvoker<Z, D, Writer> invoker = new WriterInvoker<Z, D, Writer>()
		{
			@Override
			public Writer invoke(D database, Writer writer) throws IOException
			{
				return writer.append(c);
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
		
		return this;
	}

	@Override
	public void write(final char[] cbuf, final int off, final int len) throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.write(cbuf, off, len);
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void flush() throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.flush();
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.record(invoker);
	}

	@Override
	public void close() throws IOException
	{
		WriterInvoker<Z, D, Void> invoker = new WriterInvoker<Z, D, Void>()
		{
			@Override
			public Void invoke(D database, Writer writer) throws IOException
			{
				writer.close();
				return null;
			}
		};
		
		InvocationStrategies.INVOKE_ON_EXISTING.invoke(this.factory, invoker);
		
		this.factory.remove();
	}

	private interface WriterInvoker<Z, D extends Database<Z>, R> extends Invoker<Z, D, Writer, R, IOException>
	{
	}
}
