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
package net.sf.hajdbc.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A sink type independent registry for input sinks.
 * @author Paul Ferraro
 * @param <S> sink type
 */
public class InputSinkRegistryImpl<S> implements InputSinkRegistry<S>
{
	private final List<S> sinks = new LinkedList<>();
	private final Map<Class<?>, InputSinkChannel<?, S>> channels = new HashMap<>();
	private final InputSinkStrategy<S> strategy;
	
	public InputSinkRegistryImpl(InputSinkStrategy<S> strategy)
	{
		this.addInputChannel(InputStream.class, strategy.createInputStreamChannel());
		this.addInputChannel(Reader.class, strategy.createReaderChannel());
		this.strategy = strategy;
	}
	
	private <I> void addInputChannel(Class<I> inputClass, InputSinkChannel<I, S> channel)
	{
		this.channels.put(inputClass, channel);
	}
	
	S addInputSink(S sink)
	{
		this.sinks.add(sink);
		return sink;
	}
	
	@Override
	public <I> InputSinkChannel<I, S> get(Class<I> inputClass)
	{
		final InputSinkChannel<I, S> channel = (InputSinkChannel<I, S>) this.channels.get(inputClass);
		
		return (channel == null) ? null : new InputSinkChannel<I, S>()
		{
			@Override
			public S write(I input) throws IOException
			{
				return InputSinkRegistryImpl.this.addInputSink(channel.write(input));
			}

			@Override
			public I read(S sink) throws IOException
			{
				return channel.read(sink);
			}	
		};
	}

	@Override
	public void close()
	{
		for (S sink: this.sinks)
		{
			this.strategy.close(sink);
		}
		this.sinks.clear();
	}
}
