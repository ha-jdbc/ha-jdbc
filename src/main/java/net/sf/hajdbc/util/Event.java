/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.EventObject;

/**
 * @author Paul Ferraro
 */
public class Event<T> extends EventObject
{
	private static final long serialVersionUID = 1032680699193401944L;

	public Event(T source)
	{
		super(source);
	}

	@Override
	public T getSource()
	{
		return (T) this.source;
	}

	@Override
	public boolean equals(Object object)
	{
		return ((object != null) && this.getClass().isInstance(object)) ? this.source.equals(this.getClass().cast(object).source) : false;
	}

	@Override
	public int hashCode()
	{
		return this.source.hashCode();
	}

	@Override
	public String toString()
	{
		return this.source.toString();
	}

	private void writeObject(ObjectOutputStream out) throws IOException
	{
		out.defaultWriteObject();
		// this.source is transient, so we need to explicitly write it to the output stream
		out.writeObject(this.source);
	}
	
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		in.defaultReadObject();
		// this.source is transient, so we need to explicitly read it from the input stream
		this.source = in.readObject();
	}
}
