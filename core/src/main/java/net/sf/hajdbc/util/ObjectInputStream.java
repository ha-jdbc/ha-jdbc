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
package net.sf.hajdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;

/**
 * Custom {@link java.io.ObjectInputStream} that resolves classes against a specific class loader.
 * @author Paul Ferraro
 */
public class ObjectInputStream extends java.io.ObjectInputStream
{
	private final ClassLoader loader;

	public ObjectInputStream(InputStream in, ClassLoader loader) throws IOException
	{
		super(in);
		this.loader = loader;
	}

	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException
	{
		return Class.forName(desc.getName(), false, this.loader);
	}

	@Override
	protected Class<?> resolveProxyClass(String[] names) throws IOException, ClassNotFoundException
	{
		Class<?>[] interfaces = new Class[names.length];
		for (int i = 0; i < names.length; ++i)
		{
			interfaces[i] = Class.forName(names[i], false, this.loader);
		}
		return Proxy.getProxyClass(this.loader, interfaces);
	}
}