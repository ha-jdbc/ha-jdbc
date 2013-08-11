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
package net.sf.hajdbc.sql.serial;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;

/**
 * Enumeration of supported {@link SerialLocatorFactory} types.
 * @author Paul Ferraro
 */
public enum SerialLocatorFactories
{
	ARRAY(Array.class, new SerialArrayFactory()),
	BLOB(Blob.class, new SerialBlobFactory()),
	CLOB(Clob.class, new SerialClobFactory()),
	NCLOB(NClob.class, new SerialNClobFactory()),
	REF(Ref.class, new SerialRefFactory()),
	;
	
	private final Class<?> targetClass;
	private final SerialLocatorFactory<?> factory;

	private <T> SerialLocatorFactories(Class<T> targetClass, SerialLocatorFactory<T> factory)
	{
		this.targetClass = targetClass;
		this.factory = factory;
	}

	@SuppressWarnings("unchecked")
	public <T> SerialLocatorFactory<T> getSerialFactory()
	{
		return (SerialLocatorFactory<T>) this.factory;
	}
	
	public static <R> SerialLocatorFactory<R> find(Class<R> targetClass)
	{
		for (SerialLocatorFactories value: values())
		{
			if (value.targetClass.equals(targetClass))
			{
				return value.getSerialFactory();
			}
		}
		return null;
	}
}
