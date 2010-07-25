/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-Apr 29, 2010 Paul Ferraro
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import net.sf.cglib.proxy.Proxy;

/**
 * Object utility methods.
 * @author Paul Ferraro
 */
public class Objects
{
	/**
	 * Compares two objects for equality.
	 * If the objects are arrays, then an array comparison is performed.
	 * @param object1 an object
	 * @param object2 another object
	 * @return true, if the objects are equal, false otherwise
	 */
	public static boolean equals(Object object1, Object object2)
	{
		if ((object1 == null) || (object2 == null)) return object1 == object2;

		Class<?> class1 = object1.getClass();
		Class<?> class2 = object2.getClass();
		
		if (class1.isArray() && class2.isArray())
		{
			if ((object1 instanceof boolean[]) && (object2 instanceof boolean[]))
			{
				return Arrays.equals((boolean[]) object1, (boolean[]) object2);
			}
			if ((object1 instanceof byte[]) && (object2 instanceof byte[]))
			{
				return Arrays.equals((byte[]) object1, (byte[]) object2);
			}
			if ((object1 instanceof char[]) && (object2 instanceof char[]))
			{
				return Arrays.equals((char[]) object1, (char[]) object2);
			}
			if ((object1 instanceof double[]) && (object2 instanceof double[]))
			{
				return Arrays.equals((double[]) object1, (double[]) object2);
			}
			if ((object1 instanceof float[]) && (object2 instanceof float[]))
			{
				return Arrays.equals((float[]) object1, (float[]) object2);
			}
			if ((object1 instanceof int[]) && (object2 instanceof int[]))
			{
				return Arrays.equals((int[]) object1, (int[]) object2);
			}
			if ((object1 instanceof long[]) && (object2 instanceof long[]))
			{
				return Arrays.equals((long[]) object1, (long[]) object2);
			}
			if ((object1 instanceof short[]) && (object2 instanceof short[]))
			{
				return Arrays.equals((short[]) object1, (short[]) object2);
			}
			return Arrays.equals((Object[]) object1, (Object[]) object2);
		}

		return (class1.equals(class2) && Proxy.isProxyClass(class1) && Proxy.isProxyClass(class2)) || object1.equals(object2);
	}
	
	/**
	 * Serializes the specified object.
	 * @param object the serialization target
	 * @return a serialized object
	 */
	public static byte[] serialize(Object object)
	{
		if (object == null) return null;
		
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		
		try
		{
			ObjectOutput output = new ObjectOutputStream(bytes);
			output.writeObject(object);
			output.flush();
			output.close();
			
			return bytes.toByteArray();
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Deserializes the specified bytes into the object of the specified type.
	 * @param <T> the target class
	 * @param bytes a serialized object
	 * @return a deserialized object
	 */
	public static Object deserialize(byte[] bytes)
	{
		if (bytes == null) return null;
		
		try
		{
			ObjectInput input = new ObjectInputStream(new ByteArrayInputStream(bytes));
			
			try
			{
				return input.readObject();
			}
			catch (ClassNotFoundException e)
			{
				throw new IllegalStateException(e);
			}
			catch (IOException e)
			{
				throw new IllegalStateException(e);
			}
			finally
			{
				input.close();
			}
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Deserializes the specified bytes into the object of the specified type.
	 * @param <T> the target class
	 * @param bytes a serialized object
	 * @return a deserialized object
	 */
	public static <T> T deserialize(Class<T> targetClass, byte[] bytes)
	{
		Object object = deserialize(bytes);
		
		return (object != null) ? targetClass.cast(object) : null;
	}
	
	private Objects()
	{
		// Hide
	}	
}
