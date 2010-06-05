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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;

/**
 * @author paul
 *
 */
public class Objects
{
	private Objects()
	{
		// Hide
	}
	
	public static boolean equals(Object object1, Object object2)
	{
		if ((object1 == null) || (object2 == null)) return object1 == object2;

		if (object1.getClass().isArray() && object2.getClass().isArray())
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

		return object1.equals(object2);
	}
	
	public static byte[] serialize(Object object)
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		
		try
		{
			ObjectOutput output = new ObjectOutputStream(bytes);
			output.writeObject(object);
			output.flush();
			output.close();
			
			return bytes.toByteArray();
		}
		catch (NotSerializableException e)
		{
			return serialize(e);
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
