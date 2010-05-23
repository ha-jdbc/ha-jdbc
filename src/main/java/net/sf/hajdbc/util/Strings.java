/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * A set of String utilities.
 * @author Paul Ferraro
 * @since 2.0
 */
public final class Strings
{
	public static final String ANY = "%"; //$NON-NLS-1$
	public static final String COMMA = ","; //$NON-NLS-1$
	public static final String DASH = "-"; //$NON-NLS-1$
	public static final String DOT = "."; //$NON-NLS-1$
	public static final String EMPTY = ""; //$NON-NLS-1$
	public static final String PADDED_COMMA = ", "; //$NON-NLS-1$
	public static final String QUESTION = "?"; //$NON-NLS-1$
	public static final String UNDERSCORE = "_"; //$NON-NLS-1$
	public static final String TAB = "\t";
	public static final String NEW_LINE = System.getProperty("line.separator");
	public static final String FILE_SEPARATOR = System.getProperty("file.separator");
	public static final String USER_HOME = System.getProperty("user.home");

	/**
	 * Performs the reverse of a split operation, joining the elements of the specified collection using the specified delimiter.
	 * @param collection a collection of strings
	 * @param delimiter a string to insert between each collection element
	 * @return a new String
	 */
	public static StringBuilder join(StringBuilder builder, Collection<String> collection, String delimiter)
	{
		Iterator<String> elements = collection.iterator();
		
		while (elements.hasNext())
		{
			builder.append(elements.next());
			
			if (elements.hasNext())
			{
				builder.append(delimiter);
			}
		}
		
		return builder;
	}
	
	/**
	 * Performs the reverse of a split operation, joining the elements of the specified collection using the specified delimiter.
	 * @param collection a collection of strings
	 * @param delimiter a string to insert between each collection element
	 * @return a new String
	 */
	public static String join(Collection<String> collection, String delimiter)
	{
		return join(new StringBuilder(), collection, delimiter).toString();
	}
	
	/**
	 * Performs the reverse of a split operation, joining the elements of the specified collection using the specified delimiter.
	 * @param strings an array of strings
	 * @param delimiter a string to insert between each array element
	 * @return a new String
	 */
	public static String join(String[] strings, String delimiter)
	{
		return join(Arrays.asList(strings), delimiter);
	}
	
	/**
	 * Performs the reverse of a split operation, joining the elements of the specified collection using the specified delimiter.
	 * @param strings an array of strings
	 * @param delimiter a string to insert between each array element
	 * @return a new String
	 */
	public static StringBuilder join(StringBuilder builder, String[] strings, String delimiter)
	{
		return join(builder, Arrays.asList(strings), delimiter);
	}
	
	private Strings()
	{
		// Hide constructor
	}
}
