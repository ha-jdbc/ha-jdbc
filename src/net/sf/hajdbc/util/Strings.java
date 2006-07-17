/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.util;

import java.util.Collection;
import java.util.Iterator;

/**
 * A set of String utilities.
 * @author Paul Ferraro
 * @since 1.2
 */
public final class Strings
{
	/**
	 * Performs the reverse of a split operation, joining the elements of the specified collection using the specified delimiter.
	 * @param collection a collection of strings
	 * @param delimiter a string to insert between each collection element
	 * @return a new String
	 */
	public static String join(Collection<String> collection, String delimiter)
	{
		StringBuilder builder = new StringBuilder();
		
		Iterator<String> elements = collection.iterator();
		
		while (elements.hasNext())
		{
			builder.append(elements.next());
			
			if (elements.hasNext())
			{
				builder.append(delimiter);
			}
		}
		
		return builder.toString();
	}
}
