/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

/**
 * Interface for enumerated implementation classes.
 * @author Paul Ferraro
 * @param <T> the implemented interface for each enumerated class
 */
public interface ClassEnum<T>
{
	/**
	 * Creates a new instance of the enumerated class.
	 * @return a new instance of the enumerated class
	 * @throws Exception if a new instance could not be instantiated.
	 */
	public T newInstance() throws Exception;
	
	/**
	 * Indicates whether the specified object is an instance of this enumerated class.
	 * @param object an instance
	 * @return true, if this object is an instance of this enumerated class, false otherwise
	 */
	public boolean isInstance(T object);
}
