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
package net.sf.hajdbc.cache;


/**
 * @author Paul Ferraro
 *
 */
public class ColumnPropertiesImpl implements ColumnProperties
{
	private final String name;
	private final int type;
	private final String nativeType;
	private final boolean autoIncrement;
	
	public ColumnPropertiesImpl(String name, int type, String nativeType, String defaultValue, String remarks, Boolean autoIncrement)
	{
		this.name = name;
		this.type = type;
		this.nativeType = nativeType;
		this.autoIncrement = autoIncrement;
	}
	
	/**
	 * @see net.sf.hajdbc.cache.ColumnProperties#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @see net.sf.hajdbc.cache.ColumnProperties#getType()
	 */
	@Override
	public int getType()
	{
		return this.type;
	}
	
	/**
	 * @see net.sf.hajdbc.cache.ColumnProperties#getNativeType()
	 */
	@Override
	public String getNativeType()
	{
		return this.nativeType;
	}

	/**
	 * @see net.sf.hajdbc.cache.ColumnProperties#isAutoIncrement()
	 */
	@Override
	public boolean isAutoIncrement()
	{
		return this.autoIncrement;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof ColumnProperties)) return false;
		
		ColumnProperties column = (ColumnProperties) object;
		
		return this.name.equals(column.getName());
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.name.hashCode();
	}
	
}
