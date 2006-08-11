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
package net.sf.hajdbc.cache;

import net.sf.hajdbc.ColumnProperties;

/**
 * @author Paul Ferraro
 *
 */
public class ColumnPropertiesImpl implements ColumnProperties
{
	private String column;
	private int type;
	private String nativeType;
	
	public ColumnPropertiesImpl(String column, int type, String nativeType)
	{
		this.column = column;
		this.type = type;
		this.nativeType = nativeType;
	}
	
	public String getColumn()
	{
		return this.column;
	}
	
	public int getType()
	{
		return this.type;
	}
	
	public String getNativeType()
	{
		return this.nativeType;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		return ColumnProperties.class.cast(object).getColumn().equals(this.column);
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.column.hashCode();
	}
}
