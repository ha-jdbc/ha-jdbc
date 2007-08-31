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
package net.sf.hajdbc.cache;

import net.sf.hajdbc.ColumnProperties;

/**
 * @author Paul Ferraro
 *
 */
public class ColumnPropertiesImpl implements ColumnProperties
{
	private String name;
	private int type;
	private String nativeType;
	private String remarks;
	private Boolean autoIncrement;
	private String defaultValue;
	
	public ColumnPropertiesImpl(String name, int type, String nativeType, String defaultValue, String remarks, Boolean autoIncrement)
	{
		this.name = name;
		this.type = type;
		this.nativeType = nativeType;
		this.defaultValue = defaultValue;
		this.remarks = remarks;
		this.autoIncrement = autoIncrement;
	}
	
	/**
	 * @see net.sf.hajdbc.ColumnProperties#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @see net.sf.hajdbc.ColumnProperties#getType()
	 */
	@Override
	public int getType()
	{
		return this.type;
	}
	
	/**
	 * @see net.sf.hajdbc.ColumnProperties#getNativeType()
	 */
	@Override
	public String getNativeType()
	{
		return this.nativeType;
	}

	/**
	 * @see net.sf.hajdbc.ColumnProperties#getRemarks()
	 */
	@Override
	public String getRemarks()
	{
		return this.remarks;
	}

	/**
	 * @see net.sf.hajdbc.ColumnProperties#isAutoIncrement()
	 */
	@Override
	public Boolean isAutoIncrement()
	{
		return this.autoIncrement;
	}

	/**
	 * @see net.sf.hajdbc.ColumnProperties#getDefaultValue()
	 */
	@Override
	public String getDefaultValue()
	{
		return this.defaultValue;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof ColumnProperties)) return false;
		
		String name = ((ColumnProperties) object).getName();
		
		return (name != null) && name.equals(this.name);
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
