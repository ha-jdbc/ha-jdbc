/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc.activation;

import java.text.MessageFormat;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ForeignKey
{
	private String name;
	private String table;
	private String column;
	private String foreignTable;
	private String foreignColumn;

	public ForeignKey(String name, String table, String column, String foreignTable, String foreignColumn)
	{
		this.name = name;
		this.table = table;
		this.column = column;
		this.foreignTable = foreignTable;
		this.foreignColumn = foreignColumn;
	}
	
	public String formatSQL(String pattern)
	{
		return MessageFormat.format(pattern, new Object[] { this.name, this.table, this.column, this.foreignTable, this.foreignColumn });
	}
	
	public boolean equals(Object object)
	{
		ForeignKey foreignKey = (ForeignKey) object;
		
		return (foreignKey != null) && (foreignKey.name != null) && foreignKey.name.equals(this.name);
	}
	
	public int hashCode()
	{
		return this.name.hashCode();
	}
}
