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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class Index
{
	private static final String CREATE_SQL = "CREATE INDEX {0} ON {1} ({2})";
	private static final String DROP_SQL = "DROP INDEX {0}";
	
	private String name;
	private String table;
	private List columnList = new LinkedList();
	
	public Index(String name, String table)
	{
		this.name = name;
		this.table = table;
	}
	
	public void addColumn(String column)
	{
		this.columnList.add(column);
	}
	
	public String createSQL()
	{
		return formatSQL(CREATE_SQL);
	}
	
	public String dropSQL()
	{
		return formatSQL(DROP_SQL);
	}
	
	private String formatSQL(String pattern)
	{
		StringBuffer buffer = new StringBuffer();
		Iterator columns = this.columnList.iterator();
		
		while (columns.hasNext())
		{
			String column = (String) columns.next();
			buffer.append(column);
			
			if (columns.hasNext())
			{
				buffer.append(",");
			}
		}
			
		return MessageFormat.format(pattern, new Object[] { this.name, this.table, buffer.toString() });
	}
	
	public boolean equals(Object object)
	{
		Index index = (Index) object;
		
		return (index != null) && (index.name != null) && index.name.equals(this.name);
	}
	
	public int hashCode()
	{
		return this.name.hashCode();
	}
}
