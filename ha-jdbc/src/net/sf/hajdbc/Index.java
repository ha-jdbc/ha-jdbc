/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

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
	private static final String CREATE_SQL = "CREATE {1,choice,0#|1#UNIQUE} INDEX {0} ON {2} ({3})";
	private static final String DROP_SQL = "DROP INDEX {0}";
	
	private String name;
	private boolean unique;
	private String table;
	private List columnList = new LinkedList();
	
	public Index(String name, boolean unique, String table)
	{
		this.name = name;
		this.unique = unique;
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
			
		return MessageFormat.format(pattern, new Object[] { this.name, new Integer(this.unique ? 1 : 0), this.table, buffer.toString() });
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
