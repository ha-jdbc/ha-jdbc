/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.text.MessageFormat;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ForeignKey
{
	private static final String CREATE_SQL = "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4})";
	private static final String DROP_SQL = "ALTER TABLE {1} DROP CONSTRAINT {0}";
	
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
