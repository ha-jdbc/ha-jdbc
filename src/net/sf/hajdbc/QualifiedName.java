/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

/**
 * Tuple that stores the schema name and object name of a schema qualified object.
 * @author Paul Ferraro
 */
public class QualifiedName
{
	private String schema;
	private String name;
	
	public QualifiedName(String schema, String name)
	{
		this(name);
		
		this.schema = schema;
	}
	
	public QualifiedName(String name)
	{
		this.name = name;
	}
	
	public String getSchema()
	{
		return this.schema;
	}
	
	public String getName()
	{
		return this.name;
	}
}
