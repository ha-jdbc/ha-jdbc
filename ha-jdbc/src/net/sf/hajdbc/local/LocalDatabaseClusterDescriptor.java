/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.local;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDescriptor;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseClusterDescriptor implements DatabaseClusterDescriptor
{
	private String name;
	private String validateSQL;
	private String createForeignKeySQL;
	private String dropForeignKeySQL;
	private String createIndexSQL;
	private String dropIndexSQL;
	private Map databaseMap = new HashMap();
	
	/**
	 * @return a map of databases in this cluster
	 */
	public Map getDatabaseMap()
	{
		return this.databaseMap;
	}
	
	/**
	 * Adds the specified database to this cluster
	 * @param object
	 */
	protected void addDatabase(Object object)
	{
		Database database = (Database) object;
		this.databaseMap.put(database.getId(), database);
	}
	
	/**
	 * @return the name of this database cluster
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @return the SQL used to validate that a database is active
	 */
	public String getValidateSQL()
	{
		return this.validateSQL;
	}
	
	/**
	 * @return
	 */
	public String getCreateForeignKeySQL()
	{
		return this.createForeignKeySQL;
	}
	
	/**
	 * @return
	 */
	public String getCreateIndexSQL()
	{
		return this.createIndexSQL;
	}
	
	/**
	 * @return
	 */
	public String getDropForeignKeySQL()
	{
		return this.dropForeignKeySQL;
	}
	
	/**
	 * @return
	 */
	public String getDropIndexSQL()
	{
		return this.dropIndexSQL;
	}
	
	public DatabaseCluster createDatabaseCluster() throws java.sql.SQLException
	{
		return new LocalDatabaseCluster(this);
	}
}
