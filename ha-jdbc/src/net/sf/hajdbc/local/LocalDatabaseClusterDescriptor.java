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
	public static final String DEFAULT_VALIDATE_SQL = "SELECT 1";
	public static final String DEFAULT_CREATE_FOREIGN_KEY_SQL = "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4})";
	public static final String DEFAULT_DROP_FOREIGN_KEY_SQL = "ALTER TABLE {1} DROP CONSTRAINT {0}";
	public static final String DEFAULT_TRUNCATE_TABLE_SQL = "DELETE FROM {0}";
	public static final String DEFAULT_SYNC_STRATEGY = "net.sf.hajdbc.sync.DifferentialSynchronizationStrategy";
	
	private String id;
	private String validateSQL = DEFAULT_VALIDATE_SQL;
	private String createForeignKeySQL = DEFAULT_CREATE_FOREIGN_KEY_SQL;
	private String dropForeignKeySQL = DEFAULT_DROP_FOREIGN_KEY_SQL;
	private String truncateTableSQL = DEFAULT_TRUNCATE_TABLE_SQL;
	private String defaultSynchronizationStrategy = DEFAULT_SYNC_STRATEGY;
	private Map databaseMap = new HashMap();
	
	/**
	 * Returns a mapping of database id to Database object.
	 * @return a map of databases in this cluster
	 */
	public Map getDatabaseMap()
	{
		return this.databaseMap;
	}
	
	/**
	 * Adds the specified database to this cluster
	 * @param object add the specified database to the 
	 */
	protected void addDatabase(Object object)
	{
		Database database = (Database) object;
		this.databaseMap.put(database.getId(), database);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#getId()
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#getValidateSQL()
	 */
	public String getValidateSQL()
	{
		return this.validateSQL;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#getCreateForeignKeySQL()
	 */
	public String getCreateForeignKeySQL()
	{
		return this.createForeignKeySQL;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#getDropForeignKeySQL()
	 */
	public String getDropForeignKeySQL()
	{
		return this.dropForeignKeySQL;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#getTruncateTableSQL()
	 */
	public String getTruncateTableSQL()
	{
		return this.truncateTableSQL;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#getDefaultSynchronizationStrategy()
	 */
	public String getDefaultSynchronizationStrategy()
	{
		return this.defaultSynchronizationStrategy;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterDescriptor#createDatabaseCluster()
	 */
	public DatabaseCluster createDatabaseCluster() throws java.sql.SQLException
	{
		return new LocalDatabaseCluster(this);
	}
}
