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
package net.sf.hajdbc;

import java.util.HashMap;
import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseClusterDescriptor
{
	private String name;
	private String validateSQL = "SELECT 1";
	private String createForeignKeySQL = "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4})";
	private String dropForeignKeySQL = "ALTER TABLE {1} DROP CONSTRAINT {0}";
	private String createIndexSQL = "CREATE INDEX {0} ON {1} ({2})";
	private String dropIndexSQL = "DROP INDEX {0}";
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
}
