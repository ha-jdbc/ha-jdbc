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

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface DatabaseClusterDescriptor
{
	/**
	 * Creates a new database cluster from based on this descriptor
	 * @return a DatabaseCluster
	 * @throws java.sql.SQLException
	 */
	public DatabaseCluster createDatabaseCluster() throws java.sql.SQLException;
	
	/**
	 * Returns the identifier of this database cluster
	 * @return a database cluster identifier
	 */
	public String getId();

	/**
	 * Returns the SQL statement used to validate whether or not a database is responding 
	 * @return a SQL statement
	 */
	public String getValidateSQL();
	
	/**
	 * Returns a SQL pattern used to create a foreign key using the following arguments:
	 * <ol seqnum="0">
	 *   <li>Foreign key name</li>
	 *   <li>Table name</li>
	 *   <li>Column name</li>
	 *   <li>Foreign table name</li>
	 *   <li>Foreign column name</li>
	 * </ol>
	 * @return a SQL pattern
	 */
	public String getCreateForeignKeySQL();

	/**
	 * Returns a SQL pattern used to drop a foreign key using the following arguments:
	 * <ol seqnum="0">
	 *   <li>Foreign key name</li>
	 *   <li>Table name</li>
	 * </ol>
	 * @return a SQL pattern
	 */
	public String getDropForeignKeySQL();
	
	/**
	 * Returns a SQL pattern used to truncate a table using the following arguments:
	 * <ol seqnum="0">
	 *   <li>Table name</li>
	 * </ol>
	 * @return a SQL pattern
	 */
	public String getTruncateTableSQL();
	
	/**
	 * Returns a class name of the default synchronization strategy.
	 * @return a class name that implements SynchronizationStrategy
	 */
	public String getDefaultSynchronizationStrategy();
}
