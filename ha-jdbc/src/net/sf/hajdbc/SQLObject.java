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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class SQLObject
{
	private static Log log = LogFactory.getLog(SQLObject.class);
	
	protected SQLObject parent;
	private DatabaseCluster databaseCluster;
	private Operation parentOperation;
	private Map objectMap;
	private List operationList = new LinkedList();
	
	protected SQLObject(SQLObject parent, Operation operation) throws java.sql.SQLException
	{
		this(parent.getDatabaseCluster(), parent.executeWriteToDatabase(operation));
		
		this.parent = parent;
		this.parentOperation = operation;
	}
	
	protected SQLObject(DatabaseCluster databaseCluster, Map objectMap)
	{
		this.databaseCluster = databaseCluster;
		this.objectMap = objectMap;
	}
	
	/**
	 * Returns the underlying SQL object for the specified database.
	 * If the sql object does not exist (this might be the case if the database was newly activated), it will be created from the stored operation.
	 * Any recorded operations are also executed. If the object could not be created, or if any of the executed operations failed, then the specified database is deactivated.
	 * @param database a database descriptor.
	 * @return an underlying SQL object
	 */
	public synchronized final Object getObject(Database database)
	{
		Object object = this.objectMap.get(database);
		
		if (object == null)
		{
			try
			{
				Object parentObject = this.parent.getObject(database);
				
				if (parentObject == null)
				{
					throw new java.sql.SQLException();
				}
				
				object = this.parentOperation.execute(database, parentObject);
				
				Iterator operations = this.operationList.iterator();
				
				while (operations.hasNext())
				{
					Operation operation = (Operation) operations.next();
					
					operation.execute(database, object);
				}
				
				this.objectMap.put(database, object);
			}
			catch (java.sql.SQLException e)
			{
				log.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, new Object[] { this.getClass().getName(), database }), e);
				
				this.databaseCluster.deactivate(database);
			}
		}
		
		return object;
	}
	
	/**
	 * Records an operation.
	 * @param operation a database operation
	 */
	public synchronized final void record(Operation operation)
	{
		this.operationList.add(operation);
	}
	
	/**
	 * Helper method that extracts the first result from a map of results.
	 * @param valueMap a Map<Database, Object> of operation execution results.
	 * @return a operation execution result
	 */
	protected final Object firstValue(Map valueMap)
	{
		return valueMap.values().iterator().next();
	}

	/**
	 * Helper method that call the appropriate execute method on the database cluster.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeReadFromDatabase(Operation operation) throws java.sql.SQLException
	{
		return this.databaseCluster.executeReadFromDatabase(this, operation);
	}

	/**
	 * Helper method that call the appropriate execute method on the database cluster.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeReadFromDriver(Operation operation) throws java.sql.SQLException
	{
		return this.databaseCluster.executeReadFromDriver(this, operation);
	}

	/**
	 * Helper method that call the appropriate execute method on the database cluster.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWriteToDatabase(Operation operation) throws java.sql.SQLException
	{
		return this.databaseCluster.executeWriteToDatabase(this, operation);
	}

	/**
	 * Helper method that call the appropriate execute method on the database cluster.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWriteToDriver(Operation operation) throws java.sql.SQLException
	{
		return this.databaseCluster.executeWriteToDriver(this, operation);
	}
	
	/**
	 * Returns the database cluster to which this proxy is associated.
	 * @return a database cluster
	 */
	public DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
}
