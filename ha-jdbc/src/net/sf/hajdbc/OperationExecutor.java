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

import java.util.Map;

/**
 * Helper class that enables asynchronous execution of an operation.
 */
public class OperationExecutor implements Runnable
{
	private DatabaseCluster databaseCluster;
	private Operation operation;
	private Database database;
	private Object object;
	private Map returnValueMap;
	private Map exceptionMap;
	
	/**
	 * Constructs a new OperationExecutor.
	 * @param databaseCluster a database cluster
	 * @param operation a database operation
	 * @param database a database descriptor
	 * @param object a SQL object
	 * @param returnValueMap a Map<Database, Object> that holds the results from the operation execution
	 * @param exceptionMap a Map<Database, SQLException> that holds the exceptions resulting from the operation execution
	 */
	public OperationExecutor(DatabaseCluster databaseCluster, Operation operation, Database database, Object object, Map returnValueMap, Map exceptionMap)
	{
		this.databaseCluster = databaseCluster;
		this.operation = operation;
		this.database = database;
		this.object = object;
		this.returnValueMap = returnValueMap;
		this.exceptionMap = exceptionMap;
	}
	
	/**
	 * @see java.lang.Runnable#run()
	 */
	public void run()
	{
		try
		{
			Object returnValue = this.operation.execute(this.database, this.object);
			
			synchronized (this.returnValueMap)
			{
				this.returnValueMap.put(this.database, returnValue);
			}
		}
		catch (Throwable e)
		{
			try
			{
				this.databaseCluster.handleFailure(this.database, e);
			}
			catch (Throwable exception)
			{
				synchronized (this.exceptionMap)
				{
					this.exceptionMap.put(this.database, e);
				}
			}
		}
	}
}
