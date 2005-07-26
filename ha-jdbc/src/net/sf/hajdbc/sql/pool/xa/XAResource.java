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
package net.sf.hajdbc.sql.pool.xa;

import java.sql.SQLException;
import java.util.Map;

import javax.transaction.xa.Xid;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.SQLObject;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XAResource extends SQLObject implements javax.transaction.xa.XAResource
{
	/**
	 * Constructs a new XAResourceProxy.
	 * @param connection a proxy for XAConnections
	 * @param operation an operation that creates XAResources
	 * @throws SQLException if operation execution fails
	 */
	public XAResource(XAConnection connection, XAConnectionOperation operation) throws SQLException
	{
		super(connection, operation);
	}

	/**
	 * @see javax.transaction.xa.XAResource#getTransactionTimeout()
	 */
	public int getTransactionTimeout() throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				return new Integer(resource.getTransactionTimeout());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
	 */
	public boolean setTransactionTimeout(final int seconds) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				return Boolean.valueOf(resource.setTransactionTimeout(seconds));
			}
		};
		
		return ((Boolean) this.firstValue(this.executeSet(operation))).booleanValue();
	}

	/**
	 * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
	 */
	public boolean isSameRM(final javax.transaction.xa.XAResource xaResource) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				if (this.getClass().isInstance(xaResource))
				{
					XAResource proxy = (XAResource) xaResource;
					
					return Boolean.valueOf(resource.isSameRM((XAResource) proxy.getObject(database)));
				}

				return Boolean.valueOf(resource.isSameRM(xaResource));
			}
		};
		
		return ((Boolean) this.executeGet(operation)).booleanValue();
	}

	/**
	 * @see javax.transaction.xa.XAResource#recover(int)
	 */
	public Xid[] recover(final int flag) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				return resource.recover(flag);
			}
		};

		return (Xid[]) this.firstValue(this.executeWrite(operation));
	}

	/**
	 * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
	 */
	public int prepare(final Xid id) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				return new Integer(resource.prepare(id));
			}
		};
		
		return ((Integer) this.firstValue(this.executeWrite(operation))).intValue();
	}

	/**
	 * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
	 */
	public void forget(final Xid id) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				resource.forget(id);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
	 */
	public void rollback(final Xid id) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				resource.rollback(id);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
	 */
	public void end(final Xid id, final int flag) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				resource.end(id, flag);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
	 */
	public void start(final Xid id, final int flag) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				resource.start(id, flag);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
	 */
	public void commit(final Xid id, final boolean onePhase) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(Database database, javax.transaction.xa.XAResource resource) throws javax.transaction.xa.XAException
			{
				resource.commit(id, onePhase);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}
	
	private Map executeWrite(XAResourceOperation operation) throws XAException
	{
		try
		{
			return super.executeWriteToDatabase(operation);
		}
		catch (SQLException e)
		{
			throw new XAException(e);
		}
	}

	private Map executeSet(XAResourceOperation operation) throws XAException
	{
		try
		{
			return super.executeWriteToDriver(operation);
		}
		catch (SQLException e)
		{
			throw new XAException(e);
		}
	}

	private Object executeGet(XAResourceOperation operation) throws XAException
	{
		try
		{
			return super.executeReadFromDriver(operation);
		}
		catch (SQLException e)
		{
			throw new XAException(e);
		}
	}
}
