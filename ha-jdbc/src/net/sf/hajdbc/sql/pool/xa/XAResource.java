/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import javax.transaction.xa.Xid;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.SQLObject;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XAResource extends SQLObject<javax.transaction.xa.XAResource, javax.sql.XAConnection> implements javax.transaction.xa.XAResource
{
	/**
	 * Constructs a new XAResourceProxy.
	 * @param connection a proxy for XAConnections
	 * @param operation an operation that creates XAResources
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public XAResource(XAConnection connection, Operation<javax.sql.XAConnection, javax.transaction.xa.XAResource> operation) throws java.sql.SQLException
	{
		super(connection, operation);
	}

	/**
	 * @see javax.transaction.xa.XAResource#getTransactionTimeout()
	 */
	public int getTransactionTimeout() throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Integer> operation = new Operation<javax.transaction.xa.XAResource, Integer>()
		{
			public Integer execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					return resource.getTransactionTimeout();
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			return this.executeReadFromDriver(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
	 */
	public boolean setTransactionTimeout(final int seconds) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Boolean> operation = new Operation<javax.transaction.xa.XAResource, Boolean>()
		{
			public Boolean execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					return resource.setTransactionTimeout(seconds);
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			return this.firstValue(this.executeWriteToDriver(operation));
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
	 */
	public boolean isSameRM(final javax.transaction.xa.XAResource xaResource) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Boolean> operation = new Operation<javax.transaction.xa.XAResource, Boolean>()
		{
			public Boolean execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					if (this.getClass().isInstance(xaResource))
					{
						XAResource proxy = (XAResource) xaResource;
						
						return resource.isSameRM(proxy.getObject(database));
					}

					return Boolean.valueOf(resource.isSameRM(xaResource));
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			return this.executeReadFromDriver(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#recover(int)
	 */
	public Xid[] recover(final int flag) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Xid[]> operation = new Operation<javax.transaction.xa.XAResource, Xid[]>()
		{
			public Xid[] execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					return resource.recover(flag);
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			return this.firstValue(this.executeWriteToDatabase(operation));
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
	 */
	public int prepare(final Xid id) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Integer> operation = new Operation<javax.transaction.xa.XAResource, Integer>()
		{
			public Integer execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					return resource.prepare(id);
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			return this.firstValue(this.executeWriteToDatabase(operation));
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
	 */
	public void forget(final Xid id) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Void> operation = new Operation<javax.transaction.xa.XAResource, Void>()
		{
			public Void execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					resource.forget(id);
					
					return null;
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			this.executeWriteToDatabase(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
	 */
	public void rollback(final Xid id) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Void> operation = new Operation<javax.transaction.xa.XAResource, Void>()
		{
			public Void execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					resource.rollback(id);
					
					return null;
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			this.executeWriteToDatabase(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
	 */
	public void end(final Xid id, final int flag) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Void> operation = new Operation<javax.transaction.xa.XAResource, Void>()
		{
			public Void execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					resource.end(id, flag);
					
					return null;
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			this.executeWriteToDatabase(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
	 */
	public void start(final Xid id, final int flag) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Void> operation = new Operation<javax.transaction.xa.XAResource, Void>()
		{
			public Void execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					resource.start(id, flag);
					
					return null;
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			this.executeWriteToDatabase(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}

	/**
	 * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
	 */
	public void commit(final Xid id, final boolean onePhase) throws javax.transaction.xa.XAException
	{
		Operation<javax.transaction.xa.XAResource, Void> operation = new Operation<javax.transaction.xa.XAResource, Void>()
		{
			public Void execute(Database database, javax.transaction.xa.XAResource resource) throws java.sql.SQLException
			{
				try
				{
					resource.commit(id, onePhase);
					
					return null;
				}
				catch (javax.transaction.xa.XAException e)
				{
					throw new SQLException(e);
				}
			}
		};
		
		try
		{
			this.executeWriteToDatabase(operation);
		}
		catch (java.sql.SQLException e)
		{
			throw this.translate(e);
		}
	}
	
	private javax.transaction.xa.XAException translate(java.sql.SQLException e)
	{
		if (javax.transaction.xa.XAException.class.isInstance(e.getCause()))
		{
			return (javax.transaction.xa.XAException) e.getCause();
		}

		return new XAException(e);
	}
	
	private class XAException extends javax.transaction.xa.XAException
	{
		private static final long serialVersionUID = 3833460721462950199L;

		/**
		 * Constructs a new XAException.
		 * @param message
		 */
		public XAException(String message)
		{
			super(message);
		}

		/**
		 * Constructs a new XAException.
		 * @param message
		 * @param cause 
		 */
		public XAException(String message, Throwable cause)
		{
			super(message);
			this.initCause(cause);
		}

		/**
		 * Constructs a new XAException.
		 * @param cause
		 */
		public XAException(Throwable cause)
		{
			super();
			this.initCause(cause);
		}
	}
}
