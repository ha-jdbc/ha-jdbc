package net.sf.ha.jdbc.pool.xa;

import java.sql.SQLException;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import net.sf.ha.jdbc.DatabaseCluster;
import net.sf.ha.jdbc.SQLProxy;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XAResourceProxy extends SQLProxy implements XAResource
{
	private DatabaseCluster databaseCluster;
	
	public XAResourceProxy(DatabaseCluster databaseCluster, Map resourceMap)
	{
		super(resourceMap);
		
		this.databaseCluster = databaseCluster;
	}
	
	/**
	 * @see net.sf.ha.jdbc.SQLProxy#getDatabaseCluster()
	 */
	protected DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}

	/**
	 * @see javax.transaction.xa.XAResource#getTransactionTimeout()
	 */
	public int getTransactionTimeout() throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
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
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
			{
				return Boolean.valueOf(resource.setTransactionTimeout(seconds));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeSet(operation))).booleanValue();
	}

	/**
	 * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
	 */
	public boolean isSameRM(final XAResource xaResource) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
			{
				if (this.getClass().isInstance(xaResource))
				{
					XAResourceProxy proxy = (XAResourceProxy) xaResource;
					
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
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
			{
				return resource.recover(flag);
			}
		};

		return (Xid[]) this.firstItem(this.executeWrite(operation));
	}

	/**
	 * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
	 */
	public int prepare(final Xid id) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
			{
				return new Integer(resource.prepare(id));
			}
		};
		
		return ((Integer) this.firstItem(this.executeWrite(operation))).intValue();
	}

	/**
	 * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
	 */
	public void forget(final Xid id) throws XAException
	{
		XAResourceOperation operation = new XAResourceOperation()
		{
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
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
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
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
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
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
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
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
			public Object execute(XADataSourceDatabase database, XAResource resource) throws XAException
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
			return super.executeWrite(operation);
		}
		catch (SQLException e)
		{
			XAException exception = new XAException(e.getMessage());
			exception.initCause(e);
			throw exception;
		}
	}

	private Map executeSet(XAResourceOperation operation) throws XAException
	{
		try
		{
			return super.executeSet(operation);
		}
		catch (SQLException e)
		{
			XAException exception = new XAException(e.getMessage());
			exception.initCause(e);
			throw exception;
		}
	}
/*	
	private Object executeRead(XAResourceOperation operation) throws XAException
	{
		try
		{
			return super.executeRead(operation);
		}
		catch (SQLException e)
		{
			XAException exception = new XAException(e.getMessage());
			exception.initCause(e);
			throw exception;
		}
	}
*/	
	private Object executeGet(XAResourceOperation operation) throws XAException
	{
		try
		{
			return super.executeGet(operation);
		}
		catch (SQLException e)
		{
			XAException exception = new XAException(e.getMessage());
			exception.initCause(e);
			throw exception;
		}
	}
}
