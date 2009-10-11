package net.sf.hajdbc.lock.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class CoordinatorAcquireCommand extends CoordinatorLockCommand<Boolean>
{
	private static final long serialVersionUID = 1725113200306907771L;
	
	private final long timeout;
	
	public CoordinatorAcquireCommand(RemoteLockDescriptor descriptor, long timeout)
	{
		super(descriptor);

		this.timeout = timeout;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.distributed.CoordinatorLockCommand#execute(net.sf.hajdbc.distributable.jgroups.LockCommandContext, java.util.concurrent.locks.Lock)
	 */
	@Override
	protected Boolean execute(Lock lock)
	{
		try
		{
			return lock.tryLock(this.timeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			
			return false;
		}
	}

	@Override
	public Object marshalResult(Boolean result)
	{
		return result;
	}

	@Override
	public Boolean unmarshalResult(Object object)
	{
		return (Boolean) object;
	}
}
