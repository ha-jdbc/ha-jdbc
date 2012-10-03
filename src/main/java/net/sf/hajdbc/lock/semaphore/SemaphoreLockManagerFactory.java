package net.sf.hajdbc.lock.semaphore;

import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.lock.LockManagerFactory;

public class SemaphoreLockManagerFactory implements LockManagerFactory
{
	private static final long serialVersionUID = -1330668107554832289L;

	private boolean fair;
	
	public void setFair(boolean fair)
	{
		this.fair = fair;
	}
	
	public boolean isFair()
	{
		return this.fair;
	}

	@Override
	public String getId()
	{
		return "semaphore";
	}
	
	@Override
	public LockManager createLockManager()
	{
		return new SemaphoreLockManager(this.fair);
	}
}
