/*
 * Copyright (c) 2004-2008, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;

/**
 * @author Paul Ferraro
 */
public class TransactionalDatabaseWriteInvocationStrategy<D, T, R> extends DatabaseWriteInvocationStrategy<D, T, R>
{
	private Set<String> identifierSet;
	
	public TransactionalDatabaseWriteInvocationStrategy()
	{
		this.identifierSet = Collections.emptySet();
	}
	
	public TransactionalDatabaseWriteInvocationStrategy(Set<String> identifierSet)
	{
		this.identifierSet = identifierSet;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#getExecutor(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected ExecutorService getExecutor(DatabaseCluster<D> cluster)
	{
		return cluster.getTransactionalExecutor();
	}
	
	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#getLockList(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected List<Lock> getLockList(DatabaseCluster<D> cluster)
	{
		LockManager lockManager = cluster.getLockManager();
		
		if (this.identifierSet.isEmpty()) return Collections.singletonList(lockManager.readLock(LockManager.GLOBAL));
		
		List<Lock> list = new ArrayList<Lock>(this.identifierSet.size());
		
		for (String identifier: this.identifierSet)
		{
			list.add(lockManager.writeLock(identifier));
		}
		
		return list;
	}
}
