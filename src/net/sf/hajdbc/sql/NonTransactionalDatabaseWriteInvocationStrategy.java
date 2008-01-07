/*
 * Copyright (c) 2004-2008, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 */
public class NonTransactionalDatabaseWriteInvocationStrategy<D, T, R> extends DatabaseWriteInvocationStrategy<D, T, R>
{
	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#getExecutor(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected ExecutorService getExecutor(DatabaseCluster<D> cluster)
	{
		return cluster.getNonTransactionalExecutor();
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#getLockList(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected List<Lock> getLockList(DatabaseCluster<D> cluster)
	{
		return Collections.emptyList();
	}
}
