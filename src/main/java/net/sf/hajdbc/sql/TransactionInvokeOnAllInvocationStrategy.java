package net.sf.hajdbc.sql;

import java.util.concurrent.ExecutorService;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

public class TransactionInvokeOnAllInvocationStrategy extends InvokeOnAllInvocationStrategy
{
	private final boolean end;
	
	public TransactionInvokeOnAllInvocationStrategy(boolean end)
	{
		this.end = end;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvokeOnAllInvocationStrategy#getExecutor(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected <Z, D extends Database<Z>> ExecutorService getExecutor(DatabaseCluster<Z, D> cluster)
	{
		return cluster.getTransactionMode().getTransactionExecutor(cluster.getExecutor(), this.end);
	}
}
