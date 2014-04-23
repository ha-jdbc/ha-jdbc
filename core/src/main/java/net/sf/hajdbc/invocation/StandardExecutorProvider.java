package net.sf.hajdbc.invocation;

import java.util.concurrent.ExecutorService;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.invocation.AllResultsCollector.ExecutorProvider;

public class StandardExecutorProvider implements ExecutorProvider
{
	@Override
	public <Z, D extends Database<Z>> ExecutorService getExecutor(DatabaseCluster<Z, D> cluster)
	{
		return cluster.getExecutor();
	}
}