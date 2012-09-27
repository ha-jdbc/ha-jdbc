package net.sf.hajdbc.state.simple;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;

public class SimpleStateManagerFactory implements StateManagerFactory
{
	private static final long serialVersionUID = 4387061472532427110L;
	
	@Override
	public String getId()
	{
		return "memory";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManagerFactory#createStateManager(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster)
	{
		return new SimpleStateManager();
	}
}
