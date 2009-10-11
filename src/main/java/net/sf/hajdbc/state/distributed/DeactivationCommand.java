package net.sf.hajdbc.state.distributed;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;

public class DeactivationCommand<Z, D extends Database<Z>> extends StateCommand<Z, D>
{
	private static final long serialVersionUID = -601538840572935794L;

	public DeactivationCommand(DatabaseEvent event)
	{
		super(event);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.distributed.StateCommand#execute(net.sf.hajdbc.Database, net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.state.StateManager)
	 */
	@Override
	protected boolean execute(D database, DatabaseCluster<Z, D> cluster, StateManager stateManager)
	{
		return cluster.deactivate(database, stateManager);
	}
}
