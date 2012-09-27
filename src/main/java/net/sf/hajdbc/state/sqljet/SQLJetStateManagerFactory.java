package net.sf.hajdbc.state.sqljet;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerFactory;

public class SQLJetStateManagerFactory implements StateManagerFactory
{
	private static final long serialVersionUID = 8990527398117188315L;

	@Override
	public String getId()
	{
		return "sqlite";
	}

	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster)
	{
		return new SQLJetStateManager<Z, D>(cluster);
	}
}
