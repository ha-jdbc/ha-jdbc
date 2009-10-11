package net.sf.hajdbc.state.bdb;

import java.util.Properties;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.StateManagerProvider;

public class BDBStateManagerProvider implements StateManagerProvider
{
	@Override
	public <Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster, Properties properties)
	{
		return null;
	}

	@Override
	public boolean isEnabled()
	{
		return false;
	}
}
