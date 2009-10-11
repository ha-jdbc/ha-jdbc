package net.sf.hajdbc.state;

import java.util.Properties;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

public interface StateManagerProvider
{
	<Z, D extends Database<Z>> StateManager createStateManager(DatabaseCluster<Z, D> cluster, Properties properties);
	
	boolean isEnabled();
}
