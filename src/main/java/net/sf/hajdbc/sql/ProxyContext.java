package net.sf.hajdbc.sql;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

public interface ProxyContext<Z, D extends Database<Z>>
{
	DatabaseCluster<Z, D> getDatabaseCluster();
}
