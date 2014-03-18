package net.sf.hajdbc.xml;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseBuilder;

public interface DatabaseClusterConfigurationReaderFactory
{
	<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> DatabaseClusterConfigurationReader<Z, D, B> createReader();
}
