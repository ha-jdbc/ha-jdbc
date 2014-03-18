package net.sf.hajdbc.xml;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseBuilder;

public class DatabaseClusterConfigurationReader_3_1<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> extends DatabaseClusterConfigurationReader_3_0<Z, D, B>
{
	public static final DatabaseClusterConfigurationReaderFactory FACTORY = new DatabaseClusterConfigurationReaderFactory()
	{
		@Override
		public <Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> DatabaseClusterConfigurationReader<Z, D, B> createReader()
		{
			return new DatabaseClusterConfigurationReader_3_1<>();
		}
	};
}
