package net.sf.hajdbc.sql.pool;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.DatabaseBuilderFactory;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;

public class ConnectionPoolDataSourceDatabaseClusterConfigurationBuilder extends DatabaseClusterConfigurationBuilder<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSourceDatabaseBuilder>
{
	public ConnectionPoolDataSourceDatabaseClusterConfigurationBuilder()
	{
		super(new DatabaseBuilderFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSourceDatabaseBuilder>()
		{
			@Override
			public ConnectionPoolDataSourceDatabaseBuilder createBuilder(String id)
			{
				return new ConnectionPoolDataSourceDatabaseBuilder(id);
			}
		});
	}
}