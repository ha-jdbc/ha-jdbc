package net.sf.hajdbc.sql;

import javax.sql.DataSource;

import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;
import net.sf.hajdbc.DatabaseBuilderFactory;

public class DataSourceDatabaseClusterConfigurationBuilder extends DatabaseClusterConfigurationBuilder<DataSource, DataSourceDatabase, DataSourceDatabaseBuilder>
{
	public DataSourceDatabaseClusterConfigurationBuilder()
	{
		super(new DatabaseBuilderFactory<DataSource, DataSourceDatabase, DataSourceDatabaseBuilder>()
		{
			@Override
			public DataSourceDatabaseBuilder createBuilder(String id)
			{
				return new DataSourceDatabaseBuilder(id);
			}
		});
	}
}
