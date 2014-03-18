package net.sf.hajdbc.sql;

import java.sql.Driver;

import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;
import net.sf.hajdbc.DatabaseBuilderFactory;

public class DriverDatabaseClusterConfigurationBuilder extends DatabaseClusterConfigurationBuilder<Driver, DriverDatabase, DriverDatabaseBuilder>
{
	public DriverDatabaseClusterConfigurationBuilder()
	{
		super(new DatabaseBuilderFactory<Driver, DriverDatabase, DriverDatabaseBuilder>()
		{
			@Override
			public DriverDatabaseBuilder createBuilder(String id)
			{
				return new DriverDatabaseBuilder(id);
			}
		});
	}
}
