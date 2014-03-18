package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.sql.CommonDataSourceDatabaseBuilder;

public class ConnectionPoolDataSourceDatabaseBuilder extends CommonDataSourceDatabaseBuilder<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	public ConnectionPoolDataSourceDatabaseBuilder(String id)
	{
		super(id, ConnectionPoolDataSource.class);
	}

	@Override
	public ConnectionPoolDataSourceDatabase build() throws SQLException
	{
		return new ConnectionPoolDataSourceDatabase(this.id, this.getDataSource(), this.credentials, this.weight, this.local);
	}
}
