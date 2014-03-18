package net.sf.hajdbc.sql;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.Messages;

public class DriverDatabaseBuilder extends AbstractDatabaseBuilder<Driver, DriverDatabase>
{
	public DriverDatabaseBuilder(String id)
	{
		super(id);
	}

	/**
	 * Alias for {@link #connectionSource(Driver)}.
	 */
	public DriverDatabaseBuilder driver(Driver driver)
	{
		return this.connectionSource(driver);
	}

	@Override
	public DriverDatabaseBuilder connectionSource(Driver connectionSource)
	{
		super.connectionSource(connectionSource);
		return this;
	}

	/**
	 * Alias for {@link #connectionSource(Driver)}.
	 */
	public DriverDatabaseBuilder url(String url)
	{
		return this.location(url);
	}
	
	@Override
	public DriverDatabaseBuilder location(String location)
	{
		super.location(location);
		return this;
	}

	@Override
	public DriverDatabaseBuilder property(String name, String value)
	{
		super.property(name, value);
		return this;
	}

	@Override
	public DriverDatabaseBuilder credentials(String user, String password)
	{
		super.credentials(user, password);
		return this;
	}

	@Override
	public DriverDatabaseBuilder weight(int weight)
	{
		super.weight(weight);
		return this;
	}

	@Override
	public DriverDatabaseBuilder local(boolean local)
	{
		super.local(local);
		return this;
	}

	@Override
	public DriverDatabaseBuilder read(DriverDatabase database)
	{
		super.read(database);
		this.location = database.getUrl();
		this.properties = new Properties(database.getProperties());
		return this;
	}

	@Override
	public DriverDatabase build() throws SQLException
	{
		String url = this.location;
		if (url == null)
		{
			throw new SQLException(String.format("No JDBC url specified."));
		}
		Driver driver = this.connectionSource;
		if ((driver != null) && !driver.acceptsURL(url))
		{
			throw new SQLException(Messages.JDBC_URL_REJECTED.getMessage(url));
		}
		Driver connectionSource = (driver == null) ? DriverManager.getDriver(url) : driver;
		return new DriverDatabase(this.id, connectionSource, url, new Properties(this.properties), this.credentials, this.weight, this.local);
	}
}
