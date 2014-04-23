package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.Credentials;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseBuilder;
import net.sf.hajdbc.codec.Decoder;

public abstract class AbstractDatabaseBuilder<Z, D extends Database<Z>> implements DatabaseBuilder<Z, D>
{
	protected final String id;

	protected volatile Z connectionSource;
	protected volatile String location;
	protected volatile Properties properties = new Properties();
	protected volatile Credentials credentials;
	protected volatile int weight = 1;
	protected volatile boolean local = false;
	
	protected AbstractDatabaseBuilder(String id)
	{
		this.id = id;
	}

	@Override
	public DatabaseBuilder<Z, D> connectionSource(Z connectionSource)
	{
		this.connectionSource = connectionSource;
		return this;
	}

	@Override
	public DatabaseBuilder<Z, D> location(String location)
	{
		this.location = location;
		return this;
	}

	@Override
	public DatabaseBuilder<Z, D> property(String name, String value)
	{
		this.properties.put(name, value);
		return this;
	}

	@Override
	public DatabaseBuilder<Z, D> credentials(final String user, final String password)
	{
		this.credentials = new Credentials()
		{
			@Override
			public String getUser()
			{
				return user;
			}

			@Override
			public String decodePassword(Decoder decoder) throws SQLException
			{
				return decoder.decode(password);
			}

			@Override
			public String getEncodedPassword()
			{
				return password;
			}
		};
		return this;
	}

	@Override
	public DatabaseBuilder<Z, D> weight(int weight)
	{
		this.weight = weight;
		return this;
	}

	@Override
	public DatabaseBuilder<Z, D> local(boolean local)
	{
		this.local = local;
		return this;
	}

	@Override
	public DatabaseBuilder<Z, D> read(D database)
	{
		this.connectionSource = database.getConnectionSource();
		this.credentials = database.getCredentials();
		this.weight = database.getWeight();
		this.local = database.isLocal();
		return this;
	}
}
