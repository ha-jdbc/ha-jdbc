package net.sf.hajdbc.configuration;

public class SimpleBuilder<T> implements Builder<T>
{
	private volatile T configuration;

	public SimpleBuilder(T configuration)
	{
		this.configuration = configuration;
	}
	
	@Override
	public SimpleBuilder<T> read(T configuration)
	{
		this.configuration = configuration;
		return this;
	}

	@Override
	public T build()
	{
		return this.configuration;
	}
}
