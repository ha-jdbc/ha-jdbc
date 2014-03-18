package net.sf.hajdbc.configuration;

import net.sf.hajdbc.Identifiable;
import net.sf.hajdbc.IdentifiableMatcher;
import net.sf.hajdbc.util.ServiceLoaders;

public class SimpleServiceBuilder<T extends Identifiable> implements Builder<T>
{
	private volatile Class<T> serviceClass;
	private volatile String id;

	public SimpleServiceBuilder(Class<T> serviceClass, String id)
	{
		this.serviceClass = serviceClass;
		this.id = id;
	}
	
	@Override
	public SimpleServiceBuilder<T> read(T service)
	{
		this.id = service.getId();
		return this;
	}

	@Override
	public T build()
	{
		return ServiceLoaders.findRequiredService(new IdentifiableMatcher<T>(this.id), this.serviceClass);
	}
}
