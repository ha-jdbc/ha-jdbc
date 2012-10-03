package net.sf.hajdbc;

import net.sf.hajdbc.util.Matcher;

public class IdentifiableMatcher<T extends Identifiable> implements Matcher<T>
{
	private final String id;

	public IdentifiableMatcher(String id)
	{
		this.id = id;
	}

	@Override
	public boolean matches(T service)
	{
		String serviceId = service.getId();
		return (this.id != null) && (service != null) ? this.id.equals(serviceId) : (this.id == serviceId);
	}

	@Override
	public String toString()
	{
		return this.id;
	}
}
