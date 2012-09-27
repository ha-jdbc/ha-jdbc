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
		return service.getId().equals(id);
	}

	@Override
	public String toString()
	{
		return this.id;
	}
}
