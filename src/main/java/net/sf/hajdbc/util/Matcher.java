package net.sf.hajdbc.util;

public interface Matcher<T>
{
	boolean matches(T service);
}