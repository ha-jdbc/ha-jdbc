package net.sf.hajdbc.configuration;

public interface PropertiesBuilder<T> extends Builder<T>
{
	PropertiesBuilder<T> property(String name, String value);
}
