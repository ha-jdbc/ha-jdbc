package net.sf.hajdbc.configuration;

import java.sql.SQLException;

public interface Builder<T>
{
	Builder<T> read(T configuration);
	
	T build() throws SQLException;
}
