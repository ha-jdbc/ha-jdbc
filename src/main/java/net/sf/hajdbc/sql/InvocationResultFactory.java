package net.sf.hajdbc.sql;

import java.util.SortedMap;

import net.sf.hajdbc.Database;

public interface InvocationResultFactory<Z, D extends Database<Z>, R, E extends Exception>
{
	R createResult(SortedMap<D, R> results) throws E;
}