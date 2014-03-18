package net.sf.hajdbc;

public interface DatabaseBuilderFactory<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>>
{
	B createBuilder(String id);
}
