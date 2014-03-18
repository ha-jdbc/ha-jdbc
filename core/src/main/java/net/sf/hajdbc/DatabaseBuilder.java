package net.sf.hajdbc;

import net.sf.hajdbc.configuration.PropertiesBuilder;

public interface DatabaseBuilder<Z, D extends Database<Z>> extends PropertiesBuilder<D>
{
	int ID_MAX_SIZE = 64;

	DatabaseBuilder<Z, D> connectionSource(Z connectionSource);

	DatabaseBuilder<Z, D> location(String location);

	DatabaseBuilder<Z, D> credentials(String user, String password);

	DatabaseBuilder<Z, D> weight(int weight);

	DatabaseBuilder<Z, D> local(boolean local);
}
