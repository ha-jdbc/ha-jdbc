package net.sf.hajdbc;

public interface DatabaseClusterConfigurationBuilderProvider<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>>
{
	DatabaseClusterConfigurationBuilder<Z, D, B> getConfigurationBuilder();
}
