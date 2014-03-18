package net.sf.hajdbc.management;

import net.sf.hajdbc.Database;

public interface MBeanRegistrarFactory
{
	<Z, D extends Database<Z>> MBeanRegistrar<Z, D> createMBeanRegistrar();
}
