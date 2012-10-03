package net.sf.hajdbc.lock;

import java.io.Serializable;

import net.sf.hajdbc.Identifiable;

public interface LockManagerFactory extends Identifiable, Serializable
{
	LockManager createLockManager();
}
