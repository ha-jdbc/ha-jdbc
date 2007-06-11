package net.sf.hajdbc.distributable;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.LockManager;

import org.jgroups.Address;

public interface LockDecree extends Serializable
{
	public String getId();

	public Address getAddress();

	/**
	 * Prepare phase of 2-phase vote
	 * @param lockManager a lock manager
	 * @return true if prepare phase was successful, false otherwise.
	 */
	public boolean prepare(LockManager lockManager, Map<LockDecree, Lock> lockMap);

	/**
	 * Commit phase of 2-phase vote
	 * @param lockManager a lock manager
	 * @return true if commit phase was successful, false otherwise.
	 */
	public boolean commit(Map<LockDecree, Lock> lockMap);

	/**
	 * Called when prepare phase fails.
	 * @param lockManager
	 */
	public void abort(Map<LockDecree, Lock> lockMap);
}