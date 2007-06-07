package net.sf.hajdbc.distributable;

import java.io.Externalizable;
import java.util.Set;

import net.sf.hajdbc.LockManager;

import org.jgroups.Address;

public interface LockDecree extends Externalizable
{
	public String getId();

	public Address getAddress();

	/**
	 * Prepare phase of 2-phase vote
	 * @param lockManager a lock manager
	 * @return true if prepare phase was successful, false otherwise.
	 */
	public boolean prepare(LockManager lockManager);

	/**
	 * Commit phase of 2-phase vote
	 * @param lockManager a lock manager
	 * @return true if commit phase was successful, false otherwise.
	 */
	public boolean commit(LockManager lockManager, Set<LockDecree> lockDecreeSet);

	/**
	 * Called when prepare phase fails.
	 * @param lockManager
	 */
	public void abort(LockManager lockManager);
}