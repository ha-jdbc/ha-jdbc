package net.sf.hajdbc.distributed;

public interface Stateful
{
	byte[] getState();
	
	void setState(byte[] state);
}
