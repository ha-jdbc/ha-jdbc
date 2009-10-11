package net.sf.hajdbc.distributed;


public interface MembershipListener
{
	void added(Member member);
	
	void removed(Member member);
}
