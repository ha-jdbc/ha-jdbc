package net.sf.ha.mail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.mail.Address;
import javax.mail.internet.InternetAddress;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class HostGroupingSenderStrategy extends GroupingSenderStrategy
{
	/**
	 * @see net.sf.ha.mail.GroupingSenderStrategy#groupAddresses(javax.mail.Address[])
	 */
	protected Collection groupAddresses(Address[] addresses)
	{
		Map addressListMap = new HashMap();

		for (int i = 0; i < addresses.length; ++i)
		{
			InternetAddress internetAddress = (InternetAddress) addresses[i];
			String address = internetAddress.getAddress();
			String host = address.substring(address.indexOf("@") + 1).toLowerCase();
			
			List addressList = (List) addressListMap.get(host);
			
			if (addressList == null)
			{
				addressList = new LinkedList();
				addressListMap.put(host, addressList);
			}
			
			addressList.add(addresses[i]);
		}
		
		List addressGroupList = new ArrayList(addressListMap.size());
		Iterator addressLists = addressListMap.values().iterator();
		
		while (addressLists.hasNext())
		{
			List addressList = (List) addressLists.next();
			
			addressGroupList.add(addressList.toArray(new Address[addressList.size()]));
		}
		
		return addressGroupList;
	}
}
