package com.gigaspaces.storm;

import org.openspaces.core.space.UrlSpaceConfigurer;

public class Test {

	public static void main(String[] args)throws Exception{
		
		UrlSpaceConfigurer usc=new UrlSpaceConfigurer("jini://15.185.171.42/*/streamspace");
		usc.space();

		//XAPStreamFactory fact=new XAPStreamFactory("jini://*/*/streamspace?locators=15.185.171.42");
		/*XAPTupleStream stream=fact.getTupleStream("words");
		System.out.println(stream.toString());*/
		
	}
}
