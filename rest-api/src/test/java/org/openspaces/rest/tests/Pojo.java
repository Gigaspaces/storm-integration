package org.openspaces.rest.tests;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;


public class Pojo implements IPojo{
	
	private String id;
	private String val;
	private Pojo2 nestedObj;
	
	public Pojo() {
	}

	@Override
    @SpaceId
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
    @SpaceRouting
	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}

    public Pojo2 getNestedObj() {
        return nestedObj;
    }

    public void setNestedObj(Pojo2 nestedObj) {
        this.nestedObj = nestedObj;
    }
	
}