package org.openspaces.rest.tests;

import com.gigaspaces.annotation.pojo.SpaceId;

public class Pojo3 implements IPojo{
    
    private Float id;
    private Long val;
    
    public Pojo3() {
    }

    @Override
    @SpaceId
    public Float getId() {
        return id;
    }

    public void setId(Float id) {
        this.id = id;
    }

    @Override
    public Long getVal() {
        return val;
    }

    public void setVal(Long val) {
        this.val = val;
    }
}
