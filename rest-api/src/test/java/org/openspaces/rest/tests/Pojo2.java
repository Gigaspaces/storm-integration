package org.openspaces.rest.tests;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

@SpaceClass
public class Pojo2 implements IPojo{

    private Integer id;
    private Long val;
    
    public Pojo2() {
    }

    @Override
    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
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
