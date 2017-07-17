package com.lightbend.model;

import java.io.Serializable;

// Class combining data and model records to allow merging PCollections
public class DataWithModel implements Serializable {
    private Winerecord.WineRecord data;
    private CurrentModelDescriptor model;

    public DataWithModel(Winerecord.WineRecord data){
        this.data = data;
        this.model = null;
    }
    public DataWithModel(CurrentModelDescriptor model){
        this.data = null;
        this.model = model;
    }

    public Winerecord.WineRecord getData() {
        return data;
    }

    public CurrentModelDescriptor getModel() {
        return model;
    }
}
