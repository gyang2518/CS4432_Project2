package simpledb.index.hash;


import simpledb.query.Constant;

import simpledb.record.RID;
//CS4432-project2: Name: Dian Chen / Yang Gao

public class ExtensibleHashBucketRecord {



    private Constant dataval;

  //CS4432-project2: record ID to locate where the record in the table
    
    private RID rid;



    public ExtensibleHashBucketRecord(Constant dataval, RID rid) {

        this.dataval = dataval;

        this.rid = rid;

    }



    public Constant getDataval() {



        return dataval;

    }



    public RID getRid() {

        return rid;

    }

}
