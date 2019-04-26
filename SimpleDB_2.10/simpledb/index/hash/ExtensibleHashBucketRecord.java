package simpledb.index.hash;


import simpledb.query.Constant;

import simpledb.record.RID;



/**

 * Extra class that simulates what's in a record for the Extensible Hash Index

 */

public class ExtensibleHashBucketRecord {



    private Constant dataval;

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
