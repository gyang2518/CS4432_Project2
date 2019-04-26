package simpledb.index.hash;

//CS4432-project2: Name: Dian Chen / Yang Gao

import simpledb.index.Index;

import simpledb.query.Constant;

import simpledb.query.TableScan;

import simpledb.record.RID;

import simpledb.record.Schema;

import simpledb.record.TableInfo;

import simpledb.tx.Transaction;



import java.util.ArrayList;



import static simpledb.metadata.TableMgr.MAX_NAME;



public class ExtensibleIndex implements Index {


    //CS4432-project2:set the maximum bucket size
    public static final int MAX_BUCKET_SIZE = 200;
    
    //CS4432-project2: the size of hash index entry
    public static final int MAX_HASH_SIZE = 100;



    private String idxname;

    private Schema sch;

    private Transaction tx;
    
   //CS4432-project2:current global depth for hash index
    private int globalDepth = 0;

    //CS4432-project2: the search key in the hash index
    private Constant key;

    //CS4432-project2: the bucket which matched with the search key
    private TableScan location; 

    //CS4432-project2: information about the table 
    private TableInfo Info;

    //CS4432-project2: create a hash table to store all information we want to print out
    private TableInfo HashTable;


    public ExtensibleIndex(String idxname, Schema sch, Transaction tx) {

        this.idxname = idxname;

        this.sch = sch;

        this.tx = tx;

      //CS4432-project2: initialize all te information storing in hash table

        String hashTable = idxname + "eHashTable";

        Schema hashTableSch = new Schema();

        hashTableSch.addStringField("hash", MAX_HASH_SIZE);

      //CS4432-project2: number of the free space in the bucket
        hashTableSch.addIntField("valid");

      //CS4432-project2: current number of local depth
        hashTableSch.addIntField("local");

      //CS4432-project2: position of the table
        hashTableSch.addStringField("pos", MAX_NAME);

        HashTable = new TableInfo(hashTable, hashTableSch);

        ////CS4432-project2: set the global depth into memory
        setGlobalDepth(); 

    }

  //CS4432-project2: initialize the index
    public void beforeFirst(Constant key) {

        close();

      //CS4432-project2: count the number of I/O cost
        int count = 0;

      //CS4432-project2: search key for hash index
        this.key = key;

      //CS4432-project2: initialize the search key for each bucket
        String bucket = convertToHash(key, globalDepth);

        //CS4432-project2: ID of the hash table to store the bucket information
        String tblname = bucket + idxname;

        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

        count++; 

        String current = null;

        while (HashTableScan.next()) {

            if (HashTableScan.getString("hash").equals(bucket)) {

            	//CS4432-project2: get the current position of the table
                current = HashTableScan.getString("pos");

            }

        }

      //CS4432-project2: if the index is empty, we just initialize and insert
        if (current == null) { 

            current = tblname;

            HashTableScan.insert();

          //CS4432-project2:  we should set all relevant information before modifying each index
            HashTableScan.setString("hash", bucket);

            HashTableScan.setInt("valid", MAX_BUCKET_SIZE);

            HashTableScan.setInt("local", 1);

            HashTableScan.setString("pos", tblname);

            System.out.println("Create a new index " + bucket);

        }


        count++;

        Info = new TableInfo(current, sch);

        location = new TableScan(Info, tx);

        System.out.println("Total number of I/O's taken for bucket: " + count);

    }



  //CS4432-project2: search for the next index key
    public boolean next() {

        while (location.next())

            if (location.getVal("dataval").equals(key))

                return true;

        return false;

    }


  //CS4432-project2: get the record ID of the data, which helps to set new record
    public RID getDataRid() {

        int blknum = location.getInt("block");

        int id = location.getInt("id");

        return new RID(blknum, id);

    }



  //CS4432-project2: insert data into bucket
  //Three cases: one is having enough space, just insert; no space, but do not need split global depth; have to split global depth
    public void insert(Constant dataval, RID datarid) {

    	//CS4432-project2: count for cost of I/O
        int count = 0;

        beforeFirst(dataval);

        count++;

        System.out.println("The data is inserted into bucket " + convertToHash(dataval, globalDepth));

        int valid = getvalid(dataval);   //CS4432-project2: number of free space in bucket

        System.out.println("inserting into Extensible index table where the current bucket has " + valid + " space");

      //CS4432-project2: we have enough space
        if (valid > 0) {

            beforeFirst(dataval); 

            location.insert();

            location.setInt("block", datarid.blockNumber());

            location.setInt("id", datarid.id());

            location.setVal("dataval", dataval);

            decrementvalid(dataval);

            count++;

        } else { 

        	//CS4432-project2: the number of local depth equals the global depth, we have to split
            if (getlocal(dataval) == globalDepth) {
                setGlobalDepth(globalDepth + 1);

                TableScan HashTableScan = new TableScan(HashTable, tx);

                HashTableScan.beforeFirst();

                ArrayList<ExtensibleHashTableRecord> currentHashes = new ArrayList<ExtensibleHashTableRecord>();

                while (HashTableScan.next()) {

                	//CS4432-project2: copy all information and data to new bucket
                    if (!HashTableScan.getString("pos").equals("globalDepth")) {

                        String recHash = HashTableScan.getString("hash");

                        int recvalid = HashTableScan.getInt("valid");

                        int reclocal = HashTableScan.getInt("local");

                        String recpos = HashTableScan.getString("pos");

                        currentHashes.add(new ExtensibleHashTableRecord(recHash, recvalid, reclocal, recpos));

                        HashTableScan.delete();

                    }

                }

                for (ExtensibleHashTableRecord record : currentHashes) {

                    HashTableScan.insert();

                    //CS4432-project2: record each information in the index table for later tracking
                    HashTableScan.setString("hash", "0" + record.getRecHash());

                    HashTableScan.setInt("valid", record.getvalid());

                    HashTableScan.setInt("local", record.getlocal());

                    HashTableScan.setString("pos", record.getpos());

                   //CS4432-project2: print out message for splitting the index
                    System.out.println("Splitting the global depth\n");
                    System.out.println("created a new bucket" + HashTableScan.getString("hash"));

                    HashTableScan.insert();

                    HashTableScan.setString("hash", "1" + record.getRecHash());

                    HashTableScan.setInt("valid", record.getvalid());

                    HashTableScan.setInt("local", record.getlocal());

                    HashTableScan.setString("pos", record.getpos());
                    //CS4432-project2: print out message for splitting the index
                    System.out.println("Splitting the global depth\n");
                    System.out.println("created a new bucket" + HashTableScan.getString("hash"));
                }

            }

            beforeFirst(dataval);

          //CS4432-project2:  count the number of I/O cost
          //each time we insert data, increment the number of I/O
            count++;

            ArrayList<ExtensibleHashBucketRecord> records = new ArrayList<ExtensibleHashBucketRecord>();

            location.beforeFirst();

            while (location.next()) {

                Constant recDataval = location.getVal("dataval");

                RID recRID = location.getRid();

                records.add(new ExtensibleHashBucketRecord(recDataval, recRID));

                location.delete();

            }

           

            TableScan HashTableScan = new TableScan(HashTable, tx);

            HashTableScan.beforeFirst();

            ////CS4432-project2:  we split the bucket
            String bucket = convertToHash(dataval, globalDepth);

            System.out.println("splitting buckets of " + bucket);

            while (HashTableScan.next()) {
            	
            	//CS4432-project2: convert the key for each bucket, change to binary

                if (HashTableScan.getString("pos").equals(Info.fileName().substring(0, Info.fileName().length() - 4))) {

                    String temp = HashTableScan.getString("pos");

                    String tempHash = HashTableScan.getString("hash");

                    int local = HashTableScan.getInt("local");

                    String addition = Character.toString(tempHash.charAt(tempHash.length() - local - 1));

                    HashTableScan.setString("pos", addition + temp);

                    HashTableScan.setInt("local", local + 1);

                    HashTableScan.setInt("valid", MAX_BUCKET_SIZE);

                    System.out.println("Change the bucket from " + temp + " to " + addition + temp);

                    count++;

                }

            }


            for (ExtensibleHashBucketRecord record : records) {

            	//CS4432-project2: insert each record into table 
                insert(record.getDataval(), record.getRid());

            }

            insert(dataval, datarid);

        }

        System.out.println("Total number of I/O's taken for insert: " + count);

    }

  //CS4432-project2: same rule for delete
    public void delete(Constant dataval, RID datarid) {

        int count = 0;

        beforeFirst(dataval);

      //CS4432-project2: the count of I/O for delete is 2 each time, because of one for hash table and one for bucket
        count += 2; 

        while (next()) {

            if (getDataRid().equals(datarid)) {

                location.delete();

              //CS4432-project2: increase the number of free space in bucket
                incrementvalid(dataval);

            }

        }

        System.out.println("Total number of I/O's taken for delete: " + count);

    }


    public void close() {

        if (location != null) {

            location.close();

        }

    }

  //CS4432-project2: convert the search key of each bucket to binary with proper length
    private String convertToHash(Constant value, int depth) {

        int hashCode = value.hashCode();

        int mask = (int) Math.pow(2, depth) - 1;

        int bucketVal = hashCode & mask;

        String bucketValString = Integer.toBinaryString(bucketVal);

        while (bucketValString.length() < depth) {

            bucketValString = "0" + bucketValString;

        }

        return bucketValString;

    }

  //CS4432-project2: set the nubmer of global depth to hash table
    private void setGlobalDepth() {

        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

      //CS4432-project2: check for existing record of global depth
        while (HashTableScan.next()) {

            if (HashTableScan.getString("pos").equals("globalDepth")) {

                globalDepth = HashTableScan.getInt("valid");

                return;

            }

        }

      //CS4432-project2: if there is no record for global depth, initialize one 
        HashTableScan.beforeFirst();

        HashTableScan.insert();

        HashTableScan.setInt("valid", 1);

        HashTableScan.setString("pos", "globalDepth");

        globalDepth = 1;

    }



  //CS4432-project2: set the global depth to specific one for existing record
    private void setGlobalDepth(int depth) {


        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

      //CS4432-project2: if there is existing record for global index
        while (HashTableScan.next()) {

            if (HashTableScan.getString("pos").equals("globalDepth")) {

                HashTableScan.setInt("valid", depth);

                globalDepth = depth;

                return;

            }

        }

    }



  //CS4432-project2: decrease the free space in bucket
    private void decrementvalid(Constant dataval) {

        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

        while (HashTableScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);


            if (HashTableScan.getString("hash").equals(bucket)) {

                int currentFreeSpace = HashTableScan.getInt("valid");

                HashTableScan.setInt("valid", currentFreeSpace - 1);

            }

        }

    }



  //CS4432-project2: increase the number of free space in bucket
    private void incrementvalid(Constant dataval) {

        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

        while (HashTableScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);


            if (HashTableScan.getString("hash").equals(bucket)) {

                int currentFreeSpace = HashTableScan.getInt("valid");

                HashTableScan.setInt("valid", currentFreeSpace + 1);

            }

        }

    }



  //CS4432-project2: get the number of how many spaces are free in a bucket
    private int getvalid(Constant dataval) {

        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

        while (HashTableScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);


            if (HashTableScan.getString("hash").equals(bucket)) {

                return HashTableScan.getInt("valid");

            }

        }

        return MAX_BUCKET_SIZE; //if bucket doesn't exist, say it is empty.

    }


  //CS4432-project2: get number of local depth
    private int getlocal(Constant dataval) {

        TableScan HashTableScan = new TableScan(HashTable, tx);

        HashTableScan.beforeFirst();

        while (HashTableScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);

            if (HashTableScan.getString("hash").equals(bucket)) {

                return HashTableScan.getInt("local");

            }

        }

        return -1;

    }

}
