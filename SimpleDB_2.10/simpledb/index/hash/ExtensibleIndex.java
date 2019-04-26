package simpledb.index.hash;

import simpledb.file.Block;

import simpledb.index.Index;

import simpledb.query.Constant;

import simpledb.query.TableScan;

import simpledb.record.RID;

import simpledb.record.RecordFile;

import simpledb.record.Schema;

import simpledb.record.TableInfo;

import simpledb.tx.Transaction;



import java.util.ArrayList;



import static simpledb.metadata.TableMgr.MAX_NAME;



public class ExtensibleIndex implements Index {



    public static final int MAX_BUCKET_SIZE = 200;

    public static final int MAX_HASH_SIZE = 100;



    private String idxname;

    private Schema sch;

    private Transaction tx;

    private Constant searchKey;

    private TableScan currTableScan; //table scan of bucket correlating to the searchkey

    private TableInfo currTableInfo;

    private TableInfo hashTbl;

    private int globalDepth = 0;



    //done

    public ExtensibleIndex(String idxname, Schema sch, Transaction tx) {

        this.idxname = idxname;

        this.sch = sch;

        this.tx = tx;

        //Table info for upper level hash table. Schema for table is hash number, free spots in that bucket, and name of table that represents the bucket

        String hashTable = idxname + "ehashtbl";

        Schema hashTableSch = new Schema();

        hashTableSch.addStringField("hash", MAX_HASH_SIZE);

        hashTableSch.addIntField("freeSpots");

        hashTableSch.addIntField("localDepth");

        hashTableSch.addStringField("pointerTable", MAX_NAME);

        hashTbl = new TableInfo(hashTable, hashTableSch);

        setGlobalDepth(); //puts global depth into memory

    }



    /**

     * Sets the searchkey to be looked at and loads the bucket that the key corresponds to into memory

     *

     * @param searchkey the search key value.

     */

    //done

    public void beforeFirst(Constant searchkey) {

        close();

        int iocount = 0;

        this.searchKey = searchkey;

        //get name of table that record would be in. Do this by finding hash

        String bucket = convertToHash(searchkey, globalDepth);

        String tblname = bucket + idxname;

        //open table scan on hash table to get actual table name

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        iocount++; //load hash into memory

        String actualTable = null;

        while (hashTblScan.next()) {

            if (hashTblScan.getString("hash").equals(bucket)) {

                actualTable = hashTblScan.getString("pointerTable");

            }

        }

        if (actualTable == null) { //case where index is empty

            actualTable = tblname;

            hashTblScan.insert();

            hashTblScan.setString("hash", bucket);

            hashTblScan.setInt("freeSpots", MAX_BUCKET_SIZE);

            hashTblScan.setInt("localDepth", 1);

            hashTblScan.setString("pointerTable", tblname);

            System.out.println("New index of " + bucket + " added to hash");

        }

        //open new table scan object. If table doesn't exist, it will now.

        iocount++;

        currTableInfo = new TableInfo(actualTable, sch);

        currTableScan = new TableScan(currTableInfo, tx);

        System.out.println("Total number of I/O's taken for bucket lookup: " + iocount);

    }



    //done

    public boolean next() {

        while (currTableScan.next())

            if (currTableScan.getVal("dataval").equals(searchKey))

                return true;

        return false;

    }



    //done

    public RID getDataRid() {

        int blknum = currTableScan.getInt("block");

        int id = currTableScan.getInt("id");

        return new RID(blknum, id);

    }



    //needs split code

    public void insert(Constant dataval, RID datarid) {

        int iocount = 0;

        //check current number of free spaces. If it is zero, we must split it (and maybe increase global depth).

        beforeFirst(dataval);

        iocount++;

        System.out.println("Bucket of insert is supposed  to go to bucket " + convertToHash(dataval, globalDepth));

        int freeSpots = getFreeSpots(dataval);

        System.out.println("inserting into ehash table where val has " + freeSpots + " free spots");

        if (freeSpots > 0) {

            beforeFirst(dataval); //get to table/bucket we want

            currTableScan.insert();

            currTableScan.setInt("block", datarid.blockNumber());

            currTableScan.setInt("id", datarid.id());

            currTableScan.setVal("dataval", dataval);

            decrementFreeSpots(dataval);

            iocount++;

        } else { //split case

            //methodology: check if global depth needs to be increased,

            // save records in memory, edit all hash records that relate to the bucket (increase local depths),

            // Iterate through records and insert them into hash table.

            // insert last new record in.

            //Notes: Always split into two



            //if we need to increase global depth, duplicate all hash entries and have them point to table.

            if (getLocalDepth(dataval) == globalDepth) { //need to split. Doesn't count as I/O because hash table should already be in memory

                setGlobalDepth(globalDepth + 1);

                TableScan hashTblScan = new TableScan(hashTbl, tx);

                hashTblScan.beforeFirst();

                ArrayList<ExtensibleHashTableRecord> currentHashes = new ArrayList<ExtensibleHashTableRecord>();

                while (hashTblScan.next()) {

                    if (!hashTblScan.getString("pointerTable").equals("globalDepth")) { //every record but metadata record

                        String recHash = hashTblScan.getString("hash");

                        int recFreeSpots = hashTblScan.getInt("freeSpots");

                        int recLocalDepth = hashTblScan.getInt("localDepth");

                        String recPointerTable = hashTblScan.getString("pointerTable");

                        currentHashes.add(new ExtensibleHashTableRecord(recHash, recFreeSpots, recLocalDepth, recPointerTable));

                        hashTblScan.delete();

                    }

                }

                for (ExtensibleHashTableRecord record : currentHashes) {

                    hashTblScan.insert();

                    hashTblScan.setString("hash", "0" + record.getRecHash());

                    hashTblScan.setInt("freeSpots", record.getRecFreeSpots());

                    hashTblScan.setInt("localDepth", record.getRecLocalDepth());

                    hashTblScan.setString("pointerTable", record.getRecPointerTable());

                    System.out.println("Just created new bucket of " + hashTblScan.getString("hash") + "because of splitting due to global depth increasing");

                    hashTblScan.insert();

                    hashTblScan.setString("hash", "1" + record.getRecHash());

                    hashTblScan.setInt("freeSpots", record.getRecFreeSpots());

                    hashTblScan.setInt("localDepth", record.getRecLocalDepth());

                    hashTblScan.setString("pointerTable", record.getRecPointerTable());

                    System.out.println("Just created new bucket of " + hashTblScan.getString("hash") + "because of splitting due to global depth increasing");

                }

            }

            //save records in memory for easy inserting

            beforeFirst(dataval);

            iocount++;

            ArrayList<ExtensibleHashBucketRecord> records = new ArrayList<ExtensibleHashBucketRecord>();

            currTableScan.beforeFirst();

            while (currTableScan.next()) {

                Constant recDataval = currTableScan.getVal("dataval");

                RID recRID = currTableScan.getRid();

                records.add(new ExtensibleHashBucketRecord(recDataval, recRID));

                currTableScan.delete();

            }

            //debug print

            System.out.println("Currently splitting a bucket with " + records.size() + " records");

            //split bucket into two and scan through hash table to update local depth and free spots (make it empty and increase local depth

            TableScan hashTblScan = new TableScan(hashTbl, tx);

            hashTblScan.beforeFirst();

            String bucket = convertToHash(dataval, globalDepth);

            System.out.println("splitting buckets relating to " + bucket);

            while (hashTblScan.next()) {

                //look for entries with the table name minus the .tbl extension

                if (hashTblScan.getString("pointerTable").equals(currTableInfo.fileName().substring(0, currTableInfo.fileName().length() - 4))) {

                    String tempPointer = hashTblScan.getString("pointerTable");

                    String tempHash = hashTblScan.getString("hash");

                    int localDepth = hashTblScan.getInt("localDepth");

                    String addition = Character.toString(tempHash.charAt(tempHash.length() - localDepth - 1));

                    hashTblScan.setString("pointerTable", addition + tempPointer);

                    hashTblScan.setInt("localDepth", localDepth + 1);

                    hashTblScan.setInt("freeSpots", MAX_BUCKET_SIZE);

                    System.out.println("Changing bucket from " + tempPointer + " to " + addition + tempPointer);

                    iocount++;

                }

            }

            //iterate through records and insert them (this function)

            for (ExtensibleHashBucketRecord record : records) {

//                beforeFirst(record.getDataval());

//                currTableScan.insert();

//                currTableScan.setInt("block", record.getRid().blockNumber());

//                currTableScan.setInt("id", record.getRid().id());

//                currTableScan.setVal("dataval", record.getDataval());

//                decrementFreeSpots(dataval);

                insert(record.getDataval(), record.getRid());

            }

            //add last record

//            beforeFirst(dataval); //get to table/bucket we want

//            currTableScan.insert();

//            currTableScan.setInt("block", datarid.blockNumber());

//            currTableScan.setInt("id", datarid.id());

//            currTableScan.setVal("dataval", dataval);

//            decrementFreeSpots(dataval);

            insert(dataval, datarid);

        }

        System.out.println("Total number of I/O's taken for insert: " + iocount);

    }



    //done

    public void delete(Constant dataval, RID datarid) {

        int iocount = 0;

        beforeFirst(dataval);

        iocount += 2; //2 for loading hash table and then loading bucket

        //Only concerned with records in our currrent hash bucket because duplicates of dataval would be together

        while (next()) {

            if (getDataRid().equals(datarid)) {

                currTableScan.delete();

                //increase number of free space

                incrementFreeSpots(dataval);

            }

        }

        System.out.println("Total number of I/O's taken for insert: " + iocount);

    }



    //done

    public void close() {

        if (currTableScan != null) {

            currTableScan.close();

        }

    }



    /**

     * Helper function that gets the least significant bits of a value's HashCode for indexing.

     *

     * @param value The constant

     * @param depth How many bits to look at.

     * @return A string of 0's and 1's that are the last bits of a hash code.

     */

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



    /**

     * Scans through hash table looking for metadata on global depth.

     * This is stored in it's own record  with a freeSpots value of the global depth

     * and a pointerTable value of "globalDepth"

     */

    private void setGlobalDepth() {

        //First check to see if metadata record with global depth exists.

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            if (hashTblScan.getString("pointerTable").equals("globalDepth")) {

                globalDepth = hashTblScan.getInt("freeSpots");

                return;

            }

        }

        //reached end of hash table without finding metadata record. This means table is new and needs one

        hashTblScan.beforeFirst();

        hashTblScan.insert();

        hashTblScan.setInt("freeSpots", 1); //set global depth to be 1

        hashTblScan.setString("pointerTable", "globalDepth");

        globalDepth = 1;

    }



    /**

     * Scans through hash table looking for metadata on global depth.

     * This is stored in it's own record  with a freeSpots value of the global depth

     * and a pointerTable value of "globalDepth". This function writes a new global depth to the table

     */

    private void setGlobalDepth(int depth) {

        //First check to see if metadata record with global depth exists.

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            if (hashTblScan.getString("pointerTable").equals("globalDepth")) {

                hashTblScan.setInt("freeSpots", depth);

                globalDepth = depth;

                return;

            }

        }

    }



    /**

     * Scans through hash table and looks for entry that points to bucket.

     * Decreases the number of free spaces. Used in adding elements.

     *

     * @param dataval record that is being added. Used for bucket lookup

     */

    private void decrementFreeSpots(Constant dataval) {

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);

            String tblname = idxname + bucket;

            if (hashTblScan.getString("hash").equals(bucket)) {

                int currentFreeSpace = hashTblScan.getInt("freeSpots");

                hashTblScan.setInt("freeSpots", currentFreeSpace - 1);

            }

        }

    }



    /**

     * Scans through hash table and looks for entry that points to bucket.

     * Increases the number of free spaces. Used in deleting elements.

     *

     * @param dataval record that is being added. Used for bucket lookup

     */

    private void incrementFreeSpots(Constant dataval) {

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);

            String tblname = idxname + bucket;

            if (hashTblScan.getString("hash").equals(bucket)) {

                int currentFreeSpace = hashTblScan.getInt("freeSpots");

                hashTblScan.setInt("freeSpots", currentFreeSpace + 1);

            }

        }

    }



    /**

     * Scans through hash table and looks for entry that points to bucket

     * Sets the number of free spots available in that bucket.

     *

     * @param dataval    record that is being added. Used for bucket lookup

     * @param freeSpaces number of free spaces left in that bucket.

     */

    private void setFreeSpots(Constant dataval, int freeSpaces) {

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);

            String tblname = idxname + bucket;

            if (hashTblScan.getString("hash").equals(bucket)) {

                hashTblScan.setInt("freeSpots", freeSpaces);

                return; //exit early to save time

            }

        }

    }



    /**

     * Finds number of free spots in a bucket

     *

     * @param dataval record that is being added. Used for bucket lookup

     * @return number of free spaces left in that bucket.

     */

    private int getFreeSpots(Constant dataval) {

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);

            String tblname = idxname + bucket;

            if (hashTblScan.getString("hash").equals(bucket)) {

                return hashTblScan.getInt("freeSpots");

            }

        }

        return MAX_BUCKET_SIZE; //if bucket doesn't exist, say it is empty.

    }



    /**

     * Finds local depth of a bucket

     *

     * @param dataval record that is used to determine which bucket to look at

     * @return local bit hash depth for that bucket

     */

    private int getLocalDepth(Constant dataval) {

        TableScan hashTblScan = new TableScan(hashTbl, tx);

        hashTblScan.beforeFirst();

        while (hashTblScan.next()) {

            String bucket = convertToHash(dataval, globalDepth);

            String tblname = idxname + bucket;

            if (hashTblScan.getString("hash").equals(bucket)) {

                return hashTblScan.getInt("localDepth");

            }

        }

        return -1; //if bucket doesn't exist, return error number

    }

}
