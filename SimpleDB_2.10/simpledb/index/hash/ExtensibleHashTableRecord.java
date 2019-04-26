package simpledb.index.hash;
//CS4432-project2: Name: Dian Chen / Yang Gao

public class ExtensibleHashTableRecord {



    private String recHash;

    private int valid;

    private int local;



    public ExtensibleHashTableRecord(String recHash, int valid, int local, String pos) {

    	//CS4432-project2: these are relevant information we want to track for printing out
        this.recHash = recHash;

      //CS4432-project2: the number of free space in bucket
        this.valid = valid;

      //CS4432-project2: number of local depth
        this.local = local;

      //CS4432-project2: current position for a hash table
        this.pos = pos;

    }



    public String getRecHash() {



        return recHash;

    }



    public int getvalid() {

        return valid;

    }



    public int getlocal() {

        return local;

    }



    public String getpos() {

        return pos;

    }



    private String pos;



}