package simpledb.index.hash;


public class ExtensibleHashTableRecord {



    private String recHash;

    private int recFreeSpots;

    private int recLocalDepth;



    public ExtensibleHashTableRecord(String recHash, int recFreeSpots, int recLocalDepth, String recPointerTable) {

        this.recHash = recHash;

        this.recFreeSpots = recFreeSpots;

        this.recLocalDepth = recLocalDepth;

        this.recPointerTable = recPointerTable;

    }



    public String getRecHash() {



        return recHash;

    }



    public int getRecFreeSpots() {

        return recFreeSpots;

    }



    public int getRecLocalDepth() {

        return recLocalDepth;

    }



    public String getRecPointerTable() {

        return recPointerTable;

    }



    private String recPointerTable;



}