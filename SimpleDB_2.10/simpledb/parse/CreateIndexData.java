package simpledb.parse;

/**
 * The parser for the <i>create index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {
   private String type, idxname, tblname, fldname;
   
   /**
    * Saves the table and field names of the specified index.
    */
   public CreateIndexData(String type,String idxname, String tblname, String fldname) {
	  this.type = type;
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
   }
  
   
   /**
    * CS4432: get index type
    */
   
   public String gettype() {
	   return type;
   }
   /**
    * Returns the name of the index.
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }
}

