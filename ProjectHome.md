Make possible to import in **Solr** data from **Hbase** through the **`DataImport`** Handler

This project provide customs [EntityProcessor](http://wiki.apache.org/solr/DataImportHandler#EntityProcessor) and [DataSource](http://wiki.apache.org/solr/DataImportHandler#DataSource) to connect and keep synchronized an HBase table, supporting `Full-Import` and `Delta-Import`

Here you can find an example of configuration: ConfigurationExample


---



  * The Full-import is working well
  * The Delta-import has one requirement: the first part of the RowId (of the HBase table) has to be the timestamp.



---



