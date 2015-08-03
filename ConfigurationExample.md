Example on how to configure the dataimporthadler

# Example #


Simple example of configuration.

In the example we're importing into Solr data from a table of "messages" in HBase.

```
<dataConfig>

<dataSource type="HbaseDataSource" name="HBase" host="myHBaseServer" />

    <document name="message">

        <entity name="msg" 
        	pk="ROW_KEY"
        	dataSource="HBase"
        	processor="HbaseEntityProcessor"
        	tableName="msg_message" 
        	onError="abort"
            columns="message|msg_date,
                     message|msg_descr,
		     message|msg_level"
            startRow="${dataimporter.last_index_time}"	   
            query=""
            deltaImportQuery=""
            deltaQuery="" >
            
            <field column="ROW_KEY" name="msg_id" />
            <field column="msg_date" name="msg_date" />
            <field column="msg_descr" name="msg_descr" />
            <field column="msg_level" name="msg_level" />
            
        </entity>


    </document>
</dataConfig>
```