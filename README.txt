//////////////////////////////////////////////////////////////////////////////
//////////////////////////// CS 525 Summer 2018 ///////////////////////////////
////////////////////// Yanbo Deng, Xinghang Ma, Yitong Huang //////////////////
///////////////////////////////////////////////////////////////////////////////
**************************** Assignment  *************************************

********** HOW TO RUN **********
1. Go to the folder which contains source/ README.txt and Makefile（please run in Linux machine）
2. > make
3. > ./test_assign3
4. > make clean

/////////////////////////////
/* preparation */
/////////////////////////////
1. define and initialize the table pointer
2. serialize the first page to store current table states

/////////////////////////////
/* table and manager */
/////////////////////////////

1. initRecordManager

2. shutdownRecordManager

3. createTable
1) use function serializeSchema in rm_serializer to retieve schema info
2) get how many page to save schema info
3) allocate memory to save shema info, store shema page numberand schema info to this memory

4. openTable
1) get how many page to save schema info
2) allocate memory for schema and read it
3) initialize keySize and keys memory
4) check schema info and attribute info
5) read datatype and typelenth from schema
6) assign schema details

5. closeTable
1) shutdown buffer pool
2) free schema using function freeSchema

6. deleteTable

7. getNumTuples
1) return number of tuples using pinPage function


/////////////////////////////
/* handling records in a table */
/////////////////////////////
1. insertRecord 
1) get record size and total slot number
2) insert record into appropriate slot, add a new page if all slots full
3) do markdirty, unpinPage and forcepage to update

2. deleteRecord 
1) find the position and delete the record

3. updateRecord 
1) use pinPage, markDirty, unpinPage, forcePage for updating

4. getRecord 
1) use pinPage for updating
2) get the record position and update it

/////////////////////////////
/* Scans */
/////////////////////////////
1. startScan
1) initialize RM_ScanData structure
2) assign scanDAta to scan

2. next
1) retrive the record according to the page id and slot id
2) check if the record is the one required by user, if not then check next 
 
3. closeScan
1) free scan info

/////////////////////////////
/*    Dealing with schemas  */
/////////////////////////////

1. getRecordSize
1) initial record size for data.
2) get record size from data.Different datatype got different cases
3) return recordSize.

2. createSchema
1) Allocate schema with spaces and different parameters. 
2) return schema .

3. freeSchema
1) free the schema been created
2) return RC_OK.


////////////////////////////////////////////////
/*    dealing with records and attribute values  */
////////////////////////////////////////////////

1. createRecord
1) create record then store them in data
2) Return RC_OK.

2. freeRecord
1) free the record in memory 
3) Return RC_OK. 

3. getAttr
1) check if attribute number is bigger than all attributes
2) get attributes value then calculate the distance to target position
3) add attribute size for different dataTypes then find attributes values.
4) copy from attributes to memory.Return RC_OK

4. setAttr
1) check if the value is right dataType
2) get attributes value then calculate the distance to target position
3) set data to the target position.
4) copy data from memory to attributes.Return RC_OK


