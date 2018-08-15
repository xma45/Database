#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#include "expr.h"
#include "tables.h"



RC initRecordManager (void *mgmtData) {
    printf("Start to generate record manager!\n");
    return RC_OK;
}

RC shutdownRecordManager () {
    printf("Shutdown record manager!\n");
    return RC_OK;
}


// table and manager
RC createTable (char *name, Schema *schema) {
    createPageFile(name);
    SM_FileHandle fh; 
    openPageFile(name, &fh);
//use function serializeSchema in rm_serializer to retieve schema info
    char* getSchema = serializeSchema(schema);
//get how many page to save schema info
    int schemaPageNum = (sizeof(getSchema)  + PAGE_SIZE)/(PAGE_SIZE+1);
//allocate memory to save shema info, store shema page numberand schema info to this memory
    char* metadataPage = (char *)malloc(PAGE_SIZE);
    memset(metadataPage, 0, sizeof(metadataPage));
    memmove(metadataPage, &schemaPageNum, sizeof(schemaPageNum));
    memmove(metadataPage+sizeof(schemaPageNum), getSchema, strlen(getSchema)); 
    writeBlock(0, &fh, metadataPage);
    int i=1;
    while(i < schemaPageNum){
	memset(metadataPage, 0, sizeof(metadataPage));
        memmove(metadataPage, i * PAGE_SIZE + getSchema, PAGE_SIZE);
	writeBlock(i, &fh, metadataPage);
        free(metadataPage);
	i=i+1;
    }
    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name) {
    SM_FileHandle fh;
    openPageFile(name, &fh);
    BM_BufferPool *bm = (BM_BufferPool*) malloc(sizeof(BM_BufferPool));
    initBufferPool(bm, name, 1000, RS_FIFO, NULL);
//get how many page to save schema info
    BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
    pinPage(bm, ph, 0);
    int metadataPage =*ph->data;
//allocate memory for schema and read it
    char* metadata = (char *)malloc(sizeof(char)*PAGE_SIZE);
    int i=0;
    while(i < metadataPage){
	readBlock(i, &fh, i * PAGE_SIZE + metadata);
	i=i+1;
    }
//initialize keySize and keys memory
    int keySize=1;
    int *keys=(int *)malloc(sizeof(int));
//check schema info
    char *attrInfo = strstr(metadata + 4, "<")+1;
   // printf("Schema has %s \n",  attrInfo-1 );
    char* attrList = (char *)malloc(sizeof(char));
    memset(attrList, 0, sizeof(attrList));
//read number of attributes and assign it to numAttr
    i = 0;
    while(*(attrInfo + i) != '>'){
        attrList[i] = attrInfo[i];
	i = i+1;
    }
    int numAttr = atol(attrList);
    char **attrNames = (char**) malloc(sizeof(char *)*numAttr);
    free(attrList);
//get posistion of "("
    attrInfo = strstr(attrInfo, "(")+1;
    DataType *dataType = (DataType*) malloc(sizeof(DataType)*numAttr);
    int *typeLength = (int*) malloc(sizeof(int)*numAttr);
    i=0;
    int j;
//read datatype and typelenth from schema
    while(i<numAttr){
        for(j = 0; ; j++){
		int size = 50;
		char *str = (char *)malloc(sizeof(char)*size);
		str=attrInfo + j;
  		char *dest= (char *)malloc(sizeof(char)*size);
		strncpy(dest, str, 1);
		if(strcmp(dest,":")==0){
		attrNames[i] = (char*) malloc(sizeof(char)*j);
                memcpy(attrNames[i], attrInfo, j);

		char *str1 = (char *)malloc(sizeof(char)*size);
		str1=attrInfo + j + 2;
  		char *dest1= (char *)malloc(sizeof(char)*size);
		char *dest2= (char *)malloc(sizeof(char)*size);
		char *dest3= (char *)malloc(sizeof(char)*size);
		strncpy(dest1, str1, 6);
		strncpy(dest2, str1, 3);
		strncpy(dest3, str1, 5);

		if(strcmp(dest1,"STRING")==0){                      
		    attrList=(char *)malloc(sizeof(char));
                    int k=0;
		    while(*(attrInfo + j + 9 + k)!=']'){
		        attrList[k] = attrInfo[k+j+9];
		        k=k+1;
		    }
                    dataType[i] = DT_STRING;
                    typeLength[i] = atol(attrList);           
                }else if(strcmp(dest2,"INT")==0){ 
                    dataType[i] = DT_INT;       
                    typeLength[i] = 0;
                }else if(strcmp(dest3,"FLOAT")==0){
                    dataType[i] = DT_FLOAT;              
                    typeLength[i] = 0;
                } else {
                    dataType[i] = DT_BOOL;              
                    typeLength[i] = 0;
                } 
//check if read to the last attribute
               if (i != numAttr-1){
                    attrInfo = strstr(attrInfo, ",");
                    attrInfo = attrInfo + 2;
                }
                break;

            }
        }i++;
    }
//assign schema details
    Schema *schema = createSchema(numAttr, attrNames, dataType, typeLength, keySize, keys);
    rel->name = name;
    rel->schema = schema;
    rel->mgmtData = bm;
    return RC_OK;
}


RC closeTable (RM_TableData *rel){

	
    shutdownBufferPool((BM_BufferPool*)rel->mgmtData);

    freeSchema(rel->schema);
    free(rel->mgmtData);
    int i = 0;
    while(i < rel->schema->numAttr){
	free(rel->schema->attrNames[i]);
        i = i+1;
    }
    return RC_OK;
}

RC deleteTable (char *name){
     remove(name);
     return RC_OK;
}


int getNumTuples (RM_TableData *rel){
    BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
    int tupleNum;
    pinPage(rel->mgmtData, ph, 0);
    return *ph->data+ sizeof(int);

}

// handling records in a table

RC insertRecord (RM_TableData *rel, Record *record){

	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	int rsize = getRecordSize(rel->schema);
        int curSlotSize = (rsize + SLOTSIZE -1)/SLOTSIZE;
        int firstSlot = (2+SLOTSIZE -1)/ SLOTSIZE;

	int slotNum;
	int firstPage;
	BM_PageHandle *ph1 = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	pinPage(rel->mgmtData, ph1, 0);
        memmove(&firstPage, ph1->data, sizeof(int));

	SM_FileHandle fh;
	BM_BufferPool *bm = rel->mgmtData;
	openPageFile(bm->pageFile, &fh);
	
	int lastPage = fh.totalNumPages - 1;
	closePageFile(&fh);

	while(firstPage<=lastPage){
	    pinPage(rel->mgmtData, ph, firstPage); 
            int flag;
            memmove(&flag, ph->data+12, 4);
		if(flag > 0){  
		    slotNum = flag; 
		    RID id;
 		    memmove(&id, ph->data + 12, 4);
		    flag = id.slot;
		    memmove(&flag, ph->data+12, 4);
		}
	    int totalRec;
	    memmove(&totalRec, ph->data+4, 4);
	    int maxSize;
            memmove(&maxSize,ph->data+8, 4);
		if(totalRec < maxSize){
		    slotNum = firstSlot + totalRec*curSlotSize;
                    memmove(ph->data + 4, &totalRec+1, 4);
	            break;
		}
	firstPage = firstPage +1;
	}

	
	if(firstPage == lastPage+1){
	    SM_FileHandle fh;
	    openPageFile(((BM_BufferPool *)(rel->mgmtData))->pageFile, &fh);
	    appendEmptyBlock(&fh);
	    char* recPage = (char *) malloc(PAGE_SIZE*sizeof(char));
            int maxSize = (PAGE_SIZE - 4) / (SLOTSIZE);
            memmove(recPage + 8, &maxSize, sizeof(int));
	    writeBlock(fh.totalNumPages - 1, &fh, recPage);
	    free(recPage);
	    pinPage(rel->mgmtData, ph, firstPage);
	    slotNum = firstSlot;
	    int totalRec = 1;
            memmove(ph->data + 4, &totalRec, 4);
	    closePageFile(&fh);
	}

	RID id;
	id.page = -1;
	id.slot = -1;
	memmove(ph->data + SLOTSIZE * slotNum + 2, &id, 4);
        memmove(ph->data + SLOTSIZE * slotNum + 10, record->data, 12);
	markDirty(rel->mgmtData, ph);
	record->id.slot = slotNum;
	record->id.page = firstPage;

	free(ph);
	return RC_OK;
}


RC deleteRecord (RM_TableData *rel, RID id){
	
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	pinPage(rel->mgmtData, ph, id.page);

	int flag;
	memmove(&flag, ph->data + 12, 4);
	free(ph);
	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record){
	
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	pinPage(rel->mgmtData, ph, record->id.page);
	int rsize = getRecordSize(rel->schema);
	memmove(ph->data +  record->id.slot * SLOTSIZE + 10, record->data, 12);
	free(ph);

	return RC_OK;
}


RC getRecord (RM_TableData *rel, RID id, Record *record){
	
	int rsize = getRecordSize(rel->schema);
	
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
        pinPage(rel->mgmtData, ph, id.page);
	RID ID;
	memmove(&ID, ph->data + SLOTSIZE, 4);
        memmove(record->data, ph->data + SLOTSIZE * id.slot + 10, 12);

	record->id = ID;
	return RC_OK;
}


RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	scan->rel = rel;
	scan->expr = cond;
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	pinPage(scan->rel->mgmtData, ph, 0);
        memmove(&(scan->current.page), ph->data, 4);
	scan->current.slot = 1;

	return RC_OK;
}


RC next (RM_ScanHandle *scan, Record *record){
	BM_BufferPool *bm = (BM_BufferPool*)scan->rel->mgmtData;
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));

	int rsizeInSlots = (getRecordSize(scan->rel->schema) + SLOTSIZE )/SLOTSIZE;
	int stateSlotSize = 1;

	SM_FileHandle fh; 
	openPageFile(bm->pageFile, &fh);
	int lastPage = fh.totalNumPages - 1;
	closePageFile(&fh);

	Record *rec;
	createRecord(&rec, scan->rel->schema);
	Value *result = (Value*) malloc(sizeof(Value));
	while(scan->current.page <= lastPage){
		pinPage(bm, ph, scan->current.page); 
		int totalRec,totalNumSlots;
		memcpy(&totalRec, ph->data + sizeof(int), sizeof(int));
		totalNumSlots = totalRec*rsizeInSlots + stateSlotSize;
		while(scan->current.slot < totalNumSlots){
			RC flag=getRecord(scan->rel, scan->current, rec);
			if(flag==0){
				evalExpr(rec, scan->rel->schema, scan->expr, &result);
                                if(result->v.boolV==1){
					record->id.slot = rec->id.slot;
					record->id.page = rec->id.page;
					memmove(record->data, rec->data, getRecordSize(scan->rel->schema));

                                        if(scan->current.slot > totalRec){
						scan->current.page += scan->current.page;
                                                scan->current.slot = 0;
						
                                        }else{
                                                scan->current.slot += rsizeInSlots;
                                        }
					return RC_OK;
				}
			}
		scan->current.slot += rsizeInSlots;
		}
		scan->current.page += scan->current.page;
		scan->current.slot = 0;

	}

	return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan){

	return RC_OK;
}

// dealing with schemas

int getRecordSize (Schema *schema){
	int i;
	int size = 0;

	for(i = 0; i < schema->numAttr; i++){
	    if(schema->dataTypes[i] == DT_INT){
            size = size + sizeof(int);
	    }else if(schema->dataTypes[i] == DT_STRING){
            size = size + schema->typeLength[i];
	    }else if(schema->dataTypes[i] == DT_FLOAT){
            size = size + sizeof(float);
	    }else{
            size = size + sizeof(bool);
            }
	}
	return size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	Schema *schema = (Schema*) malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    return schema;
}

RC freeSchema (Schema *schema){
	int i;
	for(i = 0; i < schema->numAttr; i++){
		free(schema->attrNames[i]);
	}
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	
	
        free(schema);
	return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
	*record = (Record *) malloc(sizeof(struct Record));
	(*record)->data = (char *) malloc(getRecordSize(schema)* sizeof(char));

	return RC_OK;
}

RC freeRecord (Record *record){
	free(record->data);
	free(record);

	return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	*value = (Value *) malloc(sizeof(Value));
	int i;
	int position = 0;
	
	for(i = 0; i < attrNum; i++){
	    if(schema->dataTypes[i] == DT_INT){
            position = position + sizeof(int);
	    }else if(schema->dataTypes[i] == DT_STRING){
            position = position + schema->typeLength[i];
	    }else if(schema->dataTypes[i] == DT_FLOAT){
            position = position + sizeof(float);
	    }else{
            position = position + sizeof(bool);
            }
	}
	
	(*value)->dt = schema->dataTypes[attrNum];
        if((*value)->dt == DT_INT)
        {
        memmove(&((*value)->v.intV), record->data + position, sizeof(int));
    	}else if((*value)->dt == DT_FLOAT)
    	{
        memmove(&((*value)->v.floatV), record->data + position, sizeof(float));
    	}else if((*value)->dt == DT_STRING)
    	{
        (*value)->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
        memmove((*value)->v.stringV, record->data + position, schema->typeLength[attrNum]);
        char end = '\0';
        memmove((*value)->v.stringV + schema->typeLength[attrNum], &end, sizeof(char));
    	}else{
        memmove(&((*value)->v.boolV), record->data + position, sizeof(bool));
       }
	return RC_OK;
}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	int i;
        int size = 0;
	int position = 0;

	for(i = 0; i < attrNum; i++){
		if(schema->dataTypes[i] == DT_INT){
            position = position + sizeof(int);
	    }else if(schema->dataTypes[i] == DT_STRING){
            position = position + schema->typeLength[i];
	    }else if(schema->dataTypes[i] == DT_FLOAT){
            position = position + sizeof(float);
	    }else{
            position = position + sizeof(bool);
            }
	}

	
    if(schema->dataTypes[attrNum] == DT_INT)
    {
        memmove((record->data + position),&(value->v.intV), sizeof(int));
    }else if(schema->dataTypes[attrNum] == DT_STRING)
    {
        if (sizeof(value->v.stringV) >= schema->typeLength[attrNum])
        {
            size = schema->typeLength[attrNum];
        }else
        {
            size = sizeof(value->v.stringV);
        }

        memmove( record->data + position, value->v.stringV, size);
    }
    else if(schema->dataTypes[attrNum] == DT_FLOAT)
    {
        memmove((record->data + position),&((value->v.floatV)), sizeof(float));
    }else 
    {
        memmove((record->data + position),&((value->v.boolV)), sizeof(bool));
    }
	return RC_OK;
}








