#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "sort_file.h"
#include "bf.h"
#include "sort_file_helping.h"

#define CALL_OR_ERROR(call) \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK){       \
    BF_PrintError(code);    \
    return SR_ERROR;        \
  }                         \
}


SR_ErrorCode sort (BF_Block **array, int fd, int bufferSize, int fieldNo) {
    int last_record_num, i;
    char *data;
    SR_Metadata metadata;
    getMetadata(fd, &metadata);

    //the numbers of records are in [0, recordCount-1]
    last_record_num = metadata.recordCount - 1;

    //get blocks and save them in array
    for (i=1; i<metadata.blockCount; i++)
        CALL_OR_ERROR(BF_GetBlock(fd, i, array[i-1]));

    quicksort (array, 0, last_record_num, fieldNo, metadata.blockCapacity);

    //unpin blocks
    for (i=1; i<metadata.blockCount; i++)
        CALL_OR_ERROR(BF_UnpinBlock(array[i-1]));
}


void quicksort (BF_Block **array, int p, int r, int fieldNo, int blockCapacity) {
    int q;
    if (p<r) {
        q = partition (array, p, r, fieldNo, blockCapacity);
        quicksort (array, p, q-1, fieldNo, blockCapacity);
        quicksort (array, q+1, r, fieldNo, blockCapacity);
    }
}

int partition (BF_Block **array, int p, int r, int fieldNo, int blockCapacity) {
    int i;
    Record currentRecord, pivot;
    getRecord (array, r, &pivot, blockCapacity);

    i = p-1;

    for (int j=p; j<=r-1; j++) {
        getRecord(array, j, &currentRecord, blockCapacity);
        if (record_LessThanOrEqual(&currentRecord, &pivot, fieldNo)) {
            i++;
            swapElements(array, i, j, blockCapacity);
        }
    }
    //swap pivot
    swapElements(array, r, i+1, blockCapacity);
    return i+1;

}

int record_LessThanOrEqual (Record *record1, Record *record2, int fieldNo) {
    switch(fieldNo) {
        case 0:
            return (record1->id <= record2->id);
        case 1:
            return (strcmp(record1->name, record2->name) <= 0);
        case 2:
            return (strcmp(record1->surname, record2->surname) <= 0);
        case 3:
            return (strcmp(record1->city, record2->city) <= 0);
        default:
            return -1;
    }
}

//returns the record of position i
SR_ErrorCode getRecord (BF_Block **array, int i, Record *record, int blockCapacity) {
    char *data;
    int blockNo, record_position;
    Record rec;

    //block 0 is the metadata block
    blockNo = i / blockCapacity + 1;

    //get data of the block
    data = BF_Block_GetData(array[blockNo-1]);

    record_position = i % blockCapacity;

    data += record_position * sizeof(Record);

    memcpy (record, data, sizeof(Record));
    
    return SR_OK;
}

SR_ErrorCode swapElements (BF_Block **array, int i, int j, int blockCapacity) {
    char *data1, *data2;
    Record temp;
    int blockNo1, blockNo2, record_position1, record_position2;
    
    Record r;

    blockNo1 = i / blockCapacity + 1;
    blockNo2 = j / blockCapacity + 1;

    //get data of the block
    data1 = BF_Block_GetData(array[blockNo1 - 1]);
    data2 = BF_Block_GetData(array[blockNo2 - 1]);

    record_position1 = i % blockCapacity;
    record_position2 = j % blockCapacity;

    data1 += record_position1 * sizeof(Record);
    data2 += record_position2 * sizeof(Record);

    memcpy(&temp, data1, sizeof(Record));
    memcpy(data1, data2, sizeof(Record));
    memcpy(data2, &temp, sizeof(Record));

    BF_Block_SetDirty(array[blockNo1 - 1]);
    BF_Block_SetDirty(array[blockNo2 - 1]);

    return SR_OK;
}


SR_ErrorCode getMetadata(int fileDesc, SR_Metadata *metadata){
  BF_Block *block;
  BF_Block_Init(&block);

  CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, block));
  char *data = BF_Block_GetData(block);

  memcpy(metadata, data, sizeof(SR_Metadata));

  CALL_OR_ERROR(BF_UnpinBlock(block));

  return SR_OK;
}

SR_ErrorCode splitFile(const char *fileName, int bufferSize, int *fileCount, int **fd){
  char tempName[50];
  char *dataIn, *dataOut;
  int fileDesc, curFd;
  int blocksLeft;

  SR_OpenFile(fileName, &fileDesc);

  SR_Metadata metadata, curMetadata;
  getMetadata(fileDesc, &metadata);

  BF_Block *blockIn, *blockOut;
  BF_Block_Init(&blockIn);
  BF_Block_Init(&blockOut);

  *fileCount = (metadata.blockCount - 1) / bufferSize;
  if((metadata.blockCount - 1) % bufferSize != 0) (*fileCount)++; //we need more space

  *fd = malloc(*fileCount * sizeof(int));
  if(*fd == NULL) return SR_ERROR;

  //Create a dir to store temporary files
  if(mkdir("./temp", 0777) == -1) return SR_ERROR;

  int curBlock = 1; //Block to be copied from the original file
  for(int i = 0; i < *fileCount; i++){
    sprintf(tempName, "./temp/temp-0-%d.db", i);
    SR_CreateFile(tempName);
    SR_OpenFile(tempName, &curFd);

    if(i == *fileCount - 1){
      //The last file may have less records and blocks
      curMetadata.blockCount = metadata.blockCount - curBlock +1;
      curMetadata.recordCount = (curMetadata.blockCount - 2) * metadata.blockCapacity + metadata.recordCount % metadata.blockCapacity;
      curMetadata.blockCapacity = metadata.blockCapacity;
    }
    else{
      curMetadata.blockCount = bufferSize + 1;
      curMetadata.recordCount = bufferSize * metadata.blockCapacity;
      curMetadata.blockCapacity = metadata.blockCapacity;
    }

    //Save new file's metadata
    CALL_OR_ERROR(BF_GetBlock(curFd, 0, blockOut));
    dataOut = BF_Block_GetData(blockOut);
    memcpy(dataOut, &curMetadata, sizeof(SR_Metadata));
    BF_Block_SetDirty(blockOut);
    CALL_OR_ERROR(BF_UnpinBlock(blockOut));

    for(int j = 0; j < curMetadata.blockCount - 1; j++){
      CALL_OR_ERROR(BF_GetBlock(fileDesc, curBlock, blockIn));
      dataIn = BF_Block_GetData(blockIn);

      CALL_OR_ERROR(BF_AllocateBlock(curFd, blockOut));
      dataOut = BF_Block_GetData(blockOut);

      memcpy(dataOut, dataIn, BF_BLOCK_SIZE);
      BF_Block_SetDirty(blockOut);

      CALL_OR_ERROR(BF_UnpinBlock(blockIn));
      CALL_OR_ERROR(BF_UnpinBlock(blockOut));

      curBlock++;
    }

    (*fd)[i] = curFd;
  }

  BF_Block_Destroy(&blockIn);
  BF_Block_Destroy(&blockOut);
  CALL_OR_ERROR(BF_CloseFile(fileDesc));
  return SR_OK;
}

int getSmallestRecord(Record *rec, int bufferSize, int fieldNo){
  int min = 1;
  for(int i = 2; i < bufferSize; i++){
    if(record_LessThanOrEqual(&(rec[i]), &(rec[min]), fieldNo)){
      min = i;
    }
  }
  return min;
}

SR_ErrorCode merge(int *fd, BF_Block **block, int bufferSize, int fieldNo){
  //In all the arrays the position with 0 index belongs to the output file
  char *data;
  int *counter;
  counter = malloc(bufferSize * sizeof(int));
  if(counter == NULL) return SR_ERROR;

  Record *rec;
  rec = malloc(bufferSize * sizeof(Record));
  if(rec == NULL) return SR_ERROR;

  SR_Metadata *metadata;
  metadata = malloc(bufferSize * sizeof(SR_Metadata));
  if(metadata == NULL) return SR_ERROR;

  //Get metadata and initialize vars
  //For the output file
  CALL_OR_ERROR(BF_GetBlock(fd[0], 0, block[0]));
  data = BF_Block_GetData(block[0]);
  memcpy(&(metadata[0]), data, sizeof(SR_Metadata));
  CALL_OR_ERROR(BF_UnpinBlock(block[0]));

  CALL_OR_ERROR(BF_AllocateBlock(fd[0], block[0]));
  counter[0] = 0;

  //For the input files
  for(int i = 1; i < bufferSize; i++){
    CALL_OR_ERROR(BF_GetBlock(fd[i], 0, block[i]));
    data = BF_Block_GetData(block[i]);
    memcpy(&(metadata[i]), data, sizeof(SR_Metadata));
    CALL_OR_ERROR(BF_UnpinBlock(block[i]));

    CALL_OR_ERROR(BF_GetBlock(fd[i], 1, block[i]));
    data = BF_Block_GetData(block[i]);
    memcpy(&(rec[i]), data, sizeof(Record));

    counter[i] = 0;
  }

  while(bufferSize > 1){
    int smallest = getSmallestRecord(rec, bufferSize, fieldNo);

    //Change output block if needed
    if(counter[0] != 0 && counter[0] % metadata[0].blockCapacity == 0){
      //Move to next block
      BF_Block_SetDirty(block[0]);
      CALL_OR_ERROR(BF_UnpinBlock(block[0]));
      CALL_OR_ERROR(BF_AllocateBlock(fd[0], block[0]));
    }

    //Copy record to output
    data = BF_Block_GetData(block[0]);
    data += (counter[0] % metadata[0].blockCapacity) * sizeof(Record);
    memcpy(data, &(rec[smallest]), sizeof(Record));

    //Move output's counter
    counter[0] += 1;

    //Move input's counter and change block if needed and load next record
    counter[smallest] += 1;
    if(counter[smallest] == metadata[smallest].recordCount){
      //If it's the last record
      CALL_OR_ERROR(BF_UnpinBlock(block[smallest]));
      SR_CloseFile(fd[smallest]);

      bufferSize--;
      if(smallest != bufferSize){
        counter[smallest] = counter[bufferSize];
        rec[smallest] = rec[bufferSize];
        fd[smallest] = fd[bufferSize];
        metadata[smallest] = metadata[bufferSize];

        BF_Block *temp = block[smallest];
        block[smallest] = block[bufferSize];
        block[bufferSize] = temp;
      }
    }
    else if(counter[smallest] % metadata[smallest].blockCapacity == 0){
      //Move to next block
      CALL_OR_ERROR(BF_UnpinBlock(block[smallest]));

      int nextBlock = counter[smallest] / metadata[smallest].blockCapacity + 1;

      CALL_OR_ERROR(BF_GetBlock(fd[smallest], nextBlock, block[smallest]));
      data = BF_Block_GetData(block[smallest]);
      memcpy(&(rec[smallest]), data, sizeof(Record));
    }
    else{
      data = BF_Block_GetData(block[smallest]);
      data += (counter[smallest] % metadata[smallest].blockCapacity) * sizeof(Record);
      memcpy(&(rec[smallest]), data, sizeof(Record));
    }
  }

  BF_Block_SetDirty(block[0]);
  CALL_OR_ERROR(BF_UnpinBlock(block[0]));

  //Write output's metadata
  metadata[0].recordCount = counter[0];
  metadata[0].blockCount = counter[0] / metadata[0].blockCapacity + 1;
  if(counter[0] % metadata[0].blockCapacity != 0){
    metadata[0].blockCount += 1;
  }

  CALL_OR_ERROR(BF_GetBlock(fd[0], 0, block[0]));
  data = BF_Block_GetData(block[0]);
  memcpy(data, &(metadata[0]), sizeof(SR_Metadata));
  BF_Block_SetDirty(block[0]);
  CALL_OR_ERROR(BF_UnpinBlock(block[0]));

  free(counter);
  free(rec);
  free(metadata);

  return SR_OK;
}

SR_ErrorCode copyFile(const char *srcName, const char *destName){
  int srcFd, destFd;
  int blockCount;

  char *inData, *outData;
  BF_Block *inBlock, *outBlock;
  BF_Block_Init(&inBlock);
  BF_Block_Init(&outBlock);

  SR_CreateFile(destName);
  SR_OpenFile(destName, &destFd);
  
  SR_OpenFile(srcName, &srcFd);

  CALL_OR_ERROR(BF_GetBlockCounter(srcFd, &blockCount));

  for(int i = 0; i < blockCount; i++){
    CALL_OR_ERROR(BF_GetBlock(srcFd, i, inBlock));
    if(i == 0){
      CALL_OR_ERROR(BF_GetBlock(destFd, 0, outBlock));
    }
    else{
      CALL_OR_ERROR(BF_AllocateBlock(destFd, outBlock));
    }

    inData = BF_Block_GetData(inBlock);
    outData = BF_Block_GetData(outBlock);

    memcpy(outData, inData, BF_BLOCK_SIZE);
    BF_Block_SetDirty(outBlock);
    CALL_OR_ERROR(BF_UnpinBlock(inBlock));
    CALL_OR_ERROR(BF_UnpinBlock(outBlock));
  }
  CALL_OR_ERROR(BF_CloseFile(srcFd));
  CALL_OR_ERROR(BF_CloseFile(destFd));
  return SR_OK;
}
