#include "sort_file.h"
#include "bf.h"
#include "sort_file_helping.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CALL_OR_ERROR(call) \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK){       \
    BF_PrintError(code);    \
    return SR_ERROR;        \
  }                         \
}

SR_ErrorCode SR_Init() {
  return SR_OK;
}

SR_ErrorCode SR_CreateFile(const char *fileName) {
  int fileDesc;
  SR_Metadata metadata;
  char *data;
  BF_Block *block;
  BF_Block_Init(&block);

  //Create file
  CALL_OR_ERROR(BF_CreateFile(fileName));

  //Open file
  CALL_OR_ERROR(BF_OpenFile(fileName, &fileDesc));

  //Allocate and write metadata in the first block
  CALL_OR_ERROR(BF_AllocateBlock(fileDesc, block));

  metadata.SR_Identifier = SR_IDENTIFIER;
  metadata.recordCount = 0;
  metadata.blockCount = 1;
  metadata.blockCapacity = BF_BLOCK_SIZE / sizeof(Record);

  data = BF_Block_GetData(block);
  memcpy(data, &metadata, sizeof(SR_Metadata));

  BF_Block_SetDirty(block);
  CALL_OR_ERROR(BF_UnpinBlock(block));

  //Close file
  CALL_OR_ERROR(BF_CloseFile(fileDesc));

  BF_Block_Destroy(&block);
  return SR_OK;
}

SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {
  char *data;
  SR_Metadata metadata;
  BF_Block *block;
  BF_Block_Init(&block);

  //Open file
  CALL_OR_ERROR(BF_OpenFile(fileName, fileDesc));

  //Check if it is a SR file
  CALL_OR_ERROR(BF_GetBlock(*fileDesc, 0, block));
  data = BF_Block_GetData(block);
  memcpy(&metadata, data, sizeof(SR_Metadata));

  if(metadata.SR_Identifier != SR_IDENTIFIER){
    return SR_ERROR;
  }

  CALL_OR_ERROR(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return SR_OK;
}

SR_ErrorCode SR_CloseFile(int fileDesc) {
  CALL_OR_ERROR(BF_CloseFile(fileDesc));
  return SR_OK;
}

SR_ErrorCode SR_InsertEntry(int fileDesc,	Record record) {
  SR_Metadata metadata;
  char *data;

  BF_Block *metaBlock, *dataBlock;
  BF_Block_Init(&metaBlock);
  BF_Block_Init(&dataBlock);

  //Load metadata
  CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, metaBlock));
  data = BF_Block_GetData(metaBlock);
  memcpy(&metadata, data, sizeof(SR_Metadata));

  //Check if all blocks are full
  if(metadata.recordCount % metadata.blockCapacity == 0){
    //If they are allocate a new block
    CALL_OR_ERROR(BF_AllocateBlock(fileDesc, dataBlock));
    metadata.blockCount += 1;
  }
  else{
    //If there is free space, load the last block
    CALL_OR_ERROR(BF_GetBlock(fileDesc, metadata.blockCount - 1, dataBlock));
  }

  //Insert the record
  data = BF_Block_GetData(dataBlock);
  data += (metadata.recordCount % metadata.blockCapacity) * sizeof(Record);
  memcpy(data, &record, sizeof(Record));
  metadata.recordCount += 1;
  BF_Block_SetDirty(dataBlock);
  CALL_OR_ERROR(BF_UnpinBlock(dataBlock));

  //Update metadata
  data = BF_Block_GetData(metaBlock);
  memcpy(data, &metadata, sizeof(SR_Metadata));
  BF_Block_SetDirty(metaBlock);
  CALL_OR_ERROR(BF_UnpinBlock(metaBlock));

  BF_Block_Destroy(&metaBlock);
  BF_Block_Destroy(&dataBlock);
  return SR_OK;
}

SR_ErrorCode SR_SortedFile(const char* input_filename, const char* output_filename, int fieldNo, int bufferSize) {
  int fd_input, fd_output, sr_error, chunksNo;
  SR_Metadata metadata;
  char *data;
  char tempName[50];

  int fileCount;
  int *fd;

  if(bufferSize < 3 || bufferSize > BF_BUFFER_SIZE) {
    printf("Wrong bufferSize\n");
    return SR_ERROR;
  }
  if(fieldNo < 0 || fieldNo > 3) {
    printf("Wrong field number\n");
    return SR_ERROR;
  }
  //Split the original file into smaller files
  splitFile(input_filename, bufferSize, &fileCount, &fd);

  //Initialize the available blocks
  BF_Block **block;
  block = malloc(bufferSize * sizeof(BF_Block*));
  if(block == NULL) return SR_ERROR;
  for(int i = 0; i < bufferSize; i++){
    BF_Block_Init(&block[i]);
  }

  //Sort smaller files with quicksort
  for(int i = 0; i < fileCount; i++){
     sort(block, fd[i], bufferSize, fieldNo);
  }

  //Initialize helping vars for merge sort
  int *mergeFd = malloc(bufferSize * sizeof(int));
  if(mergeFd == NULL) return SR_ERROR;

  //Merge Sort
  for(int iter = 0; fileCount > 1; iter++){
    int nextFileIndex = 0; //Index of the next file to be sorted
    int mergeIndex = 1; //Index of file in the mergeFd array
    int newFilesCount = 0; //Number of files created in the current iteration
    int outFd;

    while(nextFileIndex < fileCount){
      mergeFd[mergeIndex] = fd[nextFileIndex];

      mergeIndex++;
      nextFileIndex++;

      //If ready merge the files
      if(mergeIndex == bufferSize || nextFileIndex == fileCount){
        //Create output file
        sprintf(tempName, "./temp/temp-%d-%d.db", iter+1, newFilesCount);
        SR_CreateFile(tempName);
        SR_OpenFile(tempName, &outFd);
        mergeFd[0] = outFd;

        //Merge files
        if(merge(mergeFd, block, mergeIndex, fieldNo) != SR_OK){
          printf("Error while merging(iter = %d, nextFileIndex = %d)\n", iter, nextFileIndex);
          return SR_ERROR;
        }

        //Delete input files
        for(int i = nextFileIndex - mergeIndex + 1; i < nextFileIndex; i++){
          sprintf(tempName, "./temp/temp-%d-%d.db", iter, i);
          remove(tempName);
        }

        //Add output file
        fd[newFilesCount] = outFd;
        newFilesCount++;

        //Reset index
        mergeIndex = 1;
      }
    }

    fileCount = newFilesCount;

    if(fileCount == 1){
      //Save final file's name
      sprintf(tempName, "./temp/temp-%d-0.db", iter+1);
      SR_CloseFile(fd[0]);
    }
  }

  //Free all the initialized blocks and allocated memory
  for(int i = 0; i < bufferSize; i++){
    BF_Block_Destroy(&block[i]);
  }
  free(block);
  free(fd);

  //Move output
  copyFile(tempName, output_filename);
  remove(tempName);

  //Delete temp directory
  if(rmdir("./temp") != 0) return SR_ERROR;

  return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  SR_Metadata metadata;
  Record rec;
  char *data;
  int i;

  BF_Block *block;
  BF_Block_Init(&block);

  //Load metadata
  CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, block));
  data = BF_Block_GetData(block);
  memcpy(&metadata, data, sizeof(SR_Metadata));
  CALL_OR_ERROR(BF_UnpinBlock(block));

  //Print all records
  for(i = 0; i < metadata.recordCount; i++){
    if(i % metadata.blockCapacity == 0){
      //Load next data block
      CALL_OR_ERROR(BF_GetBlock(fileDesc, i/metadata.blockCapacity + 1, block));
      data = BF_Block_GetData(block);
    }
    else{
      //Move data pointer to the next record
      data += sizeof(Record);
    }

    //Load and print record
    memcpy(&rec, data, sizeof(Record));
    printf("Id: %-5d Name: %-15s Surname: %-20s City: %-20s\n", rec.id, rec.name, rec.surname, rec.city);


    //Unpin block if necessary
    if((i+1) % metadata.blockCapacity == 0 || (i+1) == metadata.recordCount){
      CALL_OR_ERROR(BF_UnpinBlock(block));
    }
  }

  BF_Block_Destroy(&block);
  return SR_OK;
}
