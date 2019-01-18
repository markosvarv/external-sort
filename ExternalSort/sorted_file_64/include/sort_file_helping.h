//reads the records from file descriptor fd in an array of blocks and sorts them
SR_ErrorCode sort (BF_Block **array, int fd, int bufferSize, int fieldNo);

//sort the records from p to r in the array
void quicksort (BF_Block **array, int p, int r, int fieldNo, int blockCapacity);

int partition (BF_Block **array, int p, int r, int fieldNo, int blockCapacity);

//get the record of position i
SR_ErrorCode getRecord (BF_Block **array, int i, Record *record, int blockCapacity);

//return 1 if record1 <= record2, and 0 if record1 > record2. Comparison depends on fieldNo
int record_LessThanOrEqual (Record *record1, Record *record2, int fieldNo);

//swap elements i and j in the array
SR_ErrorCode swapElements (BF_Block **array, int i, int j, int blockCapacity);

//get metadata of an opened file
SR_ErrorCode getMetadata(int fileDesc, SR_Metadata *metadata);

//split the file fileName in fileCount smaller files
SR_ErrorCode splitFile(const char *fileName, int bufferSize, int *fileCount, int **fd);

//merge the smaller files in one sorted file
SR_ErrorCode merge(int *fd, BF_Block **block, int bufferSize, int fieldNo);

//copy the file of srcName in a new file with name destName
SR_ErrorCode copyFile(const char *srcName, const char *destName);
