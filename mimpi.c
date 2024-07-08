/**
 * This file is for implementation of MIMPI library.
 * */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <pthread.h>
#include <semaphore.h>

#define DESC_SHIFT 30
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))

// Structures:
struct MessageParameters {
    int count;
    int tag;
};

struct WaitingMessageParameters {
    struct MessageParameters parameters;
    char* data;
    struct WaitingMessageParameters* next;
};

struct SentMessageParameters {
    struct MessageParameters parameters;
    struct SentMessageParameters* next;
};

// Global pointer to an array of sent messages:
struct SentMessageParameters** sentMessages = NULL;

// Global pointer to an array of waiting messages:
struct WaitingMessageParameters** waitingMessages = NULL;

// Global pointer to an array of current deadlock:
struct MessageParameters* currentDeadlock = NULL;

// Global pointer to an array of current receiver:
struct MessageParameters* currentReceiver = NULL;

// Global pointer to an array of semaphores:
sem_t* arrayOfSemaphores = NULL;

// Global pointer to receiver semaphore:
sem_t receiverSemaphore;

// Global pointer to an array of threads:
pthread_t *threads = NULL;

// Global pointer to an array of final flags:
int *finalFlags = NULL;

// Global pointers to arrays of descriptors:
int *mReadDesc = NULL;
int *mWriteDesc = NULL;
int *gReadDesc = NULL;
int *gWriteDesc = NULL;

// Global variables with initializations:
int worldSize = 0;
int worldRank = 0;
int parentNode = -1;
int leftChild = -1;
int rightChild = -1;
int deadlockDetection = 0;


void findNodeRelations() {
    if (worldRank == 0) {

        // I am the root:
        parentNode = -1;
        leftChild = (1 < worldSize) ? 1 : -1;
        rightChild = (2 < worldSize) ? 2 : -1;
    } else {
        parentNode = (worldRank - 1) / 2;

        // Calculate left child and right child:
        leftChild = 2 * worldRank + 1;
        rightChild = 2 * worldRank + 2;

        // Check if left child or right child is within the valid range:
        if (leftChild >= worldSize) {
            leftChild = -1;
        }
        if (rightChild >= worldSize) {
            rightChild = -1;
        }
    }
}

int findMatchingPair(int index) {

    struct SentMessageParameters* current = sentMessages[index];
    struct SentMessageParameters* previous = NULL;

    while (current != NULL) {

        // If we get a match:
        if (current->parameters.count == currentDeadlock[index].count && (current->parameters.tag == currentDeadlock[index].tag || currentDeadlock[index].tag == MIMPI_ANY_TAG)) {

            if (previous == NULL) {
                // If the node to be deleted is the head:
                sentMessages[index] = current->next;
            } else {
                // If the node to be deleted is not the head:
                previous->next = current->next;
            }
            free(current);

            currentDeadlock[index].count = -1;
            currentDeadlock[index].tag = -1;
            return 1;
        }
        previous = current;
        current = current->next;
    }
    return 0;
}

void addToSentHistory(int index, int count, int tag) {
    struct SentMessageParameters* newSentMessage = (struct SentMessageParameters*)malloc(sizeof(struct SentMessageParameters));
    if (newSentMessage == NULL) {
        perror("Memory allocation error in newSentMessage");
        exit(EXIT_FAILURE);
    }

    // Set values:
    newSentMessage->parameters.count = count;
    newSentMessage->parameters.tag = tag;
    newSentMessage->next = NULL;

    // If the list is empty, set the new node as the head:
    if (sentMessages[index] == NULL) {
        sentMessages[index] = newSentMessage;
    } else {

        // Traverse to the end of the list and append the new node:
        struct SentMessageParameters* current = sentMessages[index];
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = newSentMessage;
    }
}

struct WaitingMessageParameters* createWaitingMessage(char* data, int count, int tag) {
    struct WaitingMessageParameters* newWaitingMessage = (struct WaitingMessageParameters*)malloc(sizeof(struct WaitingMessageParameters));
    if (newWaitingMessage == NULL) {
        perror("Memory allocation error in newNode");
        exit(EXIT_FAILURE);
    }

    // Set values:
    newWaitingMessage->data = data;
    newWaitingMessage->parameters.count = count;
    newWaitingMessage->parameters.tag = tag;
    newWaitingMessage->next = NULL;

    return newWaitingMessage;
}

void addToWaitingMessages(int index, struct WaitingMessageParameters* newWaitingMessage) {

    // If the list is empty, set the new node as the head:
    if (waitingMessages[index] == NULL) {
        waitingMessages[index] = newWaitingMessage;
    } else {

        // Traverse to the end of the list and append the new node:
        struct WaitingMessageParameters* current = waitingMessages[index];
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = newWaitingMessage;
    }
}

void* messThreadFunction(void* arg) {
    int t = *(int*)arg;
    free(arg);
    int readDesc = mReadDesc[t];

    int count;
    int tag;
    int digit;
    int firstSig;

    int passedInfo;

    char smallBuffer[512] = {0};

    // Get messages until you are told not to do that anymore:
    while(true) {

        // First message, with parameters:

        passedInfo = 0;
        while (passedInfo != 512) {
            passedInfo = (int)chrecv(readDesc, smallBuffer, 512);
            if (passedInfo == -1 || passedInfo == 0) {
                return NULL;
            }
        }

        // If that is a point-to-point message:
        if (smallBuffer[511] == 'm') {

            // First message, with parameters:

            count = 0;
            tag = 0;

            // Decode tag:
            digit = 1;
            firstSig = 510;
            for(int i = 501; i <= 510; ++i) {
                if(smallBuffer[i] != (char)0) {
                    firstSig = i;
                    break;
                }
            }
            for(int i = 510; i >= firstSig; --i) {
                tag += ((int)smallBuffer[i])*digit;
                digit *= 10;
            }

            // Decode count:
            digit = 1;
            firstSig = 500;
            for(int i = 491; i <= 500; ++i) {
                if(smallBuffer[i] != (char)0) {
                    firstSig = i;
                    break;
                }
            }
            for(int i = 500; i >= firstSig; --i) {
                count += ((int)smallBuffer[i])*digit;
                digit *= 10;
            }

            // Get the rest of first message:
            char *bigBuffer = (char *)malloc(count * sizeof(char));
            if (bigBuffer == NULL) {
                perror("Memory allocation error in bigBuffer");
                exit(EXIT_FAILURE);
            }

            int remaining = count;
            int offset = 0;
            int chunkSize = (remaining > 491) ? 491 : remaining;

            // Get content from first message:
            memcpy(bigBuffer, smallBuffer, chunkSize);

            offset += chunkSize;
            remaining -= chunkSize;

            // Other messages, without parameters:

            while(remaining > 0) {

                chunkSize = (remaining > 512) ? 512 : remaining;

                // Get message:
                passedInfo = 0;
                while(passedInfo != 512) {
                    passedInfo = (int)chrecv(readDesc, smallBuffer, 512);
                    if (passedInfo == -1 || passedInfo == 0) {
                        return NULL;
                    }
                }

                // Get content:
                memcpy(bigBuffer + offset, smallBuffer, chunkSize);

                offset += chunkSize;
                remaining -= chunkSize;
            }

            // Create a node for new message to put it in waiting messages:
            struct WaitingMessageParameters* newWaitingMessage = createWaitingMessage(bigBuffer, count, tag);

            sem_wait(&arrayOfSemaphores[t]);

            // Put new message in waiting messages:
            addToWaitingMessages(t, newWaitingMessage);

            // Check if the main thread (receiver) waits for that message from that process:
            if(currentReceiver[t].count == count && (currentReceiver[t].tag == tag || currentReceiver[t].tag == MIMPI_ANY_TAG)) {
                sem_post(&receiverSemaphore);
            } else {
                sem_post(&arrayOfSemaphores[t]);
            }

        // If that is a final message:
        } else if(smallBuffer[511] == 'f') {

            // Changes to apply in send logic:
            ASSERT_SYS_OK(close(mReadDesc[t]));
            mReadDesc[t] = -1;
            ASSERT_SYS_OK(close(mWriteDesc[t]));
            mWriteDesc[t] = -1;

            sem_wait(&arrayOfSemaphores[t]);

            // Changes to apply in receiver logic:
            finalFlags[t] = 1;
            if(currentReceiver[t].count != -1 && currentReceiver[t].tag != -1) {
                sem_post(&receiverSemaphore);
            } else {
                sem_post(&arrayOfSemaphores[t]);
            }

            // There won't be any new messages from that process:
            return NULL;

        // If that is a deadlock message:
        } else if(deadlockDetection == 1) {
            //mamy komunikat deadlock - trzeba rozszyfrować count i tag, dodać i ew. przyciąć:

            count = 0;
            tag = 0;

            // Decode tag:
            digit = 1;
            firstSig = 510;
            for(int i = 501; i <= 510; ++i) {
                if(smallBuffer[i] != (char)0) {
                    firstSig = i;
                    break;
                }
            }
            for(int i = 510; i >= firstSig; --i) {
                tag += ((int)smallBuffer[i])*digit;
                digit *= 10;
            }

            // Decode count:
            digit = 1;
            firstSig = 500;
            for(int i = 491; i <= 500; ++i) {
                if(smallBuffer[i] != (char)0) {
                    firstSig = i;
                    break;
                }
            }
            for(int i = 500; i >= firstSig; --i) {
                count += ((int)smallBuffer[i])*digit;
                digit *= 10;
            }

            sem_wait(&arrayOfSemaphores[t]);

            // Update deadlock parameters:
            currentDeadlock[t].count = count;
            currentDeadlock[t].tag = tag;

            //Chcek if that deadlock message can be ignored and if not - check if the main thread (receiver) waits for a message from that process:
            if(findMatchingPair(t) == 0 && (currentReceiver[t].count != -1 && currentReceiver[t].tag != -1)) {
                sem_post(&receiverSemaphore);
            } else {
                sem_post(&arrayOfSemaphores[t]);
            }
        }

    }

}

void MIMPI_Init(bool enable_deadlock_detection) {

    channels_init();

    // Envirinment variable - worldSize:
    char *envWorldSize = getenv("MIMPI_ENV_WORLD_SIZE");
    if (envWorldSize == NULL) {
        perror("Environment variable error in MIMPI_ENV_WORLD_SIZE");
        exit(EXIT_FAILURE);
    }
    worldSize = (int)strtol(envWorldSize, NULL, 10);
    if (unsetenv("MIMPI_ENV_WORLD_SIZE") != 0) {
        fprintf(stderr, "Unsetting env variable MIMPI_ENV_WORLD_SIZE failed\n");
        exit(EXIT_FAILURE);
    }

    // Envirinment variable - worldRank:
    char *envWorldRank = getenv("MIMPI_ENV_WORLD_RANK");
    if (envWorldRank == NULL) {
        perror("Environment variable error in MIMPI_ENV_WORLD_RANK");
        exit(EXIT_FAILURE);
    }
    worldRank = (int)strtol(envWorldRank, NULL, 10);
    if (unsetenv("MIMPI_ENV_WORLD_RANK") != 0) {
        fprintf(stderr, "Unsetting env variable MIMPI_ENV_WORLD_RANK failed\n");
        exit(EXIT_FAILURE);
    }

    // Find node relations:
    findNodeRelations();

    // Waiting messages structure:
    waitingMessages = (struct WaitingMessageParameters**)malloc(worldSize * sizeof(struct WaitingMessageParameters*));
    if (waitingMessages == NULL) {
        perror("Memory allocation error in waitingMessages");
        exit(EXIT_FAILURE);
    }

    // Current receiver structure:
    currentReceiver = (struct MessageParameters *) malloc(worldSize * sizeof(struct MessageParameters));
    if (currentReceiver == NULL) {
        perror("Memory allocation error in currentReceiver");
        exit(EXIT_FAILURE);
    }

    // Final flags structure:
    finalFlags = (int *)malloc(worldSize * sizeof(int));
    if (finalFlags == NULL) {
        perror("Memory allocation error in finalFlags");
        exit(EXIT_FAILURE);
    }

    // Array of semaphores structure:
    arrayOfSemaphores = (sem_t*)malloc(worldSize * sizeof(sem_t));
    if (arrayOfSemaphores == NULL) {
        perror("Memory allocation error in arrayOfSemaphores");
        exit(EXIT_FAILURE);
    }

    // Receiver semaphore:
    ASSERT_SYS_OK(sem_init(&receiverSemaphore, 0, 0));   // Initial value 0

    // Initializations:
    for(int i = 0; i < worldSize; ++i) {
        waitingMessages[i] = NULL;

        currentReceiver[i].count = -1;
        currentReceiver[i].tag = -1;

        finalFlags[i] = 0;

        ASSERT_SYS_OK(sem_init(&arrayOfSemaphores[i], 0, 1));  // Initial value 1 (mutex)
    }

    // Deadlock detection:
    if (enable_deadlock_detection == true) {

        // Deadlock detection flag:
        deadlockDetection = 1;

        // Sent messages structure:
        sentMessages = (struct SentMessageParameters**) malloc(worldSize * sizeof(struct SentMessageParameters*));
        if (sentMessages == NULL) {
            perror("Memory allocation error in sentMessages");
            exit(EXIT_FAILURE);
        }

        // Current deadlock structure:
        currentDeadlock = (struct MessageParameters *) malloc(worldSize * sizeof(struct MessageParameters));
        if (currentDeadlock == NULL) {
            perror("Memory allocation error in currentDeadlock");
            exit(EXIT_FAILURE);
        }

        // Initializations:
        for(int i = 0; i < worldSize; ++i) {
            sentMessages[i] = NULL;

            currentDeadlock[i].count = -1;
            currentDeadlock[i].tag = -1;
        }

    }

    // Point-to-point message descriptors structure:
    mReadDesc = (int *)malloc(worldSize * sizeof(int));
    if (mReadDesc == NULL) {
        perror("Memory allocation error in mReadDesc");
        exit(EXIT_FAILURE);
    }
    mWriteDesc = (int *)malloc(worldSize * sizeof(int));
    if (mWriteDesc == NULL) {
        perror("Memory allocation error in mWriteDesc");
        exit(EXIT_FAILURE);
    }

    // Group message descriptors structure:
    gReadDesc = (int *)malloc(3 * sizeof(int)); //(0 = góra , 1 = dół/lewo , 2 = dół/prawo)
    if (gReadDesc == NULL) {
        perror("Memory allocation error in gReadDesc");
        exit(EXIT_FAILURE);
    }
    gWriteDesc = (int *)malloc(3 * sizeof(int)); //(0 = góra , 1 = dół/lewo , 2 = dół/prawo)
    if (gWriteDesc == NULL) {
        perror("Memory allocation error in gWriteDesc");
        exit(EXIT_FAILURE);
    }

    // Places for group descriptors that are not used:
    if(parentNode == -1) {
        gReadDesc[0] = -1;
        gWriteDesc[0] = -1;
    }

    if(leftChild == -1) {
        gReadDesc[1] = -1;
        gWriteDesc[1] = -1;
    }

    if(rightChild == -1) {
        gReadDesc[2] = -1;
        gWriteDesc[2] = -1;
    }

    int mReadDescInd = 0;
    int mWriteDescInd = 0;
    int k = 0;
    for(int j = 0; j < worldSize*(worldSize-1) + 2*(worldSize-1); ++j) {
        if(j != k && j%(worldSize-1) == 0 && k < worldSize) {
            k++;
        }

        // Places for point-to-point descriptors that are not used:

        if(mReadDescInd == worldRank) {
            mReadDesc[mReadDescInd] = -1;
            mReadDescInd++;
        }
        if(mWriteDescInd == worldRank) {
            mWriteDesc[mWriteDescInd] = -1;
            mWriteDescInd++;
        }

            // Descriptors for point-to-point messages:

        if(worldRank*(worldSize-1) <= j && j < (worldRank+1)*(worldSize-1)) {
            mReadDesc[mReadDescInd] = DESC_SHIFT + j*2 ;
            mReadDescInd++;
        } else if(j < worldRank*(worldSize-1) && j == k*(worldSize-1)+worldRank-1) {
            mWriteDesc[mWriteDescInd] = DESC_SHIFT + j*2 + 1;
            mWriteDescInd++;
        } else if((worldRank+1)*(worldSize-1) <= j && j < worldSize*(worldSize-1) && j == k*(worldSize-1)+worldRank) {
            mWriteDesc[mWriteDescInd] = DESC_SHIFT + j*2 + 1;
            mWriteDescInd++;

            // Descriptors for group messages:

        } else if (j == worldSize*(worldSize-1) + 2*(worldRank-1) && parentNode != -1) {
            gReadDesc[0] = DESC_SHIFT + j*2;
        } else if (j == worldSize*(worldSize-1) + 2*(worldRank-1) + 1 && parentNode != -1) {
            gWriteDesc[0] = DESC_SHIFT + j*2 + 1;
        } else if (j == worldSize*(worldSize-1) + 4*worldRank && leftChild != -1) {
            gWriteDesc[1] = DESC_SHIFT + j*2 + 1;
        } else if (j == worldSize*(worldSize-1) + 4*worldRank + 1 && leftChild != -1) {
            gReadDesc[1] = DESC_SHIFT + j*2;
        } else if (j == worldSize*(worldSize-1) + 4*worldRank+2 && rightChild != -1) {
            gWriteDesc[2] = DESC_SHIFT + j*2 + 1;
        } else if (j == worldSize*(worldSize-1) + 4*worldRank+2 + 1 && rightChild != -1) {
            gReadDesc[2] = DESC_SHIFT + j*2;
        }
    }

    // Threads:
    threads = (pthread_t *)malloc((worldSize-1) * sizeof(pthread_t));
    if (threads == NULL) {
        perror("Memory allocation error in threads");
        exit(EXIT_FAILURE);
    }

    for(int t = 0; t < worldSize-1; ++t) {
        int* worker_arg = malloc(sizeof(int));
        if (worker_arg == NULL) {
            perror("Memory allocation error in worker_arg");
            exit(EXIT_FAILURE);
        }
        *worker_arg = t < worldRank ? t : t+1;
        ASSERT_ZERO(pthread_create(&threads[t], NULL, messThreadFunction, worker_arg));

    }
}

void MIMPI_Finalize() {

    // Message for every other process that we execute finalize:
    int passedInfo;
    char messBuffer[512] = {0};

    for(int i = 0; i< worldSize; ++i) {
        if(i == worldRank) {
            continue;
        }

        // Send message:
        messBuffer[511] = 'f';
        passedInfo = 0;
        while (passedInfo != 512) {
            passedInfo = (int)chsend(mWriteDesc[i], messBuffer, 512);
            if(passedInfo == 0 || passedInfo == -1) {
                break;
            }
        }
    }

    char result = 'd';
    while(result != 'f') {
        result = 'f';

        // Left child:
        if(leftChild != -1) {
            // Get message:
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[1], messBuffer, 512);
                if(passedInfo == 0 || passedInfo == -1) {
                    messBuffer[511] = 'f';
                    break;
                }
            }
            // Update result:
            if(messBuffer[511] != 'f') {
                result = 'd';
            }
            messBuffer[511] = 0;
        }

        // Right child:
        if(rightChild != -1) {
            // Get message:
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[2], messBuffer, 512);
                if(passedInfo == 0 || passedInfo == -1) {
                    messBuffer[511] = 'f';
                    break;
                }
            }
            // Update result:
            if(messBuffer[511] != 'f') {
                result = 'd';
            }
            messBuffer[511] = 0;

        }

        // Parent:
        if(parentNode != -1) {
            // Update result:
            messBuffer[511] = result;
            // Send message:
            passedInfo = 0;
            while (passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[0], messBuffer, 512);
                if(passedInfo == 0 || passedInfo == -1) {
                    messBuffer[511] = 'f';
                    break;
                }
            }
            // Get message:
            passedInfo = 0;
            while (passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[0], messBuffer, 512);
                if(passedInfo == 0 || passedInfo == -1) {
                    messBuffer[511] = 'f';
                    break;
                }
            }
            // Update result:
            result = messBuffer[511];
        }

        // Left child:
        if(leftChild != -1) {
            //Update result:
            messBuffer[511] = result;
            //Send message:
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[1], messBuffer, 512);
                if(passedInfo == 0 || passedInfo == -1) {
                    break;
                }
            }
        }

        // Right child:
        if(rightChild != -1) {
            // Update result:
            messBuffer[511] = result;
            // Send message:
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[2], messBuffer, 512);
                if(passedInfo == 0 || passedInfo == -1) {
                    break;
                }

            }
        }

    }

    // Threads:
    for(int i = 0; i < worldSize - 1; i++) {
        ASSERT_ZERO(pthread_join(threads[i], NULL));
    }
    free(threads);


    // Descriptors:
    for(int i = 0; i < worldSize; ++i) {

        if(i < 3) {
            if(gReadDesc[i] != -1) {
                ASSERT_SYS_OK(close(gReadDesc[i]));
            }
            if(gWriteDesc[i] != -1) {
                ASSERT_SYS_OK(close(gWriteDesc[i]));
            }
        }

        if(mReadDesc[i] != -1) {
            ASSERT_SYS_OK(close(mReadDesc[i]));
        }
        if(mWriteDesc[i] != -1) {
            ASSERT_SYS_OK(close(mWriteDesc[i]));
        }

    }

    // Structures of descriptors:
    free(mReadDesc);
    free(mWriteDesc);

    free(gReadDesc);
    free(gWriteDesc);

    // Structure of current receiver:
    free(currentReceiver);

    // Structure of final flags:
    free(finalFlags);

    // Deadlock detection:
    if(deadlockDetection == 1) {

        // Contents of sent messages:
        for(int i = 0; i < worldSize; ++i) {
            struct SentMessageParameters* current = sentMessages[i];
            while (current != NULL) {
                struct SentMessageParameters* next = current->next;
                free(current);
                current = next;
            }
        }

        // Structure of sent messages:
        free(sentMessages);

        // Structure of current deadlock:
        free(currentDeadlock);
    }

    // Contents of waiting messages:
    for(int i = 0; i < worldSize; ++i) {
        struct WaitingMessageParameters* current = waitingMessages[i];
        while (current != NULL) {
            struct WaitingMessageParameters* next = current->next;
            free(current->data);
            free(current);
            current = next;
        }
    }

    // Structure of waiting messages:
    free(waitingMessages);

    // Contents of array of semaphores:
    for(int i = 0; i < worldSize; ++i) {
        sem_destroy(&arrayOfSemaphores[i]);
    }

    // Structure of array of semaphores:
    free(arrayOfSemaphores);

    // Receiver semaphore:
    sem_destroy(&receiverSemaphore);

    channels_finalize();
}

int MIMPI_World_size() {
    return worldSize;
}

int MIMPI_World_rank() {
    return worldRank;
}


MIMPI_Retcode MIMPI_Send(
        void const *data,
        int count,
        int destination,
        int tag
) {

    // Exceptions:
    if(destination == worldRank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (destination < 0 || destination >= worldSize) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    } else if (finalFlags[destination] == 1) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    int passedInfo;
    int remaining = count;
    int offset = 0;
    int chunkSize;

    // First message, with parameters:

    char smallBuffer[512] = {0};
    smallBuffer[511] = 'm';

    int partsOfCount = count;
    int partsOfTag = tag;

    // Encode tag:
    for(int i = 510; i >= 501; --i) {
        if(partsOfTag != 0) {
            smallBuffer[i] = (char)(partsOfTag%10);
            partsOfTag /= 10;
        } else {
            smallBuffer[i] = 0;
        }
    }

    // Encode count:
    for(int i = 500; i >= 491; --i) {
        if(partsOfCount != 0) {
            smallBuffer[i] = (char)(partsOfCount%10);
            partsOfCount /= 10;
        } else {
            smallBuffer[i] = 0;
        }
    }

    // Fill the rest of first message with content:
    chunkSize = (remaining > 491) ? 491 : remaining;
    memcpy(smallBuffer, data + offset, chunkSize);

    // Send first message:
    passedInfo = 0;
    while(passedInfo != 512) {
        passedInfo = (int)chsend(mWriteDesc[destination], smallBuffer, 512);
        if (passedInfo == -1 || passedInfo == 0) {
            return MIMPI_ERROR_REMOTE_FINISHED; // the remote process involved in communication has finished
        }

    }

    remaining -= chunkSize;
    offset += chunkSize;

    // Other messages, without parameters:

    while (remaining > 0) {

        // Fill message with content:
        chunkSize = (remaining > 512) ? 512 : remaining;
        memcpy(smallBuffer, data + offset, chunkSize);

        // Send message:
        passedInfo = 0;
        while(passedInfo != 512) {
            passedInfo = (int)chsend(mWriteDesc[destination], smallBuffer, 512);
            if (passedInfo == -1 || passedInfo == 0) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }

        remaining -= chunkSize;
        offset += chunkSize;

    }

    // If deadlock detection is on, we want to add that message to sent history:
    if (deadlockDetection == 1) {
        sem_wait(&arrayOfSemaphores[destination]);
        addToSentHistory(destination, count, tag);
        sem_post(&arrayOfSemaphores[destination]);
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
        void *data,
        int count,
        int source,
        int tag
) {

    // Exceptions:
    if(source == worldRank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    } else if (source < 0 || source >= worldSize) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    sem_wait(&arrayOfSemaphores[source]);

    // Send deadlock message:
    if (deadlockDetection == 1){
        char messBuffer[512] = {0};
        messBuffer[511] = 'd';

        // Encode tag:
        if(tag == MIMPI_ANY_TAG) {
            for(int i = 501; i <= 510; ++i) {
                messBuffer[i] = (char)0;
            }
        } else {
            int partsOfTag = tag;
            for(int i = 510; i >= 501; --i) {
                if(partsOfTag != 0) {
                    messBuffer[i] = (char)(partsOfTag%10);
                    partsOfTag /= 10;
                } else {
                    messBuffer[i] = (char)0;
                }
            }
        }

        // Encode count:
        int partsOfCount = count;
        for(int i = 500; i >= 491; --i) {
            if(partsOfCount != 0) {
                messBuffer[i] = (char)(partsOfCount%10);
                partsOfCount /= 10;
            } else {
                messBuffer[i] = (char)0;
            }
        }

        int passedInfo = 0;
        while (passedInfo != 512) {
            passedInfo = (int)chsend(mWriteDesc[source], messBuffer, 512);
            if (passedInfo == -1 || passedInfo == 0) {
                return MIMPI_ERROR_REMOTE_FINISHED;
            }
        }

    }

    // Find the message in waiting messages:
    struct WaitingMessageParameters* current = waitingMessages[source];
    struct WaitingMessageParameters* previous = NULL;
    while (current != NULL) {
        // If we have a match:
        if (current->parameters.count == count && (current->parameters.tag == tag || tag == MIMPI_ANY_TAG)) {

            if (previous == NULL) {
                // If the node to be deleted is the head:
                waitingMessages[source] = current->next;
            } else {
                // If the node to be deleted is not the head:
                previous->next = current->next;
            }

            memcpy(data, current->data, count);

            free(current->data);
            free(current);

            sem_post(&arrayOfSemaphores[source]);
            return MIMPI_SUCCESS;
        }
        previous = current;
        current = current->next;
    }

    // If the message is not on the list of waiting messages:

    // Final case:
    if(finalFlags[source] == 1) {
        sem_post(&arrayOfSemaphores[source]);
        return MIMPI_ERROR_REMOTE_FINISHED;
    // Deadlock case:
    } else if(deadlockDetection == 1 && (currentDeadlock[source].count != -1 || currentDeadlock[source].tag != -1)) {
        if(findMatchingPair(source) == 0) {
            currentDeadlock[source].count = -1;
            currentDeadlock[source].tag = -1;
            sem_post(&arrayOfSemaphores[source]);
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }
    }

    // Wait for the message to appear:
    currentReceiver[source].count = count;
    currentReceiver[source].tag = tag;

    sem_post(&arrayOfSemaphores[source]);
    sem_wait(&receiverSemaphore);

    currentReceiver[source].count = -1;
    currentReceiver[source].tag = -1;

    // Final case:
    if(finalFlags[source] == 1) {
        sem_post(&arrayOfSemaphores[source]);
        return MIMPI_ERROR_REMOTE_FINISHED;
    // Deadlock case:
    } else if(deadlockDetection == 1 && (currentDeadlock[source].count != -1 || currentDeadlock[source].tag != -1)) {
        currentDeadlock[source].count = -1;
        currentDeadlock[source].tag = -1;
        sem_post(&arrayOfSemaphores[source]);
        return MIMPI_ERROR_DEADLOCK_DETECTED;
    }

    // Take the message from the end of the list of waiting messages:
    if (waitingMessages[source]->next == NULL) {
        memcpy(data, waitingMessages[source]->data, count);
        free(waitingMessages[source]->data);
        free(waitingMessages[source]);
        waitingMessages[source] = NULL;
    } else {
        current = waitingMessages[source];
        while (current->next->next != NULL) {
            current = current->next;
        }
        memcpy(data, current->next->data, count);
        free(current->next->data);
        free(current->next);
        current->next = NULL;
    }

    sem_post(&arrayOfSemaphores[source]);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() { // (2log_2)

    char messBuffer[512] = {0};
    char result = 'g';

    int passedInfo;

    // Left child:
    if(leftChild != -1) {
        // Get message:
        passedInfo = 0;
        while(passedInfo != 512) {
            passedInfo = (int)chrecv(gReadDesc[1], messBuffer, 512);
        }
        // Update result:
        if(messBuffer[511] != 'g') {
            result = 'd';
        }
        messBuffer[511] = 0;
    }

    // Right child:
    if(rightChild != -1) {
        // Get message:
        passedInfo = 0;
        while(passedInfo != 512) {
            passedInfo = (int)chrecv(gReadDesc[2], messBuffer, 512);
        }
        // Update result:
        if(messBuffer[511] != 'g') {
            result = 'd';
        }
        messBuffer[511] = 0;
    }

    // Parent (I am not the root of the tree):
    if(parentNode != -1) {
        // Update result:
        messBuffer[511] = result;
        // Send message:
        passedInfo = 0;
        while (passedInfo != 512) {
            passedInfo = (int)chsend(gWriteDesc[0], messBuffer, 512);
        }
        // Get message:
        passedInfo = 0;
        while (passedInfo != 512) {
            passedInfo = (int)chrecv(gReadDesc[0], messBuffer, 512);
        }
        // Update result:
        result = messBuffer[511];
    }

    // Left child:
    if(leftChild != -1) {
        // Send message:
        messBuffer[511] = result;
        passedInfo = 0;
        while(passedInfo != 512) {
            passedInfo = (int)chsend(gWriteDesc[1], messBuffer, 512);
        }
    }

    // Right child:
    if(rightChild != -1) {
        // Send message:
        messBuffer[511] = result;
        passedInfo = 0;
        while(passedInfo != 512) {
            passedInfo = (int)chsend(gWriteDesc[2], messBuffer, 512);
        }
    }

    // Get result:
    if(result == 'g') {
        return MIMPI_SUCCESS;
    } else {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
}

MIMPI_Retcode MIMPI_Bcast( // (2log_2)
        void *data,
        int count,
        int root
) {

    // Exception:
    if(root < 0 || root >= worldSize) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    // Declarations and initializations:
    char messageBuffer[512] = {0};
    char signalMessageBuffer[512] = {0};

    char result = 'g';

    int leftIsOnPath = 0;
    int rightIsOnPath = 0;
    int passedInfo;
    int currentNode;

    int remaining = count;
    int senderMessageOffset = 0;
    int receiverMessageOffset = 0;
    int chunkSize;

    // If we need to send / get more messages:
    while(remaining > 0) {

        chunkSize = (remaining > 511) ? 511 : remaining;

        // Left child:
        if(leftChild != -1) {

            // Check if you lay on the path between the root of MIMPI_Bcast and the root of binary tree:
            if(worldRank != root) {
                currentNode = root;
                while(currentNode != 0) {
                    if(currentNode == leftChild) {
                        leftIsOnPath = 1;
                        break;
                    }
                    currentNode = (currentNode-1)/2;
                }
            }

            // Get message:
            passedInfo = 0;
            while(passedInfo != 512) {
                if(leftIsOnPath == 1) {
                    passedInfo = (int)chrecv(gReadDesc[1], messageBuffer, 512);
                } else {
                    passedInfo = (int)chrecv(gReadDesc[1], signalMessageBuffer, 512);
                }
            }

            // Update result:
            if(leftIsOnPath == 1) {
                if(messageBuffer[511] != 'g') {
                    result = 'd';
                }
                messageBuffer[511] = 0;
            } else {
                if(signalMessageBuffer[511] != 'g') {
                    result = 'd';
                }
                signalMessageBuffer[511] = 0;
            }

        }

        // Right child:
        if(rightChild != -1) {

            // Check if you lay on the path between the root of MIMPI_Bcast and the root of binary tree:
            if(worldRank != root) {
                currentNode = root;
                while(currentNode != 0) {
                    if(currentNode == rightChild) {
                        rightIsOnPath = 1;
                        break;
                    }
                    currentNode = (currentNode-1)/2;
                }
            }

            // Get message:
            passedInfo = 0;
            while(passedInfo != 512) {
                if(rightIsOnPath == 1) {
                    passedInfo = (int)chrecv(gReadDesc[2], messageBuffer, 512);
                } else {
                    passedInfo = (int)chrecv(gReadDesc[2], signalMessageBuffer, 512);
                }
            }

            // Update result:
            if(rightIsOnPath == 1) {
                if(messageBuffer[511] != 'g') {
                    result = 'd';
                }
                messageBuffer[511] = 0;
            } else {
                if(signalMessageBuffer[511] != 'g') {
                    result = 'd';
                }
                signalMessageBuffer[511] = 0;
            }
        }

        // Parent (I am not the root of the tree):
        if(parentNode != -1) {

            // I am not the root of MIMPI_Bcast:
            if(worldRank != root) {

                // Send message:
                if(leftIsOnPath == 1 || rightIsOnPath == 1) {
                    messageBuffer[511] = result;
                    passedInfo = 0;
                    while (passedInfo != 512) {
                        passedInfo = (int)chsend(gWriteDesc[0], messageBuffer, 512);
                    }

                } else {
                    signalMessageBuffer[511] = result;
                    passedInfo = 0;
                    while (passedInfo != 512) {
                        passedInfo = (int)chsend(gWriteDesc[0], signalMessageBuffer, 512);
                    }
                }

            // I am the root of MIMPI_Bcast:
            } else {

                // Create message:
                messageBuffer[511] = result;
                memcpy(messageBuffer, data + senderMessageOffset, chunkSize);
                senderMessageOffset += chunkSize;

                // Send message:
                passedInfo = 0;
                while(passedInfo != 512) {
                    passedInfo = (int)chsend(gWriteDesc[0], messageBuffer, 512);
                }

            }

            // Get message:
            passedInfo = 0;
            while (passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[0], messageBuffer, 512);
            }
            result = messageBuffer[511];

        // I am the root of the tree:
        } else {
            // I am the root of MIMPI_Bcast:
            if(worldRank == root) {
                // Create message:
                messageBuffer[511] = result;
                memcpy(messageBuffer, data + senderMessageOffset, chunkSize);
                senderMessageOffset += chunkSize;
            }

        }

        // I am not the root of MIMPI_Bcast:
        if(worldRank != root) {
            // Save message:
            memcpy(data + receiverMessageOffset, messageBuffer, chunkSize);
            receiverMessageOffset += chunkSize;
        }

        // Left child:
        if(leftChild != -1) {

            // Send message:
            messageBuffer[511] = result;
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[1], messageBuffer, 512);
            }
        }

        // Right child:
        if(rightChild != -1) {

            // Send message:
            messageBuffer[511] = result;
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[2], messageBuffer, 512);
            }
        }

        remaining -= chunkSize;
    }

    // Get result:
    if(result == 'g') {
        return MIMPI_SUCCESS;
    } else {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
}

MIMPI_Retcode MIMPI_Reduce( // (2log_2)
        void const *send_data,
        void *recv_data,
        int count,
        MIMPI_Op op,
        int root
)
{

    // Exception:
    if(root < 0 || root >= worldSize) {
        return MIMPI_ERROR_NO_SUCH_RANK; /// no process with requested rank exists in the world (ROOT)
    }

    // Declarations and initializations:
    uint8_t leftMessageBuffer[512] = {0};
    uint8_t rightMessageBuffer[512] = {0};
    uint8_t upperMessageBuffer[512] = {0};

    char result = 'g';

    int passedInfo;
    int remaining = count;
    int senderMessageOffset = 0;
    int receiverMessageOffset = 0;
    int chunkSize;

    // If we need to send / get more messages:
    while(remaining > 0) {

        // Create message:
        chunkSize = (remaining > 511) ? 511 : remaining;
        memcpy(upperMessageBuffer, send_data + senderMessageOffset, chunkSize);
        senderMessageOffset += chunkSize;

        // Left child:
        if(leftChild != -1) {
            // Get message:
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[1], leftMessageBuffer, 512);
            }
            // Update result:
            if(leftMessageBuffer[511] != 'g') {
                result = 'd';
            }
            leftMessageBuffer[511] = 0;
            // Update message:
            for(int i = 0; i < chunkSize; ++i) {
                if(op == MIMPI_MAX) {
                    upperMessageBuffer[i] = MAX(upperMessageBuffer[i], leftMessageBuffer[i]);
                } else if (op == MIMPI_MIN) {
                    upperMessageBuffer[i] = MIN(upperMessageBuffer[i], leftMessageBuffer[i]);
                } else if (op == MIMPI_SUM) {
                    upperMessageBuffer[i] = upperMessageBuffer[i] + leftMessageBuffer[i];
                } else if (op == MIMPI_PROD) {
                    upperMessageBuffer[i] = upperMessageBuffer[i] * leftMessageBuffer[i];
                }
            }

        }

        // Right child:
        if(rightChild != -1) {
            // Get message:
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[2], rightMessageBuffer, 512);
            }
            // Update result:
            if(rightMessageBuffer[511] != 'g') {
                result = 'd';
            }
            rightMessageBuffer[511] = 0;
            // Update message:
            for(int i = 0; i < chunkSize; ++i) {
                if(op == MIMPI_MAX) {
                    upperMessageBuffer[i] = MAX(upperMessageBuffer[i], rightMessageBuffer[i]);
                } else if (op == MIMPI_MIN) {
                    upperMessageBuffer[i] = MIN(upperMessageBuffer[i], rightMessageBuffer[i]);
                } else if (op == MIMPI_SUM) {
                    upperMessageBuffer[i] = upperMessageBuffer[i] + rightMessageBuffer[i];
                } else if (op == MIMPI_PROD) {
                    upperMessageBuffer[i] = upperMessageBuffer[i] * rightMessageBuffer[i];
                }
            }
        }

        // Parent (I am not the root of the tree):
        if(parentNode != -1) {
            // Update result:
            upperMessageBuffer[511] = result;
            // Send message:
            passedInfo = 0;
            while (passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[0], upperMessageBuffer, 512);
            }
            // Get message:
            passedInfo = 0;
            while (passedInfo != 512) {
                passedInfo = (int)chrecv(gReadDesc[0], upperMessageBuffer, 512);
            }
            // Update result:
            result = (char)upperMessageBuffer[511];
        }

        // I am the root of MIMPI_Reduce:
        if(worldRank == root) {
            // Save message:
            memcpy(recv_data + receiverMessageOffset, upperMessageBuffer, chunkSize);
            receiverMessageOffset += chunkSize;
        }

        // Left child:
        if(leftChild != -1) {
            // Send message
            upperMessageBuffer[511] = result;
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[1], upperMessageBuffer, 512);
            }
        }

        //Right child:
        if(rightChild != -1) {
            // Send message
            upperMessageBuffer[511] = result;
            passedInfo = 0;
            while(passedInfo != 512) {
                passedInfo = (int)chsend(gWriteDesc[2], upperMessageBuffer, 512);
            }
        }

        remaining -= chunkSize;
    }

    // Get result:
    if(result == 'g') {
        return MIMPI_SUCCESS;
    } else {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
}