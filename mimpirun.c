/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>

#include "mimpi_common.h"
#include "channel.h"

#define DESC_SHIFT 30

void findNodeRelations(const int* worldRank, const int* worldSize, int* parentNode, int* leftChild, int* rightChild) {
    if (*worldRank == 0) {

        // I am the root:
        *parentNode = -1;
        *leftChild = (1 < *worldSize) ? 1 : -1;
        *rightChild = (2 < *worldSize) ? 2 : -1;
    } else {
        *parentNode = (*worldRank - 1) / 2;

        // Calculate left child and right child:
        *leftChild = 2 * *worldRank + 1;
        *rightChild = 2 * *worldRank + 2;

        // Check if left child or right child is within the valid range:
        if (*leftChild >= *worldSize) {
            *leftChild = -1;
        }
        if (*rightChild >= *worldSize) {
            *rightChild = -1;
        }
    }
}

void setWorldSize(int worldSize) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%d", worldSize);
    if (setenv("MIMPI_ENV_WORLD_SIZE", buffer, 1) != 0) {
        fprintf(stderr, "Setting env variable MIMPI_ENV_WORLD_SIZE failed\n");
        exit(EXIT_FAILURE);
    }
}

void setWorldRank(int worldRank) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%d", worldRank);
    if (setenv("MIMPI_ENV_WORLD_RANK", buffer, 1) != 0) {
        fprintf(stderr, "Setting env variable MIMPI_ENV_WORLD_RANK failed\n");
        exit(EXIT_FAILURE);
    }
}


int main(int argc, char *argv[]) {

    // Check the number of command line arguments:
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <n> <prog> [args...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int worldSize = (int)strtol(argv[1], NULL, 10);
    char *prog = argv[2];

    // Set world size:
    setWorldSize(worldSize);

    // Define pipes for communication between child processes:
    int pipes[worldSize*(worldSize-1) + 2*(worldSize-1)][2]; //MAX 15*16*2 = 480pipes => 480*2 = 960desc => 960+20 = 980 occupied desc

    // Establish pipes:
    int firstFree = DESC_SHIFT;
    for(int i = 0; i < worldSize*(worldSize-1) + 2*(worldSize-1); ++i) {

        ASSERT_SYS_OK(channel(pipes[i]));

        ASSERT_SYS_OK(dup2(pipes[i][0], firstFree));
        ASSERT_SYS_OK(dup2(pipes[i][1], firstFree + 1));

        ASSERT_SYS_OK(close(pipes[i][0]));
        ASSERT_SYS_OK(close(pipes[i][1]));

        pipes[i][0] = firstFree;
        pipes[i][1] = firstFree + 1;

        firstFree += 2;
    }

    // Run worldSize copies of the prog program:
    for(int worldRank = 0; worldRank < worldSize; ++worldRank) {

        // Set world rank:
        setWorldRank(worldRank);

        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if(!pid) {

            // Find node relations:
            int parentNode = -1;
            int leftChild = -1;
            int rightChild = -1;
            findNodeRelations(&worldRank, &worldSize, &parentNode, &leftChild, &rightChild);

            // Close unused descriptors:
            int k = 0;
            for(int j = 0; j < worldSize*(worldSize-1) + 2*(worldSize-1); ++j) {
                if(k != j && j%(worldSize-1) == 0 && k < worldSize) {
                    k++;
                }

                // Descriptors for point-to-point messages:

                if(worldRank*(worldSize-1) <= j && j < (worldRank+1)*(worldSize-1)) { //czytające końce p-p -> zamykamy końce do pisania:
                    ASSERT_SYS_OK(close(pipes[j][1]));
                } else if(j < worldRank*(worldSize-1) && j == k*(worldSize-1)+worldRank-1) { //piszące końce p-p -> zamykamy końce do czytania:
                    ASSERT_SYS_OK(close(pipes[j][0]));
                } else if((worldRank+1)*(worldSize-1) <= j && j < worldSize*(worldSize-1) && j == k*(worldSize-1)+worldRank) { //piszące końce p-p -> zamykamy końce do czytania:
                    ASSERT_SYS_OK(close(pipes[j][0]));

                    // Descriptors for group messages:

                } else if(j == worldSize*(worldSize-1) + 2*(worldRank-1) && parentNode != -1) { //czytający koniec group (lewy pipe do parentNode) -> zamykamy koniec do pisania:
                    ASSERT_SYS_OK(close(pipes[j][1]));
                } else if(j == worldSize*(worldSize-1) + 2*(worldRank-1) + 1 && parentNode != -1) { //piszący koniec group (prawy pipe do parentNode) -> zamykamy koniec do czytania:
                    ASSERT_SYS_OK(close(pipes[j][0]));
                } else if(j == worldSize*(worldSize-1) + 4*worldRank && leftChild != -1) { //piszący koniec group (lewy pipe do leftChild) -> zamykamy koniec do czytania:
                    ASSERT_SYS_OK(close(pipes[j][0]));
                } else if(j == worldSize*(worldSize-1) + 4*worldRank + 1 && leftChild != -1) { //czytający koniec group (prawy pipe do leftChild) -> zamykamy koniec do pisania:
                    ASSERT_SYS_OK(close(pipes[j][1]));
                } else if(j == worldSize*(worldSize-1) + 4*worldRank+2 && rightChild != -1) { //piszący koniec group (lewy pipe do rightChild) -> zamykamy koniec do czytania:
                    ASSERT_SYS_OK(close(pipes[j][0]));
                } else if(j == worldSize*(worldSize-1) + 4*worldRank+2 + 1 && rightChild != -1) { //czytający koniec group (prawy pipe do rightChild) -> zamykamy koniec do pisania:
                    ASSERT_SYS_OK(close(pipes[j][1]));

                    // Other descriptors:

                } else {
                    ASSERT_SYS_OK(close(pipes[j][1]));
                    ASSERT_SYS_OK(close(pipes[j][0]));
                }
            }

            // Construct the argument list for execvp:
            char *args[argc - 1];
            args[0] = prog;
            for(int j = 1; j < argc - 1; ++j) {
                args[j] = argv[j + 2];
            }

            ASSERT_SYS_OK(execvp(prog, args));
        }
    }

    // Close unused descriptors:
    for(int i = 0; i < worldSize*(worldSize-1) + 2*(worldSize-1); ++i){
        ASSERT_SYS_OK(close(pipes[i][0]));
        ASSERT_SYS_OK(close(pipes[i][1]));
    }

    // Wait for all created processes to finish:
    for(int i = 0; i < worldSize; ++i) {
        ASSERT_SYS_OK(wait(NULL));
    }

    // Unset environment variables:
    if (unsetenv("MIMPI_ENV_WORLD_SIZE") != 0) {
        fprintf(stderr, "Unsetting env variable MIMPI_ENV_WORLD_SIZE failed\n");
        exit(EXIT_FAILURE);
    }
    if (unsetenv("MIMPI_ENV_WORLD_RANK") != 0) {
        fprintf(stderr, "Unsetting env variable MIMPI_ENV_WORLD_RANK failed\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}