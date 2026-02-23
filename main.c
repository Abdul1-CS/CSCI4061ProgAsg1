#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

#include "./include/table.h"

int main(int argc, char *argv[])
{
    /*
    argv[0] = name of program
    argv[1] = directory
    argv[2] = # of mappers
    argv[3] = # of reducers
    = 4 arguements total
    */

    // CASE 1: Incorrect amount of args
    if (argc < 4)
    {
        fprintf(stderr, "Usage: <directory> <n mappers> <n reducers>\n");
        exit(1);
    }

    // CASE 2: Invalid number of mappers or reducers
    if (atoi(argv[2]) < 1 || atoi(argv[3]) < 1)
    {
        fprintf(stderr, "MapReduce: Cannot have less than one mapper or reducer\n");
        exit(1);
    }

    // perror if directory cannot be opened
    DIR *dir = opendir(argv[1]);
    if (dir == NULL)
    {
        perror("Failed to open directory");
        exit(1);
    }
    closedir(dir);

    // open with opendir, read with readdir, to get all files in the requested dir
    // skip . and .. files
    // count all files and distribute them amongs the mappers

    DIR *dir2 = opendir(argv[1]);
    struct dirent *entry;
    int fileCount = 0;

    while ((entry = readdir(dir2)) != NULL)
    {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
            fileCount++;
    }
    closedir(dir2);
    dir2 = opendir(argv[1]);
    char **fileNames = malloc(fileCount * sizeof(char *));

    int Nmappers = atoi(argv[2]);
    int groupSize = fileCount / Nmappers;
    int remainder = fileCount % Nmappers;
    int fileID = 0;

    // assign files to mappers
    for (int i = 0; i < Nmappers; i++)
    {
        int currentGroupSize = groupSize + (i < remainder ? 1 : 0);
        int assigned = 0;

        while (assigned < currentGroupSize)
        {
            entry = readdir(dir2);
            if (entry == NULL)
            {
                break;
            }

            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            {
                continue;
            }

            size_t need = strlen(argv[1]) + 1 + strlen(entry->d_name) + 1; //+ 2 for '/' and '\0'
            fileNames[fileID] = malloc(need);
            if (!fileNames[fileID])
            {
                perror("malloc");
                exit(1);
            }

            snprintf(fileNames[fileID], need, "%s/%s", argv[1], entry->d_name);

            fileID++;
            assigned++;
        }
    }

    // 1) Fork all mappers
    for (int i = 0; i < Nmappers; i++)
    {
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("fork");
            exit(1);
        }

        if (pid == 0)
        {
            char mapperID[16];
            snprintf(mapperID, sizeof(mapperID), "%d", i);
            execl("./map", "map", fileNames[i], mapperID, NULL);
            perror("exec");
            _exit(1); // IMPORTANT: don't fall through into parent code
        }
    }

    // 2) After all forks, wait for all mappers
    int any_failed = 0;
    for (int k = 0; k < Nmappers; k++)
    {
        int status;
        pid_t w = wait(&status);
        if (w < 0)
        {
            perror("wait");
            any_failed = 1;
            continue;
        }

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        {
            any_failed = 1;
        }
    }

    if (any_failed)
    {
        fprintf(stderr, "Map process failed\n");
        return 1;
    }

    /*
    Dispatch the requested number of reducer processes to run the ./reduce executable, and wait for
them to complete after forking them all.
Each child should be given an even portion of the key space to examine, unless it is not evenly
divisible, in which case one process will need to take the remainder
They should all be instructed to read from the ./intermediate directory
The files should be written in the same style as above, but to the ./out directory
The same rule with the child exit status applies as wel
    */


    // Not so sure about this part (Abdul)

    for(int i = 0; i < atoi(argv[3]); i++){
        pid_t pid = fork();
        if (pid < 0){
            perror("fork");
            exit(1);
        }
        //instruct child to read from intermediate and write to out, with reducerID as arg
        //dispatch child to run ./reduce exec
        if (pid == 0){
            char reducerID[16];
            snprintf(reducerID,sizeof(reducerID), "%d", i);
            execl("./reduce", "reduce", reducerID, NULL);
            perror("exec");                  
            _exit(1); // WONT FALL THROUGH TO PARENT CODE
        }

    }

    return 0;
}
