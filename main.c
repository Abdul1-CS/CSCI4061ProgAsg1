#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "./include/table.h"

int main(int argc, char *argv[]) {
  /*
  argv[0] = name of program
  argv[1] = directory
  argv[2] = # of mappers
  argv[3] = # of reducers
  = 4 arguements total
  */

  // CASE 1: Incorrect amount of args
  if (argc < 4) {
    fprintf(stderr, "Usage: mapreduce <directory> <n mappers> <n reducers>\n");
    exit(1);
  }

  // CASE 2: Invalid number of mappers or reducers
  if (atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
    fprintf(stderr, "mapreduce: cannot have less than one mapper or reducer\n");
    exit(1);
  }

  // perror if directory cannot be opened
  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    perror("opendir");
    exit(1);
  }
  closedir(dir);

  // open with opendir, read with readdir, to get all files in the requested dir
  // skip . and .. files
  // count all files and distribute them amongs the mappers

  DIR *dir2 = opendir(argv[1]);
  struct dirent *entry;
  int fileCount = 0;

  while ((entry = readdir(dir2)) != NULL) {
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
  for (int i = 0; i < Nmappers; i++) {
    int currentGroupSize = groupSize + (i < remainder ? 1 : 0);
    int assigned = 0;

    while (assigned < currentGroupSize) {
      entry = readdir(dir2);
      if (entry == NULL) {
        break;
      }

      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }

      size_t need = strlen(argv[1]) + 1 + strlen(entry->d_name) +
                    1; //+ 2 for '/' and '\0'
      fileNames[fileID] = malloc(need);
      if (!fileNames[fileID]) {
        perror("malloc");
        exit(1);
      }

      snprintf(fileNames[fileID], need, "%s/%s", argv[1], entry->d_name);

      fileID++;
      assigned++;
    }
  }

  mkdir("./intermediate", 0777);

  // 1) Fork all mappers
  for (int i = 0; i < Nmappers; i++) {
    pid_t pid = fork();
    if (pid < 0) {
      perror("fork");
      exit(1);
    }

    if (pid == 0) {
      char out_path[MAX_PATH];
      snprintf(out_path, sizeof(out_path), "./intermediate/%d.tbl", i);

      int startIdx = i * groupSize + (i < remainder ? i : remainder);
      int size = groupSize + (i < remainder ? 1 : 0);

      // args: ["map", out_path, file0, file1, ..., NULL]
      char **args = malloc((3 + size) * sizeof(char *));
      args[0] = "map";
      args[1] = out_path;
      for (int j = 0; j < size; j++) {
        args[2 + j] = fileNames[startIdx + j];
      }
      args[2 + size] = NULL;

      execv("./map", args);
      perror("exec");
      _exit(1); // IMPORTANT: don't fall through into parent code
    }
  }

  // 2) After all forks, wait for all mappers
  int any_failed = 0;
  for (int k = 0; k < Nmappers; k++) {
    int status;
    pid_t w = wait(&status);
    if (w < 0) {
      perror("wait");
      any_failed = 1;
      continue;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      any_failed = 1;
    }
  }

  if (any_failed) {
    fprintf(stderr, "Map process failed\n");
    return 1;
  }

  /*
  Dispatch the requested number of reducer processes to run the ./reduce
executable, and wait for them to complete after forking them all. Each child
should be given an even portion of the key space to examine, unless it is not
evenly divisible, in which case one process will need to take the remainder They
should all be instructed to read from the ./intermediate directory The files
should be written in the same style as above, but to the ./out directory The
same rule with the child exit status applies as wel
  */

  mkdir("./out", 0777);

  int n_reducers = atoi(argv[3]);
  int key_space = 256;
  int r_group = key_space / n_reducers;
  int r_rem = key_space % n_reducers;

  for (int i = 0; i < n_reducers; i++) {
    pid_t pid = fork();
    if (pid < 0) {
      perror("fork");
      exit(1);
    }

    if (pid == 0) {
      // compute this reducer's IP first-octet range [start, end)
      int start = i * r_group + (i < r_rem ? i : r_rem);
      int end = start + r_group + (i < r_rem ? 1 : 0);

      char out_path[MAX_PATH];
      snprintf(out_path, sizeof(out_path), "./out/%d.tbl", i);

      char start_str[16], end_str[16];
      snprintf(start_str, sizeof(start_str), "%d", start);
      snprintf(end_str, sizeof(end_str), "%d", end);

      execl("./reduce", "reduce", "./intermediate", out_path, start_str,
            end_str, NULL);
      perror("exec");
      _exit(1);
    }
  }

  // Wait for all reducers to complete
  int reduce_failed = 0;
  for (int k = 0; k < n_reducers; k++) {
    int status;
    pid_t w = wait(&status);
    if (w < 0) {
      perror("wait");
      reduce_failed = 1;
      continue;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
      reduce_failed = 1;
  }

  if (reduce_failed) {
    fprintf(stderr, "Reduce process failed\n");
    return 1;
  }

  // Read each output file and print results to stdout
  DIR *out_dir = opendir("./out");
  if (out_dir == NULL) {
    perror("opendir");
    return 1;
  }

  struct dirent *out_entry;
  while ((out_entry = readdir(out_dir)) != NULL) {
    if (strcmp(out_entry->d_name, ".") == 0 || strcmp(out_entry->d_name, "..") == 0)
      continue;

    char out_path[MAX_PATH];
    snprintf(out_path, sizeof(out_path), "./out/%s", out_entry->d_name);

    table_t *result = table_from_file(out_path);
    if (result == NULL) {
      fprintf(stderr, "Failed to read output file: %s\n", out_path);
      closedir(out_dir);
      return 1;
    }

    table_print(result);
    table_free(result);
  }

  closedir(out_dir);
  return 0;
}
