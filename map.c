#include "./include/map.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "./include/table.h"
// argc = commands
// argv[0] -> program name
// argv[1] -> output file
// argv[2] -> input file 1..... argv[n] -> input file n-1

// map_log reads log files and updates table
// main writes to the outfile and calls map_log for each file, uses table_to_file to write the table to the outfile
int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        fprintf(stderr, "Usage: map <outfile> <infiles...>\n");
        exit(1);
    }

    table_t *table = table_init();
    if (table == NULL)
    {
        fprintf(stderr, "Failed to initialize table\n");
        exit(1);
    }
    for (int i = 2; i < argc; i++)
    { // i =2 since 0 is name of file and 1 is output file
        if (map_log(table, argv[i]) != 0)
        {
            fprintf(stderr, "Failed to log file");
            table_free(table);
            exit(1);
        }
    }

    if (table_to_file(table, argv[1]) != 0)
    {
        fprintf(stderr, "Failed to write to outputfile");
        table_free(table);
        exit(1);
    }
    return 0;
}

int map_log(table_t *table, const char file_path[MAX_PATH])
{
    FILE *fp = fopen(file_path, "r");
    if (!fp)
    {
        perror("fopen");
        return 1;
    }

    char line[256];
    char date[20], ip[IP_LEN], method[8], route[37], status[4];
    while (fgets(line, sizeof(line), fp) != NULL)
    {
        if (sscanf(line, "%19[^,],%15[^,],%7[^,],%36[^,],%3s",
                   date, ip, method, route, status) != 5)
        {
            continue; // skip faulty lines
        }

        bucket_t *existing = table_get(table, ip);
        if (existing != NULL)
        {
            existing->requests++;
        }
        else
        {
            bucket_t *b = bucket_init(ip);
            if (b == NULL)
            {
                fclose(fp);
                return 1;
            }
            table_add(table, b);
        }
    }

    fclose(fp);
    return 0;
}
