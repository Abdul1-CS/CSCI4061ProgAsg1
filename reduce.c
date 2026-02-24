#include "./include/reduce.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "./include/table.h"

// argv[0] -> program name
// argv[1] -> read directory
// argv[2] -> output file
// argv[3] -> start ip (first octet, inclusive)
// argv[4] -> end ip (first octet, exclusive)

int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        fprintf(stderr, "Usage: reduce <read dir> <out file> <start ip> <end ip>\n");
        exit(1);
    }

    // Validate start_ip and end_ip are integers
    char *endptr;
    long start_ip = strtol(argv[3], &endptr, 10);
    if (*endptr != '\0')
    {
        fprintf(stderr, "reduce: invalid IP range\n");
        exit(1);
    }
    long end_ip = strtol(argv[4], &endptr, 10);
    if (*endptr != '\0')
    {
        fprintf(stderr, "reduce: invalid IP range\n");
        exit(1);
    }

    DIR *dir = opendir(argv[1]);
    if (dir == NULL)
    {
        perror("opendir");
        exit(1);
    }

    table_t *table = table_init();
    if (table == NULL)
    {
        fprintf(stderr, "Failed to initialize table\n");
        closedir(dir);
        exit(1);
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        char path[MAX_PATH];
        snprintf(path, sizeof(path), "%s/%s", argv[1], entry->d_name);

        if (reduce_file(table, path, (int)start_ip, (int)end_ip) != 0)
        {
            fprintf(stderr, "Failed to reduce file: %s\n", path);
            table_free(table);
            closedir(dir);
            exit(1);
        }
    }

    closedir(dir);

    if (table_to_file(table, argv[2]) != 0)
    {
        fprintf(stderr, "Failed to write output file\n");
        table_free(table);
        exit(1);
    }

    table_free(table);
    return 0;
}

int reduce_file(table_t *table, const char file_path[MAX_PATH], const int start_ip,
                const int end_ip)
{
    table_t *src = table_from_file(file_path);
    if (src == NULL)
        return 1;

    for (int i = 0; i < TABLE_LEN; i++)
    {
        bucket_t *bucket = src->buckets[i];
        while (bucket != NULL)
        {
            int first_octet = 0;
            sscanf(bucket->ip, "%d", &first_octet);

            if (first_octet >= start_ip && first_octet < end_ip)
            {
                bucket_t *existing = table_get(table, bucket->ip);
                if (existing != NULL)
                {
                    existing->requests += bucket->requests;
                }
                else
                {
                    bucket_t *new_b = bucket_init(bucket->ip);
                    if (new_b == NULL)
                    {
                        table_free(src);
                        return 1;
                    }
                    new_b->requests = bucket->requests;
                    table_add(table, new_b);
                }
            }

            bucket = bucket->next;
        }
    }

    table_free(src);
    return 0;
}
