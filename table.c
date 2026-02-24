#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "./include/map.h"

bucket_t *bucket_init(const char ip[IP_LEN])
{
    // helper function to initialize a bucket with the given IP address
    bucket_t *bucket = malloc(sizeof(bucket_t));
    if (bucket == NULL)
    {
        perror("Couldn't allocate memory for bucket");
        return NULL;
    }
    strcpy(bucket->ip, ip);
    bucket->requests = 1;
    return bucket;
}

table_t *table_init()
{
    table_t *table = malloc(sizeof(table_t));
    if (table == NULL)
    {
        perror("Couldn't allocate memory for table");
        return NULL;
    }
    // set to 0 to avoid garbage data
    for (int i = 0; i < TABLE_LEN; i++)
        table->buckets[i] = 0;
    return table;
}

void table_print(const table_t *table)
{
    // Print format: {IP} - {num requests}
    for (int i = 0; i < TABLE_LEN; i++)
    {
        bucket_t *bucket = table->buckets[i];
        while (bucket != NULL)
        {
            printf("%s - %d\n", bucket->ip, bucket->requests);
            bucket = bucket->next;
        }
    }
    return;
}

void table_free(table_t *table)
{
    for (int i = 0; i < TABLE_LEN; i++){
        bucket_t *bucket = table ->buckets[i];
        while (bucket != NULL){
            bucket_t *temp = bucket;
            bucket = bucket->next;
            free(temp);
    }
    
   
}
free(table);
 return;
}

int table_add(table_t *table, bucket_t *bucket)
{

    if(bucket == NULL || table_get(table, bucket->ip) != NULL)return -1;
    int hash = hash_ip(bucket->ip);
    bucket->next = table->buckets[hash];
    table->buckets[hash] = bucket;
    return 0;
}

bucket_t *table_get(table_t *table, const char ip[IP_LEN])
{
    if (table == NULL || ip == NULL)return NULL;
    int hash = hash_ip(ip);
    bucket_t *bucket = table->buckets[hash];
    while(bucket != NULL && strcmp(bucket->ip,ip) != 0)bucket = bucket->next;
    if (bucket == NULL)return NULL;
    else return bucket;
}
//add the int values of every character in the ip, and mod by table length

int hash_ip(const char ip[IP_LEN])
{
    if (ip == NULL)return -1;
    int sum = 0;
    while(*ip != '\0'){
        sum += (int)*ip;
        ip++;
    }
    return sum % TABLE_LEN;
}

int table_to_file(table_t *table, const char out_file[MAX_PATH])
{
    if (table == NULL || out_file == NULL)return -1;
    FILE *fp = fopen(out_file, "w");
    if (!fp){
        perror("fopen");
        return -1;
    }
    // write every file sequentially to the file with fwrite
    for (int i = 0; i < TABLE_LEN;){
        bucket_t *bucket = table->buckets[i];
        while(bucket != NULL){
            if (fwrite(bucket,sizeof(bucket_t),1,fp) !=1){
                perror("fwrite failed");
                fclose(fp);
                return -1;
            }
            bucket = bucket->next;
        }
        i++;
    }
    fclose(fp);
    return 0;
}

table_t *table_from_file(const char in_file[MAX_PATH])
{


    if (in_file == NULL){
        perror("in_file is NULL");
        return NULL;
    }
    FILE *fp = fopen(in_file, "rb");
    if (!fp){
        perror("fopen failed");
        return NULL;
    }
    table_t *table = table_init();
    if (table == NULL){
        perror("table_init failed");
        fclose(fp); 
    }
    bucket_t temp;
    while(fread(&temp,sizeof(bucket_t),1,fp)==1){
        bucket_t *bucket = bucket_init(temp.ip);
        if (bucket == NULL){
            perror("bucket_init failed");
            table_free(table);
            fclose(fp);
            return NULL;
        }
        bucket->requests = temp.requests;
        if (table_add(table, bucket) == -1){
            perror("table_add failed");
            free(bucket);
            table_free(table);
            fclose(fp);
            return NULL;        

    }
}
    fclose(fp);
    return table;
}
