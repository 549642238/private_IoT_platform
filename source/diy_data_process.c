#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "diy_data_process.h"

MYSQL mysqlDataStore;                            // 数据库操作句柄

int connectDB(){
	char c = 1;
	mysql_library_init(0, NULL, NULL);
	mysql_init(&mysqlDataStore);
	mysql_options(&mysqlDataStore, MYSQL_OPT_RECONNECT, (char*)&c);
	if (!mysql_real_connect(&mysqlDataStore,DBhost,DBuser,DBpass,DBdb,0,NULL,0)){
		printf("%s", mysql_error(&mysqlDataStore));
		return -1;
	}else {
		return 0;
	}
}

void disconnectDB(){
	mysql_close(&mysqlDataStore);
	mysql_library_end();
}

void extractItem(cJSON* pJson, char* item, const char* itemName){
	cJSON* jsonItem = cJSON_GetObjectItem(pJson, itemName);
	if(NULL != jsonItem){
		char temp[31];
		if(strcmp(itemName, "nodeID") == 0){
			if(cJSON_IsString(jsonItem)){
				strcpy(temp, jsonItem->valuestring);
			}else if(cJSON_IsNumber(jsonItem)){
				sprintf(temp, "%d", jsonItem->valueint);
			}else{
				strcpy(temp, "NULL");
			}
		}else{
			if(cJSON_IsString(jsonItem)){
				strcpy(temp, jsonItem->valuestring);
			}else if(cJSON_IsNumber(jsonItem)){
				sprintf(temp, "%0.2lf", jsonItem->valuedouble);
			}else{
				strcpy(temp, "NULL");
			}
		}
		strcpy(item, temp);
	}else{
		strcpy(item, "NULL");
	}
}

void recordData(const char* content, const char* username){
	cJSON* pJson = cJSON_Parse(content);
	if(NULL != pJson){
		char nodeID[11];
		extractItem(pJson, nodeID, "nodeID");
		char temperature[11];
		extractItem(pJson, temperature, "temperature");
		char humidity[11];
		extractItem(pJson, humidity, "humidity");
		char pm25[11];
		extractItem(pJson, pm25, "pm25");
		char sql[200];
		sprintf(sql, "insert into `recorddata`(username,nodeID,date,Temperature,Humidity,PM25) values('%s','%s',NOW(),%s,%s,%s)", username, nodeID, temperature, humidity, pm25);
		mysql_ping(&mysqlDataStore);
		mysql_query(&mysqlDataStore, sql);
	}
	cJSON_Delete(pJson);
}
