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
		char time[16];
		extractItem(pJson, time, "time");
		char temperature[7];
		extractItem(pJson, temperature, "temperature");
		char soil_humidity[7];
		extractItem(pJson, soil_humidity, "soil_humidity");
		char humidity[7];
		extractItem(pJson, humidity, "humidity");
		char pm25[9];
		extractItem(pJson, pm25, "pm25");
		char light[9];
		extractItem(pJson, light, "light");
		char sound[9];
		extractItem(pJson, sound, "sound");
		char accelerometer[21];
		extractItem(pJson, accelerometer, "accelerometer");
		char gyro[21];
		extractItem(pJson, gyro, "gyro");
		char led[9];
		extractItem(pJson, led, "led");
		char voice[21];
		extractItem(pJson, voice, "voice");
		char relay[6];
		extractItem(pJson, relay, "relay");
		char display[21];
		extractItem(pJson, display, "display");
		char diy_funcA[16];
		extractItem(pJson, diy_funcA, "diy_funcA");
		char diy_funcB[16];
		extractItem(pJson, diy_funcB, "diy_funcB");
		char sql[300];
		sprintf(sql, "insert into `%srecordData`(nodeID,date,Time,Temperature,Soil_Humidity,Humidity,PM25,Light,Sound,Accelerometer,Gyro,LED,Voice,Relay,Display,diy_funcA,diy_funcB) values('%s',NOW(),'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')", username, nodeID, time, temperature, soil_humidity, humidity, pm25, light, sound, accelerometer, gyro, led, voice, relay, display, diy_funcA, diy_funcB);
		mysql_ping(&mysqlDataStore);
		mysql_query(&mysqlDataStore, sql);
	}
	cJSON_Delete(pJson);
}
