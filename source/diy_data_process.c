#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mosquitto.h>
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
		char temp[121];
		if(cJSON_IsString(jsonItem)){
			strcpy(temp, jsonItem->valuestring);
		}else if(cJSON_IsNumber(jsonItem)){
			sprintf(temp, "%0.2lf", jsonItem->valuedouble);
		}else{
			strcpy(temp, "NULL");
		}
		strcpy(item, temp);
	}else{
		strcpy(item, "NULL");
	}
}

void processIFTTT(const char* username, const char* user_pass, const char* topic, const char* data, cJSON* jsonConfigurationItem){
	cJSON* pJsonData = cJSON_Parse(data);
	if(NULL != pJsonData){
		int satisfy = 1;
		char message[70];
		cJSON* conditionsLeftItem = cJSON_GetObjectItem(jsonConfigurationItem, "if_left_item");
		cJSON* conditionsOpItem = cJSON_GetObjectItem(jsonConfigurationItem, "if_op");
		cJSON* conditionsRightItem = cJSON_GetObjectItem(jsonConfigurationItem, "if_right_item");
		cJSON* conditionsRightItemType = cJSON_GetObjectItem(jsonConfigurationItem, "right_item_type");
		cJSON* doContent = cJSON_GetObjectItem(jsonConfigurationItem, "do");
		int conditionNumber = cJSON_GetArraySize(conditionsLeftItem);
		strncpy(message, doContent->valuestring, 69);
		int i=0;
		for(;i<conditionNumber;i++){
			cJSON* oneConditionLeftItem = cJSON_GetArrayItem(conditionsLeftItem, i);
			cJSON* oneConditionOpItem = cJSON_GetArrayItem(conditionsOpItem, i);
			cJSON* oneConditionRightItem = cJSON_GetArrayItem(conditionsRightItem, i);
			cJSON* oneConditionRightItemType = cJSON_GetArrayItem(conditionsRightItemType, i);
			cJSON* oneRawData = cJSON_GetObjectItem(pJsonData, oneConditionLeftItem->valuestring);
			if(oneRawData == NULL){
				satisfy = 0;
				break;
			}else{
				if(strcmp(oneConditionRightItemType->valuestring, "double") == 0){
					double rightItemVal, dataVal;
					sscanf(oneConditionRightItem->valuestring, "%lf", &rightItemVal);
					sscanf(oneRawData->valuestring, "%lf", &dataVal);
					if(strcmp(oneConditionOpItem->valuestring, "<") == 0){
						if(!(dataVal < rightItemVal)){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, "<=") == 0){
						if(!(dataVal <= rightItemVal)){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, ">") == 0){
						if(!(dataVal > rightItemVal)){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, ">=") == 0){
						if(!(dataVal >= rightItemVal)){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, "=") == 0){
						if(!(dataVal == rightItemVal)){
							satisfy = 0;
							break;
						}
					}
				}else if(strcmp(oneConditionRightItemType->valuestring, "string") == 0){
					if(strcmp(oneConditionOpItem->valuestring, "<") == 0){
						if(strcmp(oneRawData->valuestring, oneConditionRightItem->valuestring) < 0){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, "<=") == 0){
						if(strcmp(oneRawData->valuestring, oneConditionRightItem->valuestring) <= 0){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, ">") == 0){
						if(strcmp(oneRawData->valuestring, oneConditionRightItem->valuestring) > 0){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, ">=") == 0){
						if(strcmp(oneRawData->valuestring, oneConditionRightItem->valuestring) >= 0){
							satisfy = 0;
							break;
						}
					}else if(strcmp(oneConditionOpItem->valuestring, "=") == 0){
						if(strcmp(oneRawData->valuestring, oneConditionRightItem->valuestring) == 0){
							satisfy = 0;
							break;
						}
					}
				}
			}
		}
		if(satisfy){
			mosquitto_lib_init();
			char transportClient[40];
			strncpy(transportClient, "ServerClient_", 39);
			strncat(transportClient, username, 26);
			struct mosquitto* mosq = mosquitto_new(transportClient, true, NULL);
			if(!mosq){
				printf("Create mosquitto client failed.\r\n");
				cJSON_Delete(pJsonData);
				mosquitto_destroy(mosq);
				mosquitto_lib_cleanup();
				return;
			}
			if(mosquitto_username_pw_set(mosq, username, user_pass)){		// Username and password must be set before connect
				printf("Client auth failed.\r\n");
				cJSON_Delete(pJsonData);
				mosquitto_destroy(mosq);
				mosquitto_lib_cleanup();
				return;
			}
			if(mosquitto_connect(mosq, "localhost", 1883, 60)){
				printf("Connect failed.\r\n");
				cJSON_Delete(pJsonData);
				mosquitto_destroy(mosq);
				mosquitto_lib_cleanup();
				return;
			}
			if(mosquitto_publish(mosq, NULL, topic, strlen(message), message, 0, false)){
				printf("Publish failed.\r\n");
			}
			mosquitto_disconnect(mosq);
			mosquitto_destroy(mosq);
			mosquitto_lib_cleanup();
		}
	}
	cJSON_Delete(pJsonData);
}

void recordData(const char* content, const char* username, const char* topic){
	cJSON* pJson = cJSON_Parse(content);
	if(NULL != pJson){
		int itemNumber = 19;
		char items[19][15]  = {"time", "temperature", "soil_humidity", "humidity", "pm25", "light", "accelerometer", "gyro", "co2", "voice", "sound", "diy_funcA", "diy_funcB", "led", "relay", "motor", "display", "buzzer", "speaker"};
		char data[200];
		strcpy(data, "{");
		char dataItem[120];
		int i=0;
		for(i=0;i<itemNumber;i++){
			extractItem(pJson, dataItem, items[i]);
			if(strcmp(dataItem, "NULL") !=0 ){
				strcat(strcat(strcat(strcat(strcat(data, "\""),items[i]),"\":\""),dataItem),"\",");
			}
		}
		int len = strlen(data);
		data[len-1] = '}';
		data[len] = '\0';
		char sql[300];
		mysql_ping(&mysqlDataStore);
		strcat(strcat(strcpy(sql,"SELECT id,pass FROM user WHERE username = '"),username),"'");
		if( mysql_real_query(&mysqlDataStore ,sql, strlen(sql)) ){
			printf("SQL Query Error!(Find user id according to username)\r\n");
			cJSON_Delete(pJson);
			return;
		}
		char user_id[12];
		char user_pass[21];
		MYSQL_RES* user_id_res = mysql_store_result(&mysqlDataStore);
		if(mysql_num_rows(user_id_res) == 0){
			mysql_free_result(user_id_res);
			cJSON_Delete(pJson);
			return;
		}
		MYSQL_ROW user_id_row = mysql_fetch_row(user_id_res);
		strcpy(user_id, user_id_row[0]);
		strcpy(user_pass, user_id_row[1]);
		mysql_free_result(user_id_res);
		char node_id[12];
		char topicTemp[16];
		strcpy(topicTemp, topic);
		char nodeName[11];
		strcpy(nodeName, strtok(topicTemp, "@wt"));
		strcpy(node_id, nodeName);
		strcat(strcat(strcat(strcat(strcpy(sql,"SELECT id FROM node WHERE user_id = "),user_id)," AND name = '"),node_id),"'");
		if( mysql_real_query(&mysqlDataStore ,sql, strlen(sql)) ){
			printf("SQL Query Error!(Find node id according to node name)\r\n");
			cJSON_Delete(pJson);
			return;
		}
		MYSQL_RES* node_id_res = mysql_store_result(&mysqlDataStore);
		if(mysql_num_rows(node_id_res) == 0){
			mysql_free_result(node_id_res);
			cJSON_Delete(pJson);
			return;
		}
		MYSQL_ROW node_id_row = mysql_fetch_row(node_id_res);
		strcpy(node_id, node_id_row[0]);
		mysql_free_result(node_id_res);
		char configuration[1000];
		strcpy(configuration, "");
		strcat(strcpy(sql, "SELECT configuration FROM userapplication WHERE node_id = "),node_id);
		if( mysql_real_query(&mysqlDataStore ,sql, strlen(sql)) ){
			printf("SQL Query Error!(Find application configuration according to node id)\r\n");
			cJSON_Delete(pJson);
			return;
		}
		MYSQL_RES* application_configuration_res = mysql_store_result(&mysqlDataStore);
		if(mysql_num_rows(application_configuration_res) != 0){
			MYSQL_ROW application_configuration_row = mysql_fetch_row(application_configuration_res);
			strcpy(configuration, application_configuration_row[0]);
		}
		mysql_free_result(application_configuration_res);
		strcat(strcat(strcat(strcat(strcpy(sql, "INSERT INTO recorddata(node_id, data, date) VALUES("),node_id),",'"),data),"',NOW())");
		mysql_query(&mysqlDataStore, sql);
		cJSON* pConfigurationJson = cJSON_Parse(configuration);
		if(NULL != pConfigurationJson){
			cJSON* jsonConfigurationItem = cJSON_GetObjectItem(pConfigurationJson, "ifttt");
			if(NULL != jsonConfigurationItem){
				char orderTopic[16];
				strcpy(orderTopic, nodeName);
				strcat(orderTopic, "@rt");
				processIFTTT(username, user_pass, orderTopic, data, jsonConfigurationItem);
			}
		}
		cJSON_Delete(pConfigurationJson);
	}
	cJSON_Delete(pJson);
}
