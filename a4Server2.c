#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <stdbool.h>
#include <sqlite3.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>

#define DEBUG true

const int BUFSIZE = 1024;
const int PORT_NO = 1234;
const int MAX_QUEUED_CONNECTIONS = 6;
const char * DB_NAME = "test.db";
const char * LOG_FILE = "a4ServerLog.txt";
FILE * lfp;

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

static int callback(void *sockfd, int argc, char **argv, char **azColName) {
	int i;
	char buffer[BUFSIZE];
	int sfd = *((int*)sockfd);
	memset(buffer, 0, sizeof buffer);
	for(i = 0; i<argc; i++) {
		// printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
		char tmp[BUFSIZE];
		memset(tmp, 0, sizeof tmp);
		sprintf(tmp, "%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
		strcat(buffer, tmp);
	}
	strcat(buffer, "\n");
	send(sfd, buffer, BUFSIZE, 0);
	return 0;
}

void get_time_string(char * timebuf) {
	struct timeval tv;
	time_t nowtime;
	struct tm *nowtm;
	gettimeofday(&tv, NULL);
	nowtime = tv.tv_sec;
	nowtm = localtime(&nowtime);
	strftime(timebuf, BUFSIZE, "%Y-%m-%d %H:%M:%S", nowtm);
}

void print_log_init_message() {
	char timebuf[BUFSIZE];
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("This is server process. PID: %d. Thread ID: %d\n", getpid(), (int)pthread_self());
	printf("Time of day: %s\n", timebuf);
	fprintf(lfp, "This is server process. PID: %d. Thread ID: %d\n", getpid(), (int)pthread_self());
	fprintf(lfp, "Time of day: %s\n", timebuf);
}

void * server_slave(void * slave_params) {
	// we want that the stdout resource only be available to one
	// slave thread at a time -- otherwise, the output of many
	// slave threads could get mixed up. Hence, lock the stdout
	// resource using a mutex lock.
	pthread_mutex_lock(&mtx);
	int tid = (int)(pthread_self());
#if DEBUG
	printf("Slave Socket_id = %d\n",*((int*)(slave_params)));
#endif
	int sockfd = *((int*)slave_params);
	char buffer[BUFSIZE];
	sqlite3 *db;
	char * zErrMsg = 0;
	int rc = 0;
	char timebuf[BUFSIZE];

	print_log_init_message();

	// Recieve query 1
	// This is the query to create a table in the database
	// We assume that the database has already been created (by touch test.db)
	memset(buffer, 0, sizeof buffer);
	recv(sockfd, buffer, BUFSIZE, 0);
	// printf("Received from client: %s\n", buffer);
	// Open db
	rc = sqlite3_open(DB_NAME, &db);
	if(rc) {
		fprintf(stdout, "Can't open database: %s\n", sqlite3_errmsg(db));
		return NULL;
	} else {
		fprintf(stdout, "Opened database successfully\n");
	}
	// Execute the create statement recieved from client
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("PID: %d. Thread ID: %d.\nProcessing query 1 - create table\n", getpid(), tid);
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "PID: %d. Thread ID: %d.\nProcessing query 1 - create table\n", getpid(), tid);
	fprintf(lfp, "Current time: %s\n", timebuf);
	rc = sqlite3_exec(db, buffer, callback, (void*)(&sockfd), &zErrMsg);
	if(rc != SQLITE_OK) {
		fprintf(stdout, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	} else {
		fprintf(stdout, "Table created successfully\n");
		fprintf(stdout, "Current time: %s\n", timebuf);
		fprintf(lfp, "Table created successfully\n");
		fprintf(lfp, "Current time: %s\n", timebuf);
		memset(buffer, 0, sizeof buffer);
		strcpy(buffer, "$$");
		send(sockfd, buffer, BUFSIZE, 0);
	}

	// Recieve query 2
	// This is the query to insert records into the database
	memset(buffer, 0, sizeof buffer);
	recv(sockfd, buffer, BUFSIZE, 0);
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("PID: %d. Thread ID: %d.\nProcessing query 2 - insert rows\n", getpid(), tid);
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "PID: %d. Thread ID: %d.\nProcessing query 2 - insert rows\n", getpid(), tid);
	fprintf(lfp, "Current time: %s\n", timebuf);
	rc = sqlite3_exec(db, buffer, callback, (void*)(&sockfd), &zErrMsg);
	if( rc != SQLITE_OK ) {
		fprintf(stdout, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	} else {
		fprintf(stdout, "Records created successfully\n");
		fprintf(stdout, "Current time: %s\n", timebuf);
		fprintf(lfp, "Records created successfully\n");
		fprintf(lfp, "Current time: %s\n", timebuf);
		memset(buffer, 0, sizeof buffer);
		strcpy(buffer, "$$");
		send(sockfd, buffer, BUFSIZE, 0);
	}
	
	
	// Recieve query 3
	// This is the query to retrieve records from the database
	memset(buffer, 0, sizeof buffer);
	recv(sockfd, buffer, BUFSIZE, 0);
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("PID: %d. Thread ID: %d.\nProcessing query 3 - select rows\n", getpid(), tid);
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "PID: %d. Thread ID: %d.\nProcessing query 3 - select rows\n", getpid(), tid);
	fprintf(lfp, "Current time: %s\n", timebuf);
	rc = sqlite3_exec(db, buffer, callback, (void*)(&sockfd), &zErrMsg);
	if( rc != SQLITE_OK ) {
		fprintf(stdout, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	} else {
		memset(timebuf, 0, sizeof timebuf);
		get_time_string(timebuf);
		fprintf(stdout, "Selection completed successfully\n");
		fprintf(stdout, "Current time: %s\n", timebuf);
		fprintf(lfp, "Selection completed successfully\n");
		fprintf(lfp, "Current time: %s\n", timebuf);
		memset(buffer, 0, sizeof buffer);
		strcpy(buffer, "$$");
		send(sockfd, buffer, BUFSIZE, 0);
	}

	// Close db
	sqlite3_close(db);

	// Server slave terminating message
	fprintf(stdout, "PID: %d. Thread ID: %d.\nThread terminating now..\n", getpid(), tid);
	fprintf(lfp, "PID: %d. Thread ID: %d.\nThread terminating now..\n", getpid(), tid);
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "Current time: %s\n", timebuf);


	// Release mutex lock
	fflush(lfp);
	pthread_mutex_unlock(&mtx);
}

void * server_main(void * server_params) {
	int welcome_socket, new_connection;
	struct sockaddr_in server_address;
	struct sockaddr_storage server_storage;
	welcome_socket = socket(AF_INET, SOCK_STREAM, 0);
	pthread_t server_slave_thread;
	
	// Populate the server_address struct
	server_address.sin_family = AF_INET;
	server_address.sin_port = htons(PORT_NO);
	// Set the IP address to localhost
	server_address.sin_addr.s_addr = inet_addr("127.0.0.1");
	memset(server_address.sin_zero, 0, sizeof server_address.sin_zero);

	// Bind address to socket
	bind(welcome_socket, (struct sockaddr *) &server_address, sizeof(server_address));

	// Listen
	while(true) {
		if(listen(welcome_socket, MAX_QUEUED_CONNECTIONS) == 0) {
			// printf("Listening now .. i\n");
			socklen_t addr_size = sizeof server_address;
			new_connection = accept(welcome_socket, (struct sockaddr*)&server_address, &addr_size);
			pthread_create(&server_slave_thread, NULL, server_slave, (void*)(&new_connection));
			// pthread_join(server_slave_thread, NULL);
		} else {
			printf("Some error occurred\n");
		}
	}

}

int main(int argc, char ** argv) {
	lfp = fopen(LOG_FILE, "w");
	server_main(NULL);
	fclose(lfp);
	return 0;
}
