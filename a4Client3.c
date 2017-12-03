#include <pthread.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <stdio.h>
#include <sys/types.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

const int PORT_NO = 1234;
const int BUFSIZE = 1024;

const char * query1 = "CREATE TABLE COMPANY("  \
         "ID INT PRIMARY KEY     NOT NULL," \
         "NAME           TEXT    NOT NULL," \
         "AGE            INT     NOT NULL," \
         "ADDRESS        CHAR(50)," \
         "SALARY         REAL );";

const char * query2 = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "  \
         "VALUES (1, 'Paul', 32, 'California', 20000.00 ); " \
         "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "  \
         "VALUES (2, 'Allen', 25, 'Texas', 15000.00 ); "     \
         "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" \
         "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );" \
         "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY)" \
         "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );";

const char * query3 = "SELECT * from COMPANY;";

const char * LOG_FILE = "a4ClientLog.txt";

void get_time_string(char * timebuf) {
	struct timeval tv;
	time_t nowtime;
	struct tm *nowtm;
	gettimeofday(&tv, NULL);
	nowtime = tv.tv_sec;
	nowtm = localtime(&nowtime);
	strftime(timebuf, BUFSIZE, "%Y-%m-%d %H:%M:%S", nowtm);
}

void print_log_init_message(FILE * lfp) {
	char timebuf[BUFSIZE];
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("This is client process. PID: %d\n", getpid());
	printf("Time of day: %s\n", timebuf);
	fprintf(lfp, "This is client process. PID: %d\n", getpid());
	fprintf(lfp, "Time of day: %s\n", timebuf);
}

int main(int argc, char ** argv) {
	int client_socket;
	struct sockaddr_in server_address;
	char buffer[BUFSIZE];
	char timebuf[BUFSIZE];
	FILE * lfp = fopen(LOG_FILE, "w");

	print_log_init_message(lfp);

	// Create the client socket
	client_socket = socket(AF_INET, SOCK_STREAM, 0);

	// Populate server_address struct
	server_address.sin_family = AF_INET;
	server_address.sin_port = htons(PORT_NO);
	server_address.sin_addr.s_addr = inet_addr("127.0.0.1");
	memset(server_address.sin_zero, 0, sizeof server_address.sin_zero);

	// Connect to the server address
	connect(client_socket, (struct sockaddr *) &server_address, sizeof server_address);
	// memset(buffer, 0, sizeof buffer);
	// strcpy(buffer, "select * from entries;");
	// send(client_socket, buffer, BUFSIZE, 0);
	
	// Print message saying that client is going to create a table
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("PID: %d. Creating table now...\n", getpid());
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "PID: %d. Creating table now...\n", getpid());
	fprintf(lfp, "Current time: %s\n", timebuf);
	// create table
	memset(buffer, 0, sizeof buffer);
	strcpy(buffer, query1);
	send(client_socket, buffer, BUFSIZE, 0);
	while(true) {
		memset(buffer, 0, sizeof buffer);
		recv(client_socket, buffer, BUFSIZE, 0);
		if(!strcmp(buffer, "$$")) break;
		printf("buffer: %s\n", buffer);
		fprintf(lfp, "buffer: %s\n", buffer);
	}
	printf("PID: %d. Finished creating table.\n", getpid());
	fprintf(lfp, "PID: %d. Finished creating table.\n", getpid());
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	fprintf(lfp, "Current time: %s\n", timebuf);

	// insert
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("PID: %d. Inserting rows now...\n", getpid());
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "PID: %d. Inserting rows now...\n", getpid());
	fprintf(lfp, "Current time: %s\n", timebuf);
	memset(buffer, 0, sizeof buffer);
	strcpy(buffer, query2);
	send(client_socket, buffer, BUFSIZE, 0);
	while(true) {
		memset(buffer, 0, sizeof buffer);
		recv(client_socket, buffer, BUFSIZE, 0);
		if(!strcmp(buffer, "$$")) break;
		printf("buffer: %s\n", buffer);
		fprintf(lfp, "buffer: %s\n", buffer);
	}
	printf("PID: %d. Finished Inserting rows.\n", getpid());
	fprintf(lfp, "PID: %d. Finished Inserting rows.\n", getpid());
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "Current time: %s\n", timebuf);

	// select query
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("PID: %d. Performing SELECT operation now...\n", getpid());
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "PID: %d. Performing SELECT operation now...\n", getpid());
	fprintf(lfp, "Current time: %s\n", timebuf);
	memset(buffer, 0, sizeof buffer);
	strcpy(buffer, query3);
	send(client_socket, buffer, BUFSIZE, 0);
	while(true) {
		memset(buffer, 0, sizeof buffer);
		recv(client_socket, buffer, BUFSIZE, 0);
		if(!strcmp(buffer, "$$")) break;
		printf("buffer: %s\n", buffer);
		fprintf(lfp, "buffer: %s\n", buffer);
	}
	printf("PID: %d. Finished SELECT operation.\n", getpid());
	fprintf(lfp, "PID: %d. Finished SELECT operation.\n", getpid());
	memset(timebuf, 0, sizeof timebuf);
	get_time_string(timebuf);
	printf("Current time: %s\n", timebuf);
	fprintf(lfp, "Current time: %s\n", timebuf);
	// printf("buffer: %s\n", buffer);

	// close the log file
	fclose(lfp);
	return 0;
}
