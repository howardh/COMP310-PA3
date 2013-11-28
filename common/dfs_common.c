#include "common/dfs_common.h"
#include <pthread.h>
/**
 * create a thread and activate it
 * entry_point - the function exeucted by the thread
 * args - argument of the function
 * return the handler of the thread
 */
extern inline pthread_t * create_thread(void * (*entry_point)(void*), void *args)
{
	//DONE: create the thread and run it
	pthread_t * thread = malloc(sizeof(pthread_t));
	pthread_create(thread, NULL, entry_point, args);

	return thread;
}

/**
 * create a socket and return
 */
int create_tcp_socket()
{
	//DONE:create the socket and return the file descriptor 
	return socket(AF_INET,SOCK_STREAM,0);
}

/**
 * create the socket and connect it to the destination address
 * return the socket fd
 */
int create_client_tcp_socket(char* address, int port)
{
	printf("Creating client socket on %s:%d\n", address, port);
	assert(port >= 0 && port < 65536);
	int socket = create_tcp_socket();
	if (socket == INVALID_SOCKET) return 1;

	//printf("Socket created.\n");

	//DONE: connect it to the destination port
	sockaddr_in sin;
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr.s_addr = inet_addr(address);
	//printf("Connecting to destination port %d.\n", port);
    int err = connect(socket, (struct sockaddr*)&sin, sizeof(struct sockaddr_in));
	if (err < 0)
		fprintf(stderr, "Connection failed.\n");
	//else
	//	printf("Connection successful.\n");

	return socket;
}

/**
 * create a socket listening on the certain local port and return
 */
int create_server_tcp_socket(int port)
{
	assert(port >= 0 && port < 65536);
	int sd = create_tcp_socket();
	if (sd == INVALID_SOCKET) return 1;

	//DONE: listen on local port
	struct sockaddr_in serv_addr, cli_addr;
	serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if (bind(sd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
		perror("ERROR on binding");

	if (listen(sd, 10) == -1)
		fprintf(stderr, "ERROR\n");
	//else
	//	printf("Listening on port %d.\n", port);

	return sd;
}

/**
 * socket - connecting socket
 * data - the buffer containing the data
 * size - the size of buffer, in byte
 */
void send_data(int socket, void* data, int size)
{
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	//DONE: send data through socket
	int n = write(socket, data, size);
	printf("%d/%d bytes sent.\n", n, size);
}

/**
 * receive data via socket
 * socket - the connecting socket
 * data - the buffer to store the data
 * size - the size of buffer in byte
 */
void receive_data(int socket, void* data, int size)
{
	if (size == 0) return;
	assert(data != NULL);
	assert(size >= 0);
	if (socket == INVALID_SOCKET) return;
	//DONE: fetch data via socket
	int n = read(socket, data, size);
	if (n <= 0)
		perror("Error reading from socket");
	printf("%d/%d bytes received.\n", n, size);
	receive_data(socket, data+n, size-n);
}
