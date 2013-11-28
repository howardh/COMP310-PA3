#include "client/dfs_client.h"
#include "datanode/ext.h"

int connect_to_nn(char* address, int port)
{
	assert(address != NULL);
	assert(port >= 1 && port <= 65535);
	//DONE: create a socket and connect it to the server (address, port)
	//assign return value to client_socket 

	//return (client_socket = create_client_tcp_socket(address, port));
	return create_client_tcp_socket(address, port);
}

int modify_file(char *ip, int port, const char* filename, int file_size, int start_addr, int end_addr)
{
	int namenode_socket = connect_to_nn(ip, port);
	if (namenode_socket == INVALID_SOCKET) return -1;
	FILE* file = fopen(filename, "rb");
	assert(file != NULL);

	//TODO:fill the request and send
	dfs_cm_client_req_t request;
	strcpy(request.file_name, filename);
	request.file_size = file_size;
	request.req_type = 3;
	
	//DONE: receive the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket, &response, sizeof(response));

	//TODO: send the updated block to the proper datanode

	fclose(file);
	return 0;
}

int push_file(int namenode_socket, const char* local_path)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(local_path != NULL);
	FILE* file = fopen(local_path, "rb");
	assert(file != NULL);

	// Create the push request
	dfs_cm_client_req_t request;

	//DONE:fill the fields in request and 
	strcpy(request.file_name, local_path);
	fseek(file, 0, SEEK_END); request.file_size = ftell(file); fseek(file, 0, SEEK_SET); //File size
	request.req_type = 1; //1 = write
	send_data(namenode_socket, &request, sizeof(request));
	
	//DONE:Receive the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket, &response, sizeof(response));

	//DONE: Send blocks to datanodes one by one
	dfs_cli_dn_req_t block;
	block.op_type = 1; //1 = create
	int num_blocks = (response.query_result.file_size + (DFS_BLOCK_SIZE - 1)) / DFS_BLOCK_SIZE;
	int i;
	for (i = 0; i < num_blocks; i++)
	{
		memcpy(&block.block, &response.query_result.block_list[i], sizeof(dfs_cm_block_t));
		int n = fread(block.block.content, sizeof(char), DFS_BLOCK_SIZE, file);
		if (n <= 0)
		{
			printf("Done reading?\n");
			break;
		}

		int dn_socket = create_client_tcp_socket(block.block.loc_ip, block.block.loc_port);
		send_data(dn_socket, &block, sizeof(block));
	}

	fclose(file);
	return 0;
}

int pull_file(int namenode_socket, const char *filename)
{
	assert(namenode_socket != INVALID_SOCKET);
	assert(filename != NULL);

	//DONE: fill the request, and send
	dfs_cm_client_req_t request;
	strcpy(request.file_name, filename);
	request.req_type = 0;
	send_data(namenode_socket, &request, sizeof(request));

	//DONE: Get the response
	dfs_cm_file_res_t response;
	receive_data(namenode_socket, &response, sizeof(response));
	
	//TODO: Receive blocks from datanodes one by one
	FILE *file = fopen(filename, "wb");
	dfs_cli_dn_req_t request2;
	request2.op_type = 0; //0 = read
	int num_blocks = (response.query_result.file_size + (DFS_BLOCK_SIZE - 1)) / DFS_BLOCK_SIZE;
	int i;
	for (i = 0; i < num_blocks; i++)
	{
		memcpy(&request2.block, &response.query_result.block_list[i], sizeof(dfs_cm_block_t));

		int dn_socket = create_client_tcp_socket(request2.block.loc_ip, request2.block.loc_port);
		send_data(dn_socket, &request2, sizeof(request2));
		receive_data(dn_socket, &request2.block.content, DFS_BLOCK_SIZE);
		int n = fwrite(request2.block.content, sizeof(char), DFS_BLOCK_SIZE, file);
	}
	
	//TODO: resemble the received blocks into the complete file
	//Done above
	fclose(file);
	return 0;
}

dfs_system_status *get_system_info(int namenode_socket)
{
	assert(namenode_socket != INVALID_SOCKET);
	//DONE: fill the result and send 
	dfs_cm_client_req_t request;
	memset(&request, 0, sizeof(request));
	request.req_type = 2;
	printf("Sending get_system_info request..."); fflush(stdout);
	send_data(namenode_socket, &request, sizeof(request));
	printf("Done.\n");
	
	//DONE: get the response
	dfs_system_status *response = malloc(sizeof(dfs_system_status)); 
	printf("Receiving get_system_info response..."); fflush(stdout);
	receive_data(namenode_socket, response, sizeof(response));
	printf("Done. (datanode_num=%d)\n", response->datanode_num);

	return response;
}

int send_file_request(char **argv, char *filename, int op_type)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		return -1;
	}

	int result = 1;
	switch (op_type)
	{
		case 0:
			result = pull_file(namenode_socket, filename);
			break;
		case 1:
			result = push_file(namenode_socket, filename);
			break;
	}
	close(namenode_socket);
	return result;
}

dfs_system_status *send_sysinfo_request(char **argv)
{
	int namenode_socket = connect_to_nn(argv[1], atoi(argv[2]));
	if (namenode_socket < 0)
	{
		fprintf(stderr, "ERROR\n");
		return NULL;
	}
	dfs_system_status* ret = get_system_info(namenode_socket);
	close(namenode_socket);
	return ret;
}
