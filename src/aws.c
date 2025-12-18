// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

int set_non_blocking(int fd) {

	int flags = fcntl(fd, F_GETFL);

	if (flags < 0)
		return -1;

	flags |= O_NONBLOCK;

	return fcntl(fd, F_SETFL, flags);
}


static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	memset(conn->send_buffer, 0, BUFSIZ);
	int len = snprintf(conn->send_buffer, BUFSIZ, "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\nConnection: close\r\n\r\n", conn->file_size);
	conn->send_len = len;
	conn->send_pos = 0;

}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	memset(conn->send_buffer, 0, BUFSIZ);
	int len = snprintf(conn->send_buffer, BUFSIZ, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
	conn->send_len = len;
	conn->send_pos = 0;

}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (strstr(conn->request_path, "static"))
		return RESOURCE_TYPE_STATIC;
	if (strstr(conn->request_path, "dynamic"))
		return RESOURCE_TYPE_DYNAMIC;
	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = malloc(sizeof(struct connection));
	DIE(conn == NULL, "malloc\n");

	memset(conn, 0, sizeof(struct connection));

	conn->sockfd = sockfd;
	conn->state = STATE_INITIAL;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */

	conn->eventfd = eventfd(0, EFD_NONBLOCK);

	DIE(conn->eventfd < 0, "eventfd\n");

	memset(conn->send_buffer, 0, BUFSIZ);

	conn->async_read_len = conn->file_size - conn->file_pos;

	dlog(LOG_INFO, "conn->file_size = %ld, conn->file_pos = %ld, conn->eventf = %d\n", conn->file_size, conn->file_pos, conn->eventfd);

	if (conn->file_size > BUFSIZ)
		conn->async_read_len = BUFSIZ;


	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, conn->async_read_len, conn->file_pos);

	io_set_eventfd(&conn->iocb, conn->eventfd);

	conn->piocb[0] = &conn->iocb;

	conn->state = STATE_ASYNC_ONGOING;

	DIE(w_epoll_update_ptr_in(epollfd, conn->sockfd, conn) < 0, "w_epoll_update_ptr_in\n");

	// add it to epoll event pool
	DIE(w_epoll_add_ptr_in(epollfd, conn->eventfd, conn) < 0, "w_epoll_add_ptr_in\n");

	DIE(io_submit(ctx, 1, conn->piocb) < 0, "io_submit\n");

}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	DIE(w_epoll_remove_ptr(epollfd, conn->sockfd, conn) < 0, "w_epoll_remove_ptr\n");

	if (conn->sockfd > 0)
		DIE(close(conn->sockfd) < 0, "close\n");
	if (conn->fd > 0)
		DIE(close(conn->fd) < 0, "close\n");
	if (conn->eventfd > 0) {
		dlog(LOG_INFO, "closing eventfd related stuff...\n");
		DIE(close(conn->eventfd) < 0, "close\n");
		w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	}
	
	free(conn);

}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	int sockfd;
	socklen_t addrlen = sizeof(socklen_t);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, &addr, &addrlen);
	DIE(sockfd < 0, "accept\n");


	/* TODO: Set socket to be non-blocking. */
	rc = set_non_blocking(sockfd);
	DIE(rc < 0, "set_non_blocking\n");

	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	DIE(w_epoll_add_ptr_in(epollfd, sockfd, conn) < 0, "w_epoll_add_ptr_in\n");

	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;


}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */

	ssize_t bytes_received = 0;

	bytes_received = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);

	if (bytes_received == EAGAIN || bytes_received == EWOULDBLOCK) {
		dlog(LOG_INFO, "recv returned EAGAIN/EWOULDBLOCK, retrying...\n");
		conn->state = STATE_RECEIVING_DATA;
		return;
	}

	if (bytes_received < 0) {
		dlog(LOG_INFO, "recv failed, closing connection...\n");
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->recv_len += bytes_received;
	// sanity check, might remove
	conn->recv_buffer[conn->recv_len] = '\0';

	// ensure we've received the full http request
	if (strstr(conn->recv_buffer, "\r\n\r\n")) {
		conn->state = STATE_REQUEST_RECEIVED;
		dlog(LOG_INFO, "received request of length %ld bytes\n", conn->recv_len);
		DIE(w_epoll_update_ptr_out(epollfd, conn->sockfd, conn) < 0, "w_epoll_update_ptr_out\n");
		return;
	}

	conn->state = STATE_RECEIVING_DATA;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	char filepath[BUFSIZ];
	memset(filepath, 0, BUFSIZ);
	snprintf(filepath, BUFSIZ, ".%s", conn->request_path);
	dlog(LOG_INFO,"request_path = %s\n", filepath);

	// no such file in path
	conn->fd = open(filepath, O_RDONLY | O_NONBLOCK);

	if (conn->fd < 0) {
		conn->state = STATE_SENDING_404;
		connection_prepare_send_404(conn);
		return -1;
	}

	struct stat stats;
	DIE(fstat(conn->fd, &stats) < 0, "fstat\n");

	conn->file_size = stats.st_size;
	dlog(LOG_INFO, "conn->file_size = %ld\n", conn->file_size);

	conn->state = STATE_SENDING_HEADER;
	connection_prepare_send_reply_header(conn);

	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */

	u_int64_t efd_val;
	int rc;

	if (read(conn->eventfd, &efd_val, sizeof(efd_val)) < 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	struct io_event event;

	rc = io_getevents(ctx, 1, 1, &event, NULL);
	if (rc < 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->send_len = event.res;
	conn->send_pos = 0;
	conn->file_pos += event.res;
	ssize_t bytes_sent = 1;
	ssize_t total = 0;

	while (bytes_sent) {
		bytes_sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);
		conn->send_pos += bytes_sent;
		total += bytes_sent;
		dlog(LOG_INFO, "async sent %ld/%ld\n", total, conn->send_len);
	}

	dlog(LOG_INFO, "file_pos = %ld, file_size = %ld\n", conn->file_pos, conn->file_size);

	if (conn->file_pos < conn->file_size) {
		conn->state = STATE_SENDING_DATA;
	} else
		conn->state = STATE_DATA_SENT;
	
	dlog(LOG_INFO, "conn->state = %d\n", conn->state);

	DIE(w_epoll_remove_ptr(epollfd, conn->eventfd, conn) < 0, "w_epoll_remove_ptr\n");
	DIE(w_epoll_update_ptr_out(epollfd, conn->sockfd, conn) < 0, "w_epoll_update_ptr_out\n");
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	size_t bytes_read = 0;
	bytes_read = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	if (bytes_read) {
		dlog(LOG_INFO,"request_path %s\n", conn->request_path);
		conn->res_type = connection_get_resource_type(conn);
	}


	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	ssize_t bytes_sent = 0;

	bytes_sent = sendfile(conn->sockfd, conn->fd, &conn->file_pos, conn->file_size - conn->file_pos);
	
	if (bytes_sent < 0) {

		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			dlog(LOG_INFO, "sendfile returned EAGAIN/EWOULDBLOCK, retrying...\n");
			return STATE_SENDING_DATA;
		}

		dlog(LOG_INFO,"sendfile failed, closing connection...\n");
		return STATE_CONNECTION_CLOSED;
	}

	dlog(LOG_INFO, "sendfile progress %ld/%ld\n", conn->file_pos, conn->file_size);

	if (conn->file_pos >= conn->file_size)
		return STATE_DATA_SENT;

	return STATE_SENDING_DATA;


}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */


	ssize_t bytes_sent = 0;
	
	if (conn->state == STATE_SENDING_HEADER || conn->state == STATE_SENDING_404) {
		bytes_sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);
		conn->send_pos += bytes_sent;

		dlog(LOG_INFO, "send progress %ld/%ld\n", conn->send_pos, conn->send_len);

		if (conn->send_len == conn->send_pos) {
			conn->state = (STATE_SENDING_HEADER == conn->state ? STATE_HEADER_SENT : STATE_404_SENT);
			dlog(LOG_INFO, "header to send = %s\n", conn->send_buffer);
		}

		if (bytes_sent < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				dlog(LOG_INFO, "send returned EAGAIN/EWOULDBLOCK, retrying...\n");
				return -1;
			}
			dlog(LOG_INFO, "send failed, closing connection...\n");
			conn->state = STATE_CONNECTION_CLOSED;
			return -1;
		}

	} else if (conn->res_type == RESOURCE_TYPE_STATIC) {
		conn->state = connection_send_static(conn);
	} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
		return connection_send_dynamic(conn);
	}


	return 0;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (conn->state != STATE_ASYNC_ONGOING) {
		dlog(LOG_INFO, "starting async io\n");
		connection_start_async_io(conn);
		return 0;
	}

	return 0;

}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	switch (conn->state) {
	case STATE_INITIAL:
		dlog(LOG_INFO, "we are in state %d\n", conn->state);
	case STATE_RECEIVING_DATA:
		receive_data(conn);
		break;
	case STATE_ASYNC_ONGOING:
		dlog(LOG_INFO, "async read finished\n");
		connection_complete_async_io(conn);
		break;
	default:
		printf("shouldn't get here %d\n", conn->state);
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */

	switch (conn->state) {
	case STATE_REQUEST_RECEIVED:
		parse_header(conn);
		connection_open_file(conn);
	case STATE_SENDING_HEADER:
	case STATE_SENDING_404:
	case STATE_SENDING_DATA:
	case STATE_HEADER_SENT:
		connection_send_data(conn);
		break;
	case STATE_404_SENT:
		dlog(LOG_INFO, "404 sent\n");
	case STATE_DATA_SENT:
		dlog(LOG_INFO, "Data sent\n");
	case STATE_CONNECTION_CLOSED:
		dlog(LOG_INFO,"Closing connection\n");
		connection_remove(conn);
		break;

	default:
		dlog(LOG_ERR, "%d\n", conn->state);
		ERR("Unexpected state\n");
		exit(1);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLIN) {
		handle_input(conn);
	} else if (event & EPOLLOUT) {
		handle_output(conn);
	}
}

int main(void)
{
	int rc;

	/* TODO: Initialize asynchronous operations. */

	DIE(io_setup(AWS_MAX_AIO, &ctx) < 0, "io_setup\n");

	epollfd = w_epoll_create();
	
	DIE(epollfd < 0, "w_epoll_create\n");

	/* TODO: Initialize multiplexing. */

	listenfd = tcp_create_listener(AWS_LISTEN_PORT, AWS_BACKLOG);

	DIE(listenfd < 0, "tcp_create_listener\n");

	/* TODO: Create server socket. */

	/* TODO: Add server socket to epoll object*/

	DIE(w_epoll_add_fd_in(epollfd, listenfd) < 0, "w_epoll_add_fd_in\n");

	/* Uncomment the following line for debugging. */
	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */

		rc = w_epoll_wait_infinite(epollfd, &rev);

		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN) {
				dlog(LOG_INFO, "New connection received\n");
				handle_new_connection();
			}
		} else {
			handle_client(rev.events, rev.data.ptr);
		}
	}

	return 0;
}
