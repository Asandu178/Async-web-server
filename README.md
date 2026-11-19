# Asynchronous Web Server

## Overview

This project was originally developed as a homework assignment for the Operating Systems course. I decided to make the repository public to showcase some of the concepts I worked with, particularly socket programming and asynchronous I/O.

The goal of the assignment was to implement a simple HTTP server capable of serving files to multiple clients using advanced I/O operations.

## Implementation

Working on this project helped me understand several concepts related to network programming and asynchronous I/O, such as:

- Using sockets to establish connections and communicate with clients.
- Handling multiple client connections using `epoll()`.
- Working with non-blocking sockets to avoid blocking the server while handling requests.
- Using `sendfile()` to transfer static files without copying data through a user-space buffer.
- Using `libaio` to perform asynchronous file I/O when serving dynamic files.
- Managing asynchronous I/O operations and tracking each connection's progress using a state machine.
- Handling HTTP requests and constructing appropriate responses, including `404` errors.

This assignment gave me practical experience with Linux I/O APIs and helped me better understand how servers handle multiple connections efficiently.