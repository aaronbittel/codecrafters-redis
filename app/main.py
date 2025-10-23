import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_sock, addr = server_socket.accept()
    print(f"client with addr {addr} connected.")

    client_sock.recv(1024)
    client_sock.send(b"+PONG\r\n")


if __name__ == "__main__":
    main()
