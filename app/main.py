import socket
import threading


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        client_sock, addr = server_socket.accept()
        print(f"client with addr {addr} connected.")
        threading.Thread(target=handle_connection, args=(client_sock,)).start()

    server_socket.close()


def handle_connection(sock: socket.socket) -> None:
    while True:
        if not sock.recv(1024):
            break

        sock.sendall(b"+PONG\r\n")
    sock.close()


if __name__ == "__main__":
    main()
