import socket  # noqa: F401

port_number = 9092

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", port_number), reuse_port=True)
    server.accept() # wait for client


if __name__ == "__main__":
    main()
