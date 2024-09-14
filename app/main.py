import socket  # noqa: F401

class Broker:
    def __init__(self, port_number, reuse_port) -> None:
        self.server = socket.create_server(("localhost", port_number), reuse_port=reuse_port)
        self.client = None
        self.address = None

    def stream(self):
        while True:
            self.client, self.address = self.server.accept()
            self.handle_request()

    def handle_request(self):
        self.client.recv(1024)
        self.client.sendall(self.create_response(7))
        self.client.close()

    def create_response(self, message: int):
        msg_bytes = message.to_bytes(4, byteorder="big", signed=True)
        len_bytes = len(msg_bytes).to_bytes(4, byteorder="big", signed=True)
        return len_bytes + msg_bytes

def main():
    print("Logs from your program will appear here!")

    broker = Broker(9092, True)
    broker.stream()

if __name__ == "__main__":
    main()
