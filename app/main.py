import socket  # noqa: F401
import struct

class Parser:
    def __init__(self, ) -> None:
        self.correlationBytesIndices = [8, 12]
        self.apiVersionBytesIndices = [6, 8]

    def get_correlation_id(self, client_response):
        return int.from_bytes(
            client_response[
                self.correlationBytesIndices[0]:
                self.correlationBytesIndices[1]
            ], 
            byteorder="big"
        )
    
    def get_api_version(self, client_response):
        return int.from_bytes(
            client_response[
                self.apiVersionBytesIndices[0]:
                self.apiVersionBytesIndices[1]
            ],
            byteorder="big"
        )

class Broker:
    def __init__(self, port_number, reuse_port) -> None:
        self.server = socket.create_server(("localhost", port_number), reuse_port=reuse_port)
        self.parser = Parser()
        self.client = None
        self.address = None

    def stream(self):
        while True:
            self.client, self.address = self.server.accept()
            self.handle_request()

    def handle_request(self):
        client_response = self.client.recv(1024)

        correlation_id = self.parser.get_correlation_id(client_response)
        # api_version = self.parser.get_api_version(client_response)
        api_version = 4
        min_version = 0
        max_version = 4
        error_code = 0

        if api_version < 0 or api_version >= 4:
            error_code = 35

        response_body = (
            struct.pack(">H", api_version) +
            struct.pack(">H", min_version) +
            struct.pack(">H", max_version) + 
            struct.pack(">B", 0)
        )

        print(response_body)

        response_header = (
            struct.pack(">I", correlation_id) +
            struct.pack(">H", error_code)
        )

        print(response_header)
        
        broker_response = self.create_response(response_header, response_body)

        print(broker_response)
        
        self.client.sendall(broker_response)

        self.client.close()        

    def create_response(self, header, body):
        msg_len = len(header) + len(body)

        print(msg_len)

        len_bytes = struct.pack(">I", msg_len)

        print(len_bytes)
        
        response = len_bytes + header + body
        return response

def main():
    print("Logs from your program will appear here!")

    broker = Broker(9092, True)
    broker.stream()

if __name__ == "__main__":
    main()
