import socket  # noqa: F401

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
        api_version = self.parser.get_api_version(client_response)
        broker_response = None

        if api_version < 0 or api_version > 4:
            broker_response = self.create_response(correlation_id, 35)
        else:
            broker_response = self.create_response(correlation_id, -1)
        
        self.client.sendall(broker_response)
        
        self.client.close()        

    def create_response(self, message: int, error_code: int):
        msg_bytes = message.to_bytes(4, byteorder="big", signed=True)
        len_bytes = len(msg_bytes).to_bytes(4, byteorder="big", signed=True)

        if error_code != -1:
            err_bytes = int(35).to_bytes(2, byteorder="big", signed=False)
            return len_bytes + msg_bytes + err_bytes

        return len_bytes + msg_bytes

def main():
    print("Logs from your program will appear here!")

    broker = Broker(9092, True)
    broker.stream()

if __name__ == "__main__":
    main()
