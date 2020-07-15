from thrift.protocol import TBinaryProtocol
from thrift.server import TNonblockingServer
from thrift.transport import TSocket, TTransport

from api import SpiderService

# 爬虫实现
from util.covid19_spider import Spider

host = '127.0.0.1'
port = '9090'

def main():
    # 1. 创建 Thrift Server 的 ServeSocket
    serverSocket = TSocket.TServerSocket(host=host, port=port)
    # 3. 创建 Thrift Server 的 Protocol = 二进制传输协议
    protocolFactory = TBinaryProtocol.TBinaryProtocolFactory()
    # 4. 创建 Thrift Server 提供的处理方法
    handler = SpiderServiceHandler()
    # from message.api import MessageService
    processor = SpiderService.Processor(handler)
    # 5. 创建 Thrift Server, 指明如何处理，监听什么端口，传输方式，传输协议
    # thriftServer = TServer.TSimpleServer(processor,
    #                                      serverSocket,
    #                                      transportFactory,
    #                                      protocolFactory)
    thriftServer = TNonblockingServer.TNonblockingServer(processor,
                                                         serverSocket,
                                                         protocolFactory,
                                                         protocolFactory)
    # 6. 启动 Thrift Server, 等待客户端的访问
    print("Python Thrift Spider Server Start ...")
    thriftServer.serve()
    print("Python Thrift Spider Server Stop ...")

class SpiderServiceHandler:

    spider = None

    def __init__(self):
        self.spider = Spider()

    def updateOthersData(self):
        return self.spider.getOtherCountries()

    def updateChinaData(self):
        return self.spider.getChinaData()

    def updateChinaProvienceData(self, provience):
        return self.spider.getProvinceData(provience)

if __name__ == "__main__":
    main()