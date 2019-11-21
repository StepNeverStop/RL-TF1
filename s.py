from rpyc import Service
from rpyc.utils.server import ThreadedServer
from threading import Timer
import threading


class T(Service):
    def __init__(self):
        super().__init__()
        self.conns = []

    def on_connect(self, conn):
        self.conns.append(conn)

    def exposed_set_flag(self):
        for i in self.conns:
            i.root.set_flag()
    def exposed_push(self, tag):
        print(tag)
        if tag:
            print('begin set')
            self.conns[0].root.set_v_flag()
            print('set success')

    def on_disconnect(self, conn):
        self.conns.remove(conn)


if __name__ == "__main__":
    s = ThreadedServer(
        service=T(),
        hostname='0.0.0.0',
        port=12346
    )
    s.start()
