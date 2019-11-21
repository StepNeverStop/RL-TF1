import rpyc
from rpyc import Service
from threading import Timer
import threading

_global_flag = False
_global_v_flag = False


def change_flag():
    global _global_flag
    _global_flag = True


def change_v_flag():
    global _global_v_flag
    _global_v_flag = True


class C(Service):
    def exposed_set_flag(self):
        change_flag()

    def exposed_set_v_flag(self):
        print('123')
        change_v_flag()


def run(conn):
    global _global_flag
    global _global_v_flag
    conn.root.push(True)
    # t = threading.Thread(target=bg, args=(conn,))
    # t.start()
    while True:
        if _global_v_flag:
            conn.root.set_flag()
        if _global_flag:
            print(_global_flag)
            break
    print(1)

def bg(conn):
    global _global_flag
    global _global_v_flag
    while not _global_flag:
        if _global_v_flag:
            conn.root.set_flag()

if __name__ == "__main__":
    conn = rpyc.connect(
        host='111.186.116.71',
        port=12346,
        keepalive=True,
        service=C()
    )
    run(conn)
