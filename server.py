from rpyc import Service
from rpyc.utils.server import ThreadedServer
import os
import sys
from threading import Timer
import time
import platform
import zipfile
import pandas as pd
import threading
import shutil
import numpy as np


def fix_path(filename):
    if platform.system() == "Windows":
        if ':' in filename:
            return filename.replace('\\', '/').replace(r'//', r'/').replace('C:', 'C:/server')
        else:
            return 'C:/server' + filename
    else:
        if ':' in filename:
            return filename.replace('\\', '/').replace(r'//', r'/').replace('C:', 'C:/server').split(':')[-1]
        else:
            return '/server' + filename


class RL(Service):

    exposed_status = False

    def __init__(self):
        super().__init__()
        self.id_count = 0
        self.base_dir = r'C:/server/RLData' if platform.system() == "Windows" else r'/server/RLData'
        self.connect_list = pd.DataFrame(columns=['id', 'conn', 'job', 'reward'])
        self.training_list = pd.DataFrame(columns=['id', 'job_name', 'path', 'algo', 'policy_mode', 'save_frequency', 'max_step', 'judge_interval', 'now_interval', 'links', 'push_id', 'get_model'])
        t = threading.Thread(target=self.bg_server)
        t.start()

    def on_connect(self, conn):
        self.id_count += 1
        self.connect_list = self.connect_list.append({
            'id': self.id_count,
            'conn': conn,
            'job': 'no_train',
            'reward': np.inf
        }, ignore_index=True)
        conn.root.set_id(self.id_count)
        print(self.connect_list)

    def on_disconnect(self, conn):
        this_conn = self.connect_list[self.connect_list['conn'].isin([conn])]
        index = this_conn.index
        job = this_conn['job'].values[0]
        self.connect_list.drop(index, axis=0, inplace=True)
        if job in self.training_list['job_name'].values:
            indexs = self.training_list[self.training_list['job_name'] == job].index.tolist()
            for i in indexs:
                self.training_list.loc[i, 'links'] -= 1
            self.training_list.drop(self.training_list[self.training_list['links'] <= 0].index, axis=0, inplace=True)
        print(self.training_list)

    def exposed_get_connect_info(self):
        return """
---------------Welcome to connect to your King. ---------------
YOU CAN DO THIS THINGS:

    1. Get the training list, and select one task to help training model.

    2. Submit a new training task.

    q. exit

**PLEASE SELECT CAREFULLY, 'CAUSE IT'S NONRETURNABLE.**
        """

    def exposed_get_training_list(self):
        _string = f"""
Total: {self.training_list.shape[0]}
{self.training_list}
q.  back
        """
        return _string

    def exposed_push_zipfile(self, filename, _file):
        filepath = fix_path(filename)
        if os.path.exists(filepath):
            return True
        else:
            local_file = open(filepath, 'wb')
            chunk = _file.read(1024 * 1024)
            while chunk:
                local_file.write(chunk)
                chunk = _file.read(1024 * 1024)
            local_file.close()
        f = zipfile.ZipFile(filepath, 'r')
        for _f in f.namelist():
            print(_f)
            f.extract(_f, os.path.split(filepath)[0])
        return False

    def exposed_get_file_from_client(self, _root, filename, _file):
        filepath = fix_path(filename)
        root = fix_path(_root)
        if not os.path.isdir(root):
            os.makedirs(root)
        local_file = open(filepath, 'wb')
        chunk = _file.read(1024 * 1024)
        while chunk:
            local_file.write(chunk)
            chunk = _file.read(1024 * 1024)
        local_file.close()

    def exposed_get_env(self, _id, job_name):
        conn = self.connect_list.conn[self.connect_list['id'] == _id].values[0]
        zip_file = os.path.dirname(fix_path(self.training_list.path[self.training_list.job_name == job_name].values[0])) + '.zip'
        f_open = open(zip_file, 'rb')
        zip_exist_flag = conn.root.get_zipfile(zip_file, f_open)
        f_open.close()

    def exposed_get_model(self, _id, job_name):
        conn = self.connect_list.conn[self.connect_list['id'] == _id].values[0]
        env_name = os.path.join(*os.path.dirname(fix_path(self.training_list.path[self.training_list.job_name == job_name].values[0])).split('/')[-2:])
        print('env_name', env_name)
        model_dir = os.path.join(self.base_dir, env_name, self.training_list.algo[self.training_list.job_name == job_name].values[0], job_name)
        print('model_dir', model_dir)
        for root, dirs, files in os.walk(model_dir):
            for _file in files:
                _file_path = os.path.join(root, _file)
                f_open = open(_file_path, 'rb')
                conn.root.get_file_from_server(root, _file_path, f_open)
                f_open.close()

    def exposed_clear_model(self, _model_dir):
        model_dir = fix_path(_model_dir)
        if os.path.isdir(model_dir):
            try:
                shutil.rmtree(model_dir)
            except Exception as e:
                print(e)
                pass

    def exposed_register_train_task(self, _id, job_name):
        self.connect_list.loc[self.connect_list.id == _id, 'job'] = job_name
        self.training_list.loc[self.training_list.job_name == job_name, 'links'] += 1

    def exposed_set_timer(self, _id, job_name):
        judge_interval = self.training_list.now_interval[self.training_list.job_name == job_name].values[0]
        judge_interval = judge_interval if judge_interval > 15 else 15
        for i in self.connect_list.conn[self.connect_list.id == _id].values.tolist():
            i.root.set_judge_flag(judge_interval)

    def exposed_get_train_config(self, num):
        _data = self.training_list.loc[num]
        return _data.job_name, _data.path, _data.algo, _data.save_frequency, _data.max_step

    def exposed_push_train_config(self, myID, name, my_filepath, algo, policy_mode, save_frequency, max_step, judge_interval):
        self.training_list = self.training_list.append({
            'id': myID,
            'job_name': name,
            'path': my_filepath,
            'algo': algo,
            'policy_mode': policy_mode,
            'save_frequency': save_frequency,
            'max_step': max_step,
            'judge_interval': judge_interval,
            'now_interval': judge_interval,
            'links': 0,
            'push_id': 0,
            'get_model': False
        }, ignore_index=True)
        self.connect_list.loc[self.connect_list.id == myID, 'job'] = name

    def exposed_push_reward(self, _id, reward):
        self.connect_list.loc[self.connect_list['id'] == _id, 'reward'] = reward
        job_name = self.connect_list.job[self.connect_list['id'] == _id].values[0]
        self.training_list.loc[self.training_list.job_name == job_name, 'push_id'] = 0
        workers = self.connect_list[self.connect_list.job == job_name]
        if np.inf not in workers.reward.values.tolist():
            print(workers)
            need_push_id = workers.id[workers.reward == workers.reward.values.max()].values[0]
            self.training_list.loc[self.training_list.job_name == job_name, 'get_model'] = False
            self.training_list.loc[self.training_list.job_name == job_name, 'push_id'] = need_push_id
            print(need_push_id)
            self.connect_list.loc[self.connect_list.job == job_name, 'reward'] = np.inf

    def exposed_get_need_push_id(self, job_name):
        return self.training_list.push_id[self.training_list['job_name'] == job_name].values[0]

    def exposed_set_push_done_flag(self, job_name):
        self.training_list.loc[self.training_list.job_name == job_name, 'get_model'] = True

    def exposed_get_model_flag(self, job_name):
        return self.training_list.get_model[self.training_list['job_name'] == job_name].values[0]

    def exposed_open(self, filename):
        return os.path.getsize(filename), open(filename, 'rb')

    def bg_server(self):
        while True:
            time.sleep(1)
            self.training_list.now_interval = self.training_list.now_interval.map(lambda x: x - 1)
            indexs = self.training_list[self.training_list.now_interval <= 0].index.tolist()
            for i in indexs:
                self.training_list.loc[i, 'now_interval'] = self.training_list.loc[i, 'judge_interval']

    @staticmethod
    def authenticator(sock):
        return sock, None


def run():
    s = ThreadedServer(
        service=RL(),
        hostname='0.0.0.0',
        port=12345,
        authenticator=RL.authenticator,
        auto_register=False,
        protocol_config={
            'allow_public_attrs': True,
            'sync_request_timeout': 120
        }
    )
    s.start()


if __name__ == "__main__":
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    try:
        run()
    except Exception as e:
        print(e)
    finally:
        sys.exit()
