from time import sleep
from datetime import datetime
import sys
import json
import traceback
import schedule
import paramiko
from VirtualMachine import VirtualMachine
from Snapshot import Snapshot

class SnapshotSchedulerProgram:
    def __init__(self):
        self.vms = []
        self.snapshot_time = None
    def snapshot_all(self):
        datetime_current = datetime.now()
        snapname = datetime_current.strftime('auto_%Y%m%d') #назовем снапшоты для каждой вм auto_YYYYMMDD
        for vm in self.vms:
            print('-------------------------------------------------')
            print('Starting snapshot creation for', vm.name, '(', vm.vmid, ')')
            print('-------------------------------------------------')
            self.snapshot_vm(vm, snapname)
        print('Next job scheduled at', self.snapshot_time)
        print('-------------------------------------------------')
    def snapshot_vm(self, vm, snapname):
        try:
            snap = Snapshot(vm, snapname)
            snap.create()
            result = snap.test() #проверяем заданным в конфиге типом проверки
            if not result: #тесты вернули не True - vm недоступна
                raise ValueError('Incorrect testing result')
            print('Successfully created snapshot', snapname, 'for', vm.name, '(', vm.vmid, ')')
            #проверим что снапшотов не больше трех старых + 1 current
            with paramiko.SSHClient() as client:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(vm.node['hostname'], username=vm.node['username'], password=vm.node['password'])
                stdout = client.exec_command('pvesh get /nodes/{node}/qemu/{vmid}/snapshot --output-format json'.format(node=vm.node['name'], vmid=vm.vmid))[1] #достаем список снапшотов для vm
                snapshot_list = json.loads(stdout.read().decode('ascii').strip("\n"))
                snapshots = sorted(snapshot_list, key=lambda k: k.get('snaptime', sys.maxsize)) #находим самый старый из них
            if len(snapshots) > 4:
                print('Deleting old snapshot', snapshots[0]['name'])
                oldsnap = Snapshot(vm, snapshots[0]['name']) #создаем для него прототип снапшота и вызываем их удаление
                oldsnap.delete()
        except:
            snap.delete() #если произошла ошибка, удалим наш неработающий снапшот
            print('ERROR: SNAPSHOT CREATION FAILED!', snapname, vm.name, '(', vm.vmid, ')')
            traceback.print_exc()
    def _test_node(self, client):
        errors = 0
        stderr = client.exec_command('zfs version')[2] #проверяем что на сервере доступны утилиты zfs
        errors += stderr.tell()
        stderr = client.exec_command('qm list')[2] #qm
        errors += stderr.tell()
        stderr = client.exec_command('pvesh get /cluster')[2] #и pvesh
        errors += stderr.tell()
        stderr = client.exec_command('apt-get install arp-scan -y')[2] #тестовый пакет для сканирования сети
        return errors
    def load_config(self, filename):
        with open(filename) as f:
            config = json.load(f)
            self.snapshot_time = config['snapshot_time'] #устанавливаем время для снапшотов = времени в конфиге
            for node in config['nodes']:
                try:
                    with paramiko.SSHClient() as client:
                        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        client.connect(node['hostname'], username=node['username'], password=node['password'], timeout=30) #здесь есть таймаут на случай если мы не можем подключится
                        test_result = self._test_node(client) #подключимся к каждой ноде и проверим что на ней установлен proxmox ve и zfs
                    if test_result != 0:
                        print('Node', node['hostname'], 'testing unsuccessful') #не установлены утилиты proxmox/zfs
                    else:
                        print('Added new node:', node['hostname'])
                        for vm in node['vms']: #сохраним объекты VM, которые будем снапшотить в массив
                            if not 'type' in vm:
                                vm['type'] = 'status' #по умолчанию смотрим что запущена VM
                            if not 'attribute' in vm:
                                vm['attribute'] = None
                            vm = VirtualMachine(node, vm['id'], vm['type'], vm['attribute'])
                            if vm.is_available: #на подключенной ноде есть vm с этим id
                                self.vms.append(vm)
                                print(vm.name, '(', vm.vmid, ') loaded')
                            else:
                                print('ERROR: Failed to load VM with id', vm['id'])
                except:
                    traceback.print_exc()
                    print('Node', node['hostname'], 'connection unsuccessful') #ошибка соединения/ssh
    def start_schedule(self):
        if not self.snapshot_time:
            print('No schedule time set!')
            return
        print('Next job scheduled at', self.snapshot_time)
        schedule.every().day.at(self.snapshot_time).do(self.snapshot_all)
        while True: #держит скрипт запущенным пока не придет время запустить задачу снапшотинга
            schedule.run_pending()
            sleep(60)
if __name__ == "__main__":
    scheduler = SnapshotSchedulerProgram()
    scheduler.load_config('config.json')
    scheduler.start_schedule()
#"type":"http",
                    #"attribute":"/status/check"
