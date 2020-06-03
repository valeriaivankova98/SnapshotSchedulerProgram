from time import sleep
import re
import json
import paramiko

class Snapshot:
    def __init__(self, vm, snapname): #Создает прототип для снапшота в Proxmox по объекту node, айди виртуальной машины и названию снапшота
        self.vm = vm
        self.snapname = snapname
    def _wait_for_task(self, upid): #Ждет окончания текущей задачи
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.vm.node['hostname'], username=self.vm.node['username'], password=self.vm.node['password'])
            result = {'status': None}
            while result['status'] != 'stopped':
                stdin, stdout, stderr = client.exec_command('pvesh get /nodes/{node}/tasks/{upid}/status --output-format json'.format(node=self.vm.node['name'], upid=upid)) #тут можно проверять на stderr
                #print(stderr.read())
                result = json.loads(stdout.read().decode('ascii').strip('\n'))
                sleep(2)
            return result
    def create(self): #Создает снапшот, если он еще не создан
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.vm.node['hostname'], username=self.vm.node['username'], password=self.vm.node['password'])
            stdout = client.exec_command('pvesh get /nodes/{node}/qemu/{vmid}/snapshot --output-format json'.format(node=self.vm.node['name'], vmid=self.vm.vmid))[1]
            snapshots = json.loads(stdout.read().decode('ascii').strip('\n'))
            for snapshot in snapshots:
                if snapshot['name'] == self.snapname:
                    return snapshot
            print('snapshot', self.snapname, 'not found, creating')
            stdout = client.exec_command('pvesh create /nodes/{node}/qemu/{vmid}/snapshot -snapname {snapname}'.format(node=self.vm.node['name'], vmid=self.vm.vmid, snapname=self.snapname))[1]
            upid = stdout.read().decode('ascii').strip('\n')
            self._wait_for_task(upid)
            stdout = client.exec_command('pvesh get /nodes/{node}/qemu/{vmid}/snapshot --output-format json'.format(node=self.vm.node['name'], vmid=self.vm.vmid))[1]
            snapshots = json.loads(stdout.read().decode('ascii').strip('\n'))
            for snapshot in snapshots:
                if snapshot['name'] == self.snapname:
                    return snapshot
            return None
    def delete(self): #Удаляет снапшот
        print('deleting snapshot', self.snapname)
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.vm.node['hostname'], username=self.vm.node['username'], password=self.vm.node['password'])
            stdout = client.exec_command('pvesh delete /nodes/{node}/qemu/{vmid}/snapshot/{snapname}'.format(node=self.vm.node['name'], vmid=self.vm.vmid, snapname=self.snapname))[1]
            upid = stdout.read().decode('ascii').strip('\n')
        return self._wait_for_task(upid)
    def test(self): #Клонирует виртуальную машину и откатывает её к снапшоту, а затем проверяет работоспособность
        with paramiko.SSHClient() as client:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.vm.node['hostname'], username=self.vm.node['username'], password=self.vm.node['password'])
            nextid = client.exec_command('pvesh get /cluster/nextid')[1].read().decode('ascii').strip('\n')
            try:
                print('cloning to vm', nextid)
                stdout = client.exec_command('pvesh create /nodes/{node}/qemu/{vmid}/clone -newid {newid}'.format(node=self.vm.node['name'], vmid=self.vm.vmid, newid=nextid))[1]
                upid = stdout.read().decode('ascii').split('\n')[-2]
                self._wait_for_task(upid)
                print('rolling back to snapshot', self.snapname, '. this might take a while...')
                stderr = client.exec_command('zfs destroy rpool/data/vm-{vmid}-disk-0-snapshot-clone'.format(vmid=self.vm.vmid))[2]
                stderr.read() #на всякий случай удаляем предыдущую копию диска с названием snapshot-clone
                stderr = client.exec_command('zfs clone rpool/data/vm-{vmid}-disk-0@{snapname} rpool/data/vm-{vmid}-disk-0-snapshot-clone'.format(vmid=self.vm.vmid, snapname=self.snapname))[2]
                stderr.read() #копируем снапшот в новый диск snapshot-clone
                stderr = client.exec_command('dd if=/dev/zvol/rpool/data/vm-{vmid}-disk-0-snapshot-clone of=/dev/zvol/rpool/data/vm-{nextid}-disk-0 bs=1M'.format(vmid=self.vm.vmid, nextid=nextid))[2]
                stderr.read().decode('ascii') #побайтово переносим содержимое диска в новую виртуальную машину
                client.exec_command('zfs destroy rpool/data/vm-{vmid}-disk-0-snapshot-clone'.format(vmid=self.vm.vmid)) #удаляем временную копию диска
                print('starting vm', nextid)
                stdout = client.exec_command('pvesh create /nodes/{node}/qemu/{vmid}/status/start'.format(node=self.vm.node['name'], vmid=nextid))[1]
                upid = stdout.read().decode('ascii').strip('\n')
                self._wait_for_task(upid)
                print('testing vm', nextid)
                #проверка = работает ли vm после запуска
                sleep(60) #дадим приложению 60 секунд чтобы поднятся - теоретически стоит сделать это время настраиваемым
                test_result = False

                ip = None #мы не знаем айпи клона, найдем его с помощью извращений с сетью
                stdout = client.exec_command('pvesh get /nodes/{node}/qemu/{vmid}/config --output-format json'.format(node=self.vm.node['name'], vmid=nextid))[1]
                config = json.loads(stdout.read().decode('ascii').strip('\n'))
                if match := re.search(r'virtio=([A-F0-9:]+),.*bridge=([A-Za-z0-9]+),', config['net0']): #тестово - смотрим только на сетевой интерфейс net0, ищем куда он подключен у нас - это работает для мостов
                    stdout = client.exec_command('arp-scan -l -I {interface} | grep \'{mac}\''.format(interface=match.group(2), mac=match.group(1).lower()))[1].read().decode('ascii').strip('\n') #сканируем мост на мак адрес
                    if stdout:
                        search = re.search(r'(?:\d{1,3}\.){3}\d{1,3}', stdout) #достаем айпи из результата команды
                        if search.group(0):
                            ip = search.group(0)

                if self.vm.test_type == 'status': #базовый тест - смотрим что вм все еще работает
                    stdout = client.exec_command('pvesh get /nodes/{node}/qemu/{vmid}/status/current --output-format json'.format(node=self.vm.node['name'], vmid=nextid))[1]
                    status = json.loads(stdout.read().decode('ascii').strip('\n'))
                    if status['status'] == 'running':
                        test_result = True
                elif self.vm.test_type == 'icmp': #пинг, не требует аттрибута.
                    if ip:
                        stdout = client.exec_command('ping {ip} -q -c 1'.format(ip=ip))[1].read().decode('ascii').split('\n')[-3]
                        search = re.search(r'([01]) received', stdout)
                        if search and int(search.group(1)) > 0: #пришло 1 packets transmitted, 1 received
                            test_result = True
                elif self.vm.test_type == 'tcp': #проверка открыт ли порт. атрибут = порт
                    if ip:
                        stdout = client.exec_command('nc -zvw3 {ip} {port}'.format(ip=ip, port=self.vm.test_attr))[2].read().decode('ascii').split('\n')[-2]
                        search = re.search(r'open', stdout)
                        if search:
                            test_result = True
                elif self.vm.test_type == 'http': #проверка открывается ли вебстраница. атрибут = относительный путь
                    if ip:
                        stdout = client.exec_command('curl -s -o /dev/null -w "%{{http_code}}" http://{ip}{path}'.format(ip=ip, path=self.vm.test_attr))[1].read().decode('ascii').strip('\n')
                        if stdout == '200':
                            test_result = True
                elif self.vm.test_type == 'https': #проверка открывается ли вебстраница по https. атрибут = относительный путь
                    if ip:
                        stdout = client.exec_command('curl -k -s -o /dev/null -w "%{{http_code}}" https://{ip}{path}'.format(ip=ip, path=self.vm.test_attr))[1].read().decode('ascii').strip('\n')
                        if stdout == '200':
                            test_result = True


                stdout = client.exec_command('pvesh create /nodes/{node}/qemu/{vmid}/status/stop'.format(node=self.vm.node['name'], vmid=nextid))[1]
                upid = stdout.read().decode('ascii').strip('\n')
                self._wait_for_task(upid)
                print('destroying vm', nextid)
                stdout = client.exec_command('pvesh delete /nodes/{node}/qemu/{vmid}'.format(node=self.vm.node['name'], vmid=nextid))[1]
                return test_result
            except:
                stdout = client.exec_command('zfs destroy rpool/data/vm-{vmid}-disk-0-snapshot-clone'.format(vmid=self.vm.vmid))[1] #попробуем удалить временную копию диска
                stdout.read()
                client.exec_command('pvesh delete /nodes/{node}/qemu/{vmid}'.format(node=self.vm.node['name'], vmid=nextid)) #попробуем удалить клон
                raise
