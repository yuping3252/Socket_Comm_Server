import sys
import socket
import ssl
import threading
 

class MsgExchange:
    def __init__(self): 
        self.cond = threading.Condition()
        self.server_msg_sock = ""
        self.server_obj_sock = ""

        self.global_data  = ""
        self.global_pos   = 0
        self.recv_hd_lst  = []
        self.recv_obj_lst = []


    def NotifyAll(self):
        print("    In NotifyAll()")

        if self.cond.acquire():
            print("   ", str(self.global_pos) , " notifying ...")
            self.cond.notifyAll()
            self.cond.release()


    def clientThreadIn(self, conn, pos, nick):
        print("        thread IN,  ", nick, pos, "    start")
        while True: 
            try: 
                temp = conn.recv(1024)
                self.global_pos  = pos
                self.global_data = temp.decode()
                print("        Thread IN .... global_data=", self.global_data)

                if self.global_data:
                    self.recv_hd_lst = self.global_data.split('|||')
                    if self.recv_hd_lst[6] == 'xxxcccxxx':
                        print("        Thread IN,  read obj run ................")

                        self.recv_obj_lst = self.read_obj_run()
                        print("    Thread IN,  read obj run ................ done")
                    self.NotifyAll()
                else:
                    print("        Thread IN, global_data is empty")
            except:
                self.global_data = "disconnected"
                self.global_pos  = pos

                print("        thread IN disconnected, ", nick, pos, " closed")
                self.NotifyAll()
                sys.exit()


    def server_msg_socket_run(self): 
        try:
            host    = socket.gethostname()
            host_id = socket.gethostbyname(host)
            port    = 29002
            address = (host_id, port)

            self.server_msg_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_msg_sock.bind(address)
            self.server_msg_sock.listen()
            print("        In server_msg_socket_run(), connect to ", address, "  success")
        except:
            print("        In server_msg_socket_run(), connect to ", address, "  failed")


    def server_obj_socket_run(self): 
        try:
            host    = socket.gethostname()
            host_id = socket.gethostbyname(host)
            port    = 29003
            address = (host_id, port)

            self.server_obj_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_obj_sock.bind(address)
            self.server_obj_sock.listen()
            print("        In server_obj_socket_run(), connect to ", address, "  success")
        except:
            print("        In server_obj_socket_run(), connect to ", address, "  failed")


    def read_obj_run(self): 
        print("")
        print("        In read_obj_run(), waitng for an obj client connection")
        client_sock, client_addr = self.server_obj_sock.accept()
        print("        In read_obj_run(), get an obj client connection")
        client_file = client_sock.makefile('rb')
        recv_obj_lst = []
        data = 'empty'
        while data and data != '': 
            data = client_file.read()
            recv_obj_lst.append(data)
            print("        In read_obj_run(), read loop")
        client_file.close()
        print("        In read_obj_run(), read file close")
        client_sock.close()
        print("        In read_obj_run(), read socket close")
        print("")
        return recv_obj_lst


    def clientThreadOut(self, conn, pos, nick): 
        print("        thread OUT, ", nick, pos, "    start")
        while True: 
            if self.cond.acquire(): 
                print("        thread OUT, ", nick, pos, "    wait ......................")
                print("")
                self.cond.wait()                               # release lock, wait
                print("        thread OUT, ", nick, pos, "    wake up")

                if self.global_pos != pos: 
                    if self.global_data == "disconnected": 
                        continue

                    send_msg = self.global_data.encode()
                    try:
                        print("        thread OUT  ", self.global_data, "     send to ", nick)
                        conn.send(send_msg)
                        print("        ThreadOut ................. sent")

                    except:
                        print("        thread OUT sending failed, global_data=", self.global_data)
                        self.cond.release()
                        sys.exit()                 # terminate "OUT" thread

                    if self.recv_hd_lst: 
                        print("        check if need to send objects,  recv_hd_lst=", self.recv_hd_lst)

                        if self.recv_hd_lst[6] == 'xxxcccxxx': 
                            print("        clientThreadOut(),    to send objects")
                            self.send_obj_run()
                            print("        clientThreadOut(),    objects .............. sent")

                    self.cond.release()

                elif self.global_data == "disconnected": 
                    print("        thread IN already closed, now close thread OUT, global_data=", self.global_data)
                    #conn.shutdown(socket.SHUT_RDWR)
                    #conn.close()
                    #del conn
                    print("        thread OUT ", nick, pos, " closed    connection socket deleted")
                    self.cond.release()
                    sys.exit()                 # terminate "OUT" thread

    def send_obj_run(self): 
        print("            In send_obj_run(), waiting for an obj client connection")
        client_sock, addr = self.server_obj_sock.accept()
        client_file = client_sock.makefile('wb')

        for chunk in self.recv_obj_lst: 
            print("            In send_obj_run(), write loop")
            client_file.write(chunk)

        client_file.flush()
        client_file.close()
        print("            In send_obj_run(), write file close")
        client_sock.close()
        print("            In send_obj_run(), write socket close")

    
def main(): 
    exch = MsgExchange()
    exch.server_msg_socket_run()   # server side, a socket is created and listening to client connection for message
    exch.server_obj_socket_run()   # server side, a socket is created and listening to client connection for objects

    threads1 = []
    threads2 = []
    pos = 0

    while True:
        exch.global_pos    = pos
        print("")
        print("main(), ............ communication exchange server is running, wait for a msg connect to accept ............")
        print("")
        print("")
        newsock, addr = exch.server_msg_sock.accept()  # newsock: a socket from a client, addr: address on other end
        print("main(), a new client msg connect")
  
        newssl        = ssl.wrap_socket(newsock, server_side=True, certfile="cert.pem", keyfile="cert.pem", 
                                 ssl_version=ssl.PROTOCOL_TLSv1)
        temp          = newssl.read()
        exch.global_data   = temp.decode()

        if temp.decode() != "":                        # 1st data from a client
            print("main(), global_data=", exch.global_data)
            comm_partner, comm_me, fullname, fname, fsuffix, filetype, obj = exch.global_data.split('|||')
            print("main(), connected from  ", comm_me)
            exch.NotifyAll()                           # wake up to be sent jobs

            try:
                newssl.send(temp)                      # send message back to where it came from
                threads1.append(threading.Thread(name=comm_me + " threadIn",target=exch.clientThreadIn, 
                                      args=(newssl, pos, comm_me)))
                threads1[pos].start()
                threads2.append(threading.Thread(name=comm_me + " threadOut",target=exch.clientThreadOut,
                                      args=(newssl, pos, comm_me)))
                threads2[pos].start()
            except:
                print("             threads for ", comm_me, " cannot start")
        pos += 1
    s.close()



if __name__=='__main__':
    main()
    
