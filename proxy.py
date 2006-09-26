import sys, socket, select

class HostPortProxy() :
    def __init__(self, local_bind, remote_bind) :
        self.local_bind = local_bind
        self.remote_bind = remote_bind
        self.so = []
        self.s2s = {}
        
        sol = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sol.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT") :
            sol.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)        
        sol.bind(self.local_bind)
        sol.listen(3)
        
        self.sol = sol
        self.so.append(sol)
    
    def sockets(self) : return self.so
    
    def handle_event(self, so) :
        if so == self.sol :
            so1, addr = self.sol.accept()
            so2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try :
                so2.connect(self.remote_bind)
                self.s2s[so1] = so2
                self.s2s[so2] = so1
                self.so.append(so1)
                self.so.append(so2)
            except :
                so1.close()
        else :
            so1 = so
            try :
                so2 = self.s2s[so1]
                buf = so.recv(4096)
                if len(buf) == 0 : raise Exception, "Socket died"
                so2.send(buf)
            except :
                so1.close()
                so2.close()
                self.s2s.pop(so1)
                self.s2s.pop(so2)
                self.so.remove(so1)
                self.so.remove(so2)

    
class ProxySelect() :
    def __init__(self) :
        self.proxies = []
        
    def add_proxy(self, proxy) :
        self.proxies.append(proxy)
    
    def select(self, timeout=None) :
        if len(self.proxies) == 0 :
            raise Exception, 'No proxies to select on!'
        sos = []
        soprx = {}
        for proxy in self.proxies :
            sop = proxy.sockets()
            for so in sop :
                soprx[so] = proxy         
            sos = sos + sop
           
        ri, ro, rerr = select.select(sos, [], [], timeout)
        for so in ri :
            soprx[so].handle_event(so)
            

local_bind = (sys.argv[1], long(sys.argv[2]))
remote_bind = (sys.argv[3], long(sys.argv[4]))

sl = ProxySelect()
prx = HostPortProxy(local_bind, remote_bind)
sl.add_proxy(prx)

while 1 :
    sl.select()